import sys
import logging
import time
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, TimestampType, LongType
)
import os
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ------------------------------------
# Logging
# ------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SmartRetailFraudPipeline")

# ------------------------------------
# Config
# ------------------------------------
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "smart-retail-raw"


def create_spark_session():
    logger.info("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("SmartRetailFraudPipeline")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.metrics.logger.level", "WARN")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized.")
    return spark

# ------------------------------------
# Schemas
# ------------------------------------
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", TimestampType()),
])

user_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("country", StringType()),
    StructField("signup_date", TimestampType()),
])

product_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("base_price", DoubleType()),
    StructField("supplier", StringType()),
    StructField("country", StringType()),
    StructField("in_stock", BooleanType()),
    StructField("discount", DoubleType()),
    StructField("product_added_date", TimestampType()),
])

# ------------------------------------
# Read raw data from MinIO
# ------------------------------------
def read_from_minio(spark):
    logger.info("Reading transactions from MinIO...")
    transactions_df = spark.readStream \
        .format("csv") \
        .option("header", True) \
        .schema(transaction_schema) \
        .load("s3a://smart-retail-raw/raw/transactions/")

    logger.info("Reading users from MinIO...")
    users_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(user_schema) \
        .load("s3a://smart-retail-raw/raw/users/")

    logger.info("Reading products from MinIO...")
    products_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(product_schema) \
        .load("s3a://smart-retail-raw/raw/products/")

    return transactions_df, users_df, products_df



def process_stream(transactions_df, users_df, products_df):
    logger.info("Processing stream: joins + fraud logic + aggregates")

    # ✅ Fix conflicting column names
    users_df = users_df.withColumnRenamed("country", "user_country") \
                       .withColumnRenamed("name", "user_name")

    products_df = products_df.withColumnRenamed("country", "product_country") \
                             .withColumnRenamed("name", "product_name")

    # ✅ Apply watermark ONLY on streaming transactions
    transactions_df = transactions_df.withWatermark("timestamp", "15 minutes")

    # ✅ JOIN streaming transactions with static users and products
    enriched_df = (
        transactions_df
        .join(users_df, on="user_id", how="leftOuter")
        .join(products_df, on="product_id", how="leftOuter")
    )

    # ✅ Add fraud detection columns
    fraud_df = (
        enriched_df
        .withColumn(
            "country_mismatch",
            F.when(F.col("user_country") != F.col("product_country"), 1).otherwise(0)
        )
        .withColumn(
            "high_value_flag",
            F.when(F.col("amount") > 500, 1).otherwise(0)
        )
        .withColumn(
            "fraud_score",
            (F.col("country_mismatch") * 0.5) + (F.col("high_value_flag") * 0.5)
        )
        .withColumn(
            "discounted_price",
            F.when(
                F.col("base_price").isNotNull() & F.col("discount").isNotNull(),
                F.col("base_price") - (F.col("base_price") * F.col("discount") / 100)
            ).otherwise(None)
        )
    )

    # ✅ User spend trend (with window_start and window_end)
    user_spend_df = (
        fraud_df
        .groupBy(
            F.window("timestamp", "1 minute"),
            F.col("user_id")
        )
        .agg(
            F.sum("amount").alias("total_spent"),
            F.approx_count_distinct("transaction_id").alias("num_transactions")
        )
        .withColumn("window_start", F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_end", F.date_format(F.col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        .drop("window")
    )

    # ✅ Category sales trend (with window_start and window_end)
    category_trend_df = (
        fraud_df
        .groupBy(
            F.window("timestamp", "1 minute"),
            F.col("category")
        )
        .agg(
            F.sum("amount").alias("total_sales"),
            F.approx_count_distinct("transaction_id").alias("num_transactions")
        )
        .withColumn("window_start", F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_end", F.date_format(F.col("window.end"), "yyyy-MM-dd HH:mm:ss"))
        .drop("window")
    )

    return fraud_df, user_spend_df, category_trend_df

# ------------------------------------
# Write foreachBatch handlers
# ------------------------------------


def process_and_write_fraud(df, batch_id):
    try:
        row_count = df.count()
        if row_count > 0:
            logger.info(f"Fraud Batch {batch_id} → {row_count} rows")

            # ✅ Drop all legacy ambiguous date columns
            for col_name in ["year", "month", "day"]:
                if col_name in df.columns:
                    df = df.drop(col_name)

            # ✅ Add correct ones
            df = (
                df
                .withColumn("tx_year", year(col("timestamp")))
                .withColumn("tx_month", month(col("timestamp")))
                .withColumn("tx_day", dayofmonth(col("timestamp")))
            )

            # ✅ Write with clear partitions only
            output_path = f"s3a://{MINIO_BUCKET}/process/fraud_records/"
            df.write.mode("append").partitionBy(
                "payment_method", "tx_year", "tx_month", "tx_day"
            ).parquet(output_path)
    except Exception as e:
        logger.error(f"Error processing fraud batch {batch_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_and_write_user_spend(df, batch_id):
    row_count = df.count()
    if row_count > 0:
        logger.info(f"User Spend Batch {batch_id} → {row_count} rows")
        output_path = f"s3a://{MINIO_BUCKET}/process/user_spend_trends/"
        df.write.mode("append").parquet(output_path)
    else:
        logger.info(f"User Spend Batch {batch_id} → 0 rows")

def process_and_write_category_trend(df, batch_id):
    row_count = df.count()
    if row_count > 0:
        logger.info(f"Category Trend Batch {batch_id} → {row_count} rows")
        output_path = f"s3a://{MINIO_BUCKET}/process/category_trends/"
        df.write.mode("append").parquet(output_path)
    else:
        logger.info(f"Category Trend Batch {batch_id} → 0 rows")

# ------------------------------------
# Main Driver
# ------------------------------------
def main():
    try:
        logger.info("Creating Spark session...")
        spark = create_spark_session()

        transactions_df, users_df, products_df = read_from_minio(spark)
        fraud_df, user_spend_df, category_trend_df = process_stream(transactions_df, users_df, products_df)

        logger.info("Starting write queries...")

        queries = [
            fraud_df.writeStream
                .outputMode("append")
                .foreachBatch(process_and_write_fraud)
                .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/transactions_df")
                .start(),

            user_spend_df.writeStream
                .outputMode("complete")
                .foreachBatch(process_and_write_user_spend)
                .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/users_df")
                .start(),

            category_trend_df.writeStream
                .outputMode("complete")
                .foreachBatch(process_and_write_category_trend)
                .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/products_df")
                .start()
        ]

        while True:
            for query in queries:
                if query and query.exception():
                    logger.error(f"Query failed: {query.exception()}")
                    raise query.exception()
            time.sleep(10)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
