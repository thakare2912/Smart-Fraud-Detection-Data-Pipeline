

import logging
import sys
import traceback
from datetime import datetime
import boto3
import pandas as pd
import numpy as np
import snowflake.connector
import pyarrow.parquet as pq
import io
from pandas import NaT  # This is the standard way to import Not-a-Time
# Logging Config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# MinIO Config
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "smart-retail-raw"
S3_PREFIX = "process/fraud_records/"

SNOWFLAKE_ACCOUNT = ""
SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_DATABASE = "ECOMMER"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "FRAUD_RECORDS"

def init_s3_client():
    """Initialize S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def init_snowflake_connection():
    """Initialize Snowflake connection."""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        ocsp_fail_open=True,
        insecure_mode=True
    )

def create_snowflake_table(conn):
    """Create Snowflake table if it doesn't exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        transaction_id STRING,
        user_id INT,
        product_id INT,
        store_id STRING,
        amount FLOAT,
        payment_method STRING,
        timestamp TIMESTAMP,
        user_name STRING,
        email STRING,
        user_country STRING,
        signup_date TIMESTAMP,
        product_name STRING,
        category STRING,
        base_price FLOAT,
        supplier STRING,
        product_country STRING,
        in_stock BOOLEAN,
        discount FLOAT,
        product_added_date TIMESTAMP,
        country_mismatch INT,
        high_value_flag INT,
        fraud_score FLOAT,
        discounted_price FLOAT,
        tx_year INT,
        tx_month INT,
        tx_day INT,
        last_updated TIMESTAMP,
        PRIMARY KEY (transaction_id, timestamp)
    )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        logger.info("Snowflake table checked/created successfully.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise
    finally:
        cursor.close()

def read_processed_parquet(s3_client):
    """Read and process partitioned parquet files from S3 (MinIO)."""
    logger.info(f"Scanning S3 prefix: s3://{S3_BUCKET}/{S3_PREFIX}")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

        dfs = []
        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                #Skip folders and metadata files
                if key.endswith("/") or "_SUCCESS" in key or "_temporary" in key:
                    continue

                if not key.endswith(".parquet"):
                    continue

                logger.info(f"Processing parquet file: {key}")

                # Extract partition values
                path_parts = key.split('/')
                partitions = {k: v for k, v in (p.split('=') for p in path_parts if '=' in p)}

                # Read parquet file into pandas
                response_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                df = pd.read_parquet(io.BytesIO(response_obj["Body"].read()))

                # Normalize
                if "country" in df.columns:
                    df.rename(columns={"country": "user_country"}, inplace=True)

                # Add partition columns
                for col in ["payment_method", "tx_year", "tx_month", "tx_day"]:
                    df[col] = partitions.get(col, df[col] if col in df.columns else None)

                # Add last_updated
                df["last_updated"] = datetime.now()

                dfs.append(df)

        if not dfs:
            logger.info("No parquet data found.")
            return None

        df_all = pd.concat(dfs, ignore_index=True)

        # Deduplicate
        before = len(df_all)
        df_all = df_all.drop_duplicates(subset=["transaction_id", "timestamp"], keep="last")
        logger.info(f"Deduplicated {before - len(df_all)} rows")

        # Ensure schema
        required_columns = [
            'transaction_id', 'user_id', 'product_id', 'store_id', 'amount',
            'payment_method', 'timestamp', 'user_name', 'email', 'user_country',
            'signup_date', 'product_name', 'category', 'base_price', 'supplier',
            'product_country', 'in_stock', 'discount', 'product_added_date',
            'country_mismatch', 'high_value_flag', 'fraud_score',
            'discounted_price', 'tx_year', 'tx_month', 'tx_day', 'last_updated'
        ]
        for col in required_columns:
            if col not in df_all.columns:
                df_all[col] = None

        # Fix datetime cols
        for col in ['timestamp', 'signup_date', 'product_added_date']:
            if col in df_all.columns:
                df_all[col] = pd.to_datetime(df_all[col], errors="coerce")

        logger.info(f"Final parquet DataFrame shape: {df_all.shape}")
        return df_all[required_columns]

    except Exception as e:
        logger.error(f"Error reading parquet: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def incremental_load_to_snowflake(conn, df):
    """Load data into Snowflake using MERGE (upsert) with robust error handling."""
    if df is None or df.empty:
        logger.info("No data to load.")
        return

    # Standardize and clean DataFrame
    df.columns = df.columns.str.upper()  # Uppercase for Snowflake
    df = df.loc[:, ~df.columns.duplicated()]

    # Ensure essential columns exist
    required_columns = {'TRANSACTION_ID', 'TIMESTAMP', 'USER_ID', 'PRODUCT_ID', 
                        'STORE_ID', 'AMOUNT', 'PAYMENT_METHOD'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    cursor = conn.cursor()
    try:
        # Get target table schema
        cursor.execute(f"DESCRIBE TABLE {SNOWFLAKE_TABLE}")
        table_columns = [row[0] for row in cursor.fetchall()]
        
        # Align DataFrame with table schema
        for col in table_columns:
            if col not in df.columns:
                df[col] = None
                logger.warning(f"Added missing column: {col}")

        extra_columns = set(df.columns) - set(table_columns)
        if extra_columns:
            logger.warning(f"Dropping columns not in table: {extra_columns}")
            df = df[table_columns]

        # Create temporary staging table
        stage_table = "TEMP_STAGE_TRANSACTIONS"
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {stage_table} 
            AS SELECT * FROM {SNOWFLAKE_TABLE} WHERE 1=0
        """)

        # Prepare data for insert
        records = []
        for _, row in df.iterrows():
            record = []
            for val in row:
                if pd.isna(val):
                    record.append(None)
                elif isinstance(val, pd.Timestamp):
                    record.append(val.to_pydatetime())
                elif isinstance(val, (np.integer, np.int64)):
                    record.append(int(val))
                elif isinstance(val, (np.floating, np.float64)):
                    record.append(float(val))
                elif isinstance(val, bool):
                    record.append(str(val).upper())
                else:
                    record.append(str(val) if val is not None else None)
            records.append(tuple(record))

        # Batch insert into staging
        columns = list(df.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES ({placeholders})"
        
        batch_size = 1000
        success_count = 0
        for i in range(0, len(records), batch_size):
            try:
                cursor.executemany(insert_query, records[i:i + batch_size])
                success_count += len(records[i:i + batch_size])
                logger.info(f"Inserted batch {i//batch_size + 1} ({len(records[i:i + batch_size])} rows)")
            except Exception as batch_error:
                logger.error(f"Failed on batch {i//batch_size + 1}: {str(batch_error)}")
                logger.debug(f"Problematic record sample: {records[i][:5]}...")
                raise

        # Execute MERGE
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.TRANSACTION_ID = source.TRANSACTION_ID 
           AND target.TIMESTAMP = source.TIMESTAMP
        WHEN MATCHED THEN UPDATE SET
            {', '.join([f"{col} = source.{col}" 
                       for col in columns 
                       if col not in ['TRANSACTION_ID', 'TIMESTAMP']])}
        WHEN NOT MATCHED THEN INSERT (
            {','.join(columns)}
        ) VALUES (
            {','.join([f'source.{col}' for col in columns])}
        )
        """
        cursor.execute(merge_query)
        logger.info(f"Successfully merged {success_count} records into target table")

    except Exception as e:
        logger.error(f"Error during load: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        cursor.close()

def main():
    """Main execution function."""
    logger.info("Starting fraud data pipeline...")
    s3_client = init_s3_client()
    conn = init_snowflake_connection()
    try:
        create_snowflake_table(conn)
        df = read_processed_parquet(s3_client)
        if df is not None:
            logger.info(f"Data shape: {df.shape}")
            logger.info(f"Sample data:\n{df.head(2)}")
            incremental_load_to_snowflake(conn, df)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        conn.close()
        logger.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()
