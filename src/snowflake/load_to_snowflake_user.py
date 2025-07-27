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
S3_PREFIX = "process/user_spend_trends/"

# Snowflake Config
SNOWFLAKE_ACCOUNT = ""
SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_DATABASE = "ECOMMER"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "USER_RECORDS"

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
    )

def create_snowflake_table(conn):
    """Create Snowflake table if it doesn't exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        user_id INT,
        total_spent DOUBLE,
        num_transactions BIGINT,
        window_start STRING,
        window_end STRING,
        processed_at TIMESTAMP,
        PRIMARY KEY (user_id, window_start)
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
    """Read and process partitioned parquet files from S3."""
    logger.info(f"Scanning S3 prefix: s3://{S3_BUCKET}/{S3_PREFIX}")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        
        dfs = []
        for page in pages:
            if "Contents" not in page:
                continue
                
            for obj in page["Contents"]:
                if not obj["Key"].endswith(".parquet") or "_SUCCESS" in obj["Key"]:
                    continue

                # Read parquet file
                response_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                df = pd.read_parquet(io.BytesIO(response_obj["Body"].read()))

                # Add processing timestamp
                df['processed_at'] = datetime.now()

                dfs.append(df)

        if not dfs:
            logger.info("No valid parquet files found.")
            return None

        df_all = pd.concat(dfs, ignore_index=True)
        
        # Ensure all required columns exist
        required_columns = [
            'user_id', 'total_spent', 'num_transactions',
            'window_start', 'window_end', 'processed_at'
        ]
        
        for col in required_columns:
            if col not in df_all.columns:
                df_all[col] = None
                logger.warning(f"Added missing column: {col}")

        # Convert data types
        df_all['user_id'] = df_all['user_id'].astype('int64')
        df_all['total_spent'] = df_all['total_spent'].astype('float64')
        df_all['num_transactions'] = df_all['num_transactions'].astype('int64')
        
        logger.info(f"Loaded {len(df_all)} records from parquet files.")
        return df_all[required_columns]

    except Exception as e:
        logger.error(f"Error reading from S3: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def incremental_load_to_snowflake(conn, df):
    """Load data into Snowflake using MERGE (upsert) with robust error handling."""
    if df is None or df.empty:
        logger.info("No data to load.")
        return

    # 1. Clean and validate the DataFrame
    df.columns = df.columns.str.upper()  # Standardize to uppercase
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Ensure required columns exist
    required_columns = {
        'USER_ID', 'TOTAL_SPENT', 'NUM_TRANSACTIONS',
        'WINDOW_START', 'WINDOW_END'
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    cursor = conn.cursor()
    try:
        # 2. Get target table schema
        cursor.execute(f"DESCRIBE TABLE {SNOWFLAKE_TABLE}")
        table_columns = [row[0] for row in cursor.fetchall()]
        
        # 3. Align DataFrame with table schema
        for col in table_columns:
            if col not in df.columns:
                df[col] = None
                logger.warning(f"Added missing column: {col}")
        
        df = df[table_columns]

        # 4. Create temporary staging table
        stage_table = "TEMP_STAGE_USER_SPEND"
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {stage_table} 
            AS SELECT * FROM {SNOWFLAKE_TABLE} WHERE 1=0
        """)

        # 5. Prepare data with proper type conversion
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
                else:
                    record.append(str(val) if val is not None else None)
            records.append(tuple(record))

        # 6. Batch insert into staging table
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

        # 7. Execute MERGE operation
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.USER_ID = source.USER_ID 
           AND target.WINDOW_START = source.WINDOW_START
        WHEN MATCHED THEN UPDATE SET
            TOTAL_SPENT = source.TOTAL_SPENT,
            NUM_TRANSACTIONS = source.NUM_TRANSACTIONS,
            WINDOW_END = source.WINDOW_END,
            PROCESSED_AT = source.PROCESSED_AT
        WHEN NOT MATCHED THEN INSERT (
            USER_ID, TOTAL_SPENT, NUM_TRANSACTIONS,
            WINDOW_START, WINDOW_END, PROCESSED_AT
        ) VALUES (
            source.USER_ID, source.TOTAL_SPENT, source.NUM_TRANSACTIONS,
            source.WINDOW_START, source.WINDOW_END, source.PROCESSED_AT
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
    logger.info("Starting user spend trends pipeline...")
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