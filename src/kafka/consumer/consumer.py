import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# === Kafka Config ===
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPICS = ["transactions"]
KAFKA_GROUP_ID = "smart-retail-batch-consumer"

# === MinIO Config ===
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "smart-retail-raw"
MINIO_ENDPOINT = "localhost:9000"

# === Batch Config ===
BATCH_SIZE = 100
FLUSH_INTERVAL = 60  # seconds

def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

def ensure_bucket_exists(minio_client, bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info(f"Created bucket {bucket_name}")
    else:
        logger.info(f"Bucket {bucket_name} exists")

def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe(KAFKA_TOPICS)
    logger.info(f"Subscribed to topics: {KAFKA_TOPICS}")

    buffers = {topic: [] for topic in KAFKA_TOPICS}
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.time()

            if msg is None:
                if now - last_flush >= FLUSH_INTERVAL:
                    for topic, buffer in buffers.items():
                        if buffer:
                            flush_buffer(topic, buffer, minio_client)
                            buffer.clear()
                    last_flush = now
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            record = json.loads(msg.value().decode("utf-8"))
            buffers[topic].append(record)

            if len(buffers[topic]) >= BATCH_SIZE:
                flush_buffer(topic, buffers[topic], minio_client)
                buffers[topic].clear()
                last_flush = now

            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer stopped manually.")
        for topic, buffer in buffers.items():
            if buffer:
                flush_buffer(topic, buffer, minio_client)
                buffer.clear()
    finally:
        consumer.close()

def flush_buffer(topic, records, minio_client):
    if not records:
        return

    now = datetime.utcnow()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    timestamp_str = now.strftime("%Y%m%d%H%M%S%f")

    df = pd.DataFrame(records)

    local_file = f"/tmp/{topic}_batch_{timestamp_str}.csv"
    df.to_csv(local_file, index=False)

    object_name = f"raw/{topic}/year={year}/month={month}/day={day}/batch_{timestamp_str}.csv"
    minio_client.fput_object(MINIO_BUCKET, object_name, local_file)

    logger.info(f"Flushed {len(records)} records → s3a://{MINIO_BUCKET}/{object_name}")

if __name__ == "__main__":
    main()
