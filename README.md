# Smart Fraud Detection Data Pipeline

An end-to-end real-time data pipeline that detects fraudulent transactions, analyzes user spending behavior, and generates product trend insights using cutting-edge big data tools. Built using **Kafka**, **Spark Structured Streaming**, **MinIO**, **Airflow**, and **Snowflake**.

---

## Architecture Overview


---

##  Tech Stack

| Component           | Technology              |
|---------------------|--------------------------|
| Streaming Platform  | Apache Kafka (`confluent_kafka`) |
| Data Processing     | Apache Spark Structured Streaming |
| Storage Layer       | MinIO (S3-compatible object storage) |
| Orchestration       | Apache Airflow (modular DAGs) |
| Data Warehouse      | Snowflake               |
| Language            | Python                  |
| Data Format         | Parquet (partitioned)   |

---

##  Features

- Simulates real-time financial transactions using Kafka.
- Detects fraud using rule-based and conditional logic.
- Calculates:
  - Per-user spend trends
  - Product/category popularity metrics
- Writes partitioned Parquet files to MinIO (by year/month/day).
- Loads curated datasets into Snowflake using Airflow DAGs.
- Modular design with separate consumer, analytics, and loader logic.

---
---

## How It Works

### 1. **Kafka Producer**
- Simulates financial transactions with fields like `user_id`, `amount`, `payment_method`, etc.
- Sends them to a Kafka topic (`transactions`).

### 2. **Spark Structured Streaming**
- Consumes real-time messages from Kafka.
- Applies fraud detection rules:
  - High-value amount + suspicious location
  - Rapid-fire multiple transactions
- Computes:
  - `fraud_records`
  - `user_spend_trends`
  - `category_trends`
- Writes all outputs to MinIO in Parquet format, partitioned by date.

### 3. **Airflow DAGs**
- Scheduled DAGs read partitioned data from MinIO.
- Use Python-based loaders to:
  - Create Snowflake tables if not exist
  - Insert new data using merge/upsert
  - Track `last_updated` timestamps

---


