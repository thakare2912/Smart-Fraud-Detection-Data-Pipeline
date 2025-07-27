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
## Architecture Overview

```mermaid
graph TD
    A[Transaction Simulator] -->|Produce JSON events| B[Kafka Topic<br>"transactions"]
    B -->|Stream processing| C[Spark Structured Streaming]
    C -->|Fraud Detection| D1[fraud.parquet]
    C -->|User Trends| D2[user_trends.parquet]
    C -->|Category Trends| D3[category_trends.parquet]
    D1 -->|MinIO Partitioned Storage| E[/raw/fraud/<br>date=YYYY-MM-DD/]
    D2 -->|MinIO Partitioned Storage| F[/raw/trends/user/]
    D3 -->|MinIO Partitioned Storage| G[/raw/trends/category/]
    E -->|Airflow Load| H[Snowflake<br>FRAUD_RECORDS]
    F -->|Airflow Load| I[Snowflake<br>USER_TRENDS]
    G -->|Airflow Load| J[Snowflake<br>CATEGORY_TRENDS]
    H --> K[Fraud Dashboard]
    I --> L[User Analytics]
    J --> M[Product Reports]

