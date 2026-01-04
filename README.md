# Smart Fraud Detection Data Pipeline

A real-time data pipeline for fraud detection and spending trend analysis.
## Architecture Overview

<img src="pipeline_architecture.png" alt="Pipeline Architecture Diagram" width="350" style="max-width:100%;">  
*Diagram of the data flow from Kafka to Snowflake*


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

# Smart Fraud Detection Data Pipeline

## 📌 Overview
This project implements an **end-to-end data engineering pipeline** that ingests retail transaction data in real time, enriches it with user and product information, detects potential fraud, and writes results to reliable storage for analytics and reporting.

The pipeline is designed with **production-ready patterns** using Apache Kafka, Spark Structured Streaming, Airflow orchestration, MinIO object storage, and Snowflake for warehousing.

---

## 🏗️ Architecture Overview

<img src="pipeline_architecture.png" alt="Pipeline Architecture Diagram" width="350" style="max-width:100%;">  
*Diagram of the data flow from Kafka to Snowflake*


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

## 📥 Getting Started
✅ Prerequisites

Make sure the following tools are installed on your system:

Docker

Docker Compose

Python 3.8+

Apache Airflow CLI (optional)

Snowflake Account (optional – for warehouse testing)

### 📦 Clone the Repository

```bash
git clone https://github.com/thakare2912/Smart-Fraud-Detection-Data-Pipeline.git
cd Smart-Fraud-Detection-Data-Pipeline
```
### 📦 Start Docker 

```bash
docker-compose up -d
```
### check runnig staus 
```bash
docker-compose ps
```
### produce the transaction  data kafka producer 
``` bash
python src\kafka\producer\transction_producer
```

### consume the data with use kafka conumer and sent to minio 
``` bash
python src\kafka\consumer\transction_producer
```
### Run the Streaming Pipeline 
```bash
cd src\spark\jobs\spark.py
```
``` bash
docker exec -it smart_retail_fraud_trend_pipeline-spark-master-1 /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/spark.py
```

### 📊 Output Storage 
minio/
└── smart-retail-raw/
    └── process/
        ├── fraud_records/
        ├── user_spend_trends/
        └── category_trends/




