# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Unix/macOS
# OR
venv\Scripts\activate     # On Windows

# run the kafka producer
cd src\kafka\producers
python product_producer.py

# run the kafka consumer 
cd src\kafka\consumers
python consumer.py

# run the  spark code 
docker exec -it smart_retail_fraud_trend_pipeline-spark-master-1 /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/spark.py

#then load data to snowflake using airflow 