docker compose up -d 
install python 3.10.11
pip install -r requirements.txt

cd src/kafka/producer
python transaction_producer.py

cd src/kafka/consumer
python consumer.py

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/spark.py
