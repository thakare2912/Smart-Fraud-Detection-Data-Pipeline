import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'transactions'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def generate_transaction():
    return {
        "transaction_id": f"T{random.randint(1000000, 9999999)}",  # PK
        "user_id": random.randint(1, 2000),  # FK
        "product_id": random.randint(1, 500),  # FK
        "store_id": f"S{random.randint(1, 5)}",
        "amount": round(random.uniform(10, 5000), 2),
        "payment_method": random.choice(['credit_card', 'debit_card', 'cash', 'paypal']),
        "country": random.choice(['USA', 'UK', 'Germany', 'India']),
        "transaction_time": datetime.utcnow().isoformat()
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Transaction record delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    print(f"Starting Transaction Producer on topic: {TOPIC}")

    while True:
        txn = generate_transaction()
        producer.produce(
            TOPIC,
            key=txn['transaction_id'],
            value=json.dumps(txn),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.5)  # 1 txn every 2 sec

    producer.flush()
