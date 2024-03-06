import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

if __name__ == "__main__":
    for i in range(10):
        print(f"Sending message {i}")
        producer.send("events", value={"number": i})
        time.sleep(3)
    producer.flush()
