import time
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

if __name__ == "__main__":
    for message in consumer:
        print(f"Received message: {message.value}")
        time.sleep(1)
        # this will stop if e is pressed
        if input() == "e":
            break
