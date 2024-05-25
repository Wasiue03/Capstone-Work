from kafka import KafkaProducer
import requests
import json
import time

class APIProducer:
    def __init__(self, api_url, kafka_topic, bootstrap_servers='localhost:9092'):
        self.api_url = api_url
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic

    def fetch_and_send(self):
        response = requests.get(self.api_url)
        response.raise_for_status()
        data = response.json()
        for item in data:
            self.producer.send(self.kafka_topic, item)
            print(f"Sent: {item}")
            time.sleep(1)  # Simulate delay

if __name__ == "__main__":
    api_url = "https://jsonplaceholder.typicode.com/todos"
    kafka_topic = "api_data"
    producer = APIProducer(api_url, kafka_topic)
    producer.fetch_and_send()
