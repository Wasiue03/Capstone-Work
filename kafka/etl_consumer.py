from kafka import KafkaConsumer
import redis
import json

class ETLConsumer:
    def __init__(self, kafka_topic, redis_host='localhost', redis_port=6379, bootstrap_servers='localhost:9092'):
        try:
            self.consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        except Exception as e:
            # Handle initialization errors
            print(f"Error initializing Kafka consumer or Redis client: {e}")
            raise

    def transform_and_store(self):
        try:
            for message in self.consumer:
                data = message.value
                # Example transformation: add a new field "status"
                data['status'] = 'active' if data.get('completed') else 'inactive'
                key = f"task:{data['id']}"
                self.redis_client.set(key, json.dumps(data))
                print(f"Stored: {data}")
        except Exception as e:
            # Handle message processing errors
            print(f"Error processing Kafka message or storing data in Redis: {e}")
            # You can choose to continue processing other messages or raise the exception
            # raise

if __name__ == "__main__":
    kafka_topic = "api_data"
    print("Starting Kafka consumer...")
    consumer = ETLConsumer(kafka_topic)
    print("Kafka consumer initialized successfully.")
    try:
        print("Starting message processing...")
        consumer.transform_and_store()
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping Kafka consumer.")
    except Exception as e:
        print(f"An error occurred: {e}")
