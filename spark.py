# import redis
# import time
# import json

# class SimpleProducer:
#     def __init__(self):
#         self.data = [
#             {"id": 1, "name": "Alice", "age": 30},
#             {"id": 2, "name": "Bob", "age": 25},
#             {"id": 3, "name": "Charlie", "age": 35}
#         ]
#         self.index = 0

#     def get_data(self):
#         if self.index < len(self.data):
#             record = self.data[self.index]
#             self.index += 1
#             return record
#         return None

# class ETLConsumer:
#     def __init__(self, redis_host='localhost', redis_port=6379):
#         self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

#     def transform(self, data):
#         # Example transformation: add a new field "status"
#         data['status'] = 'active' if data['age'] < 30 else 'inactive'
#         return data

#     def load_to_redis(self, data):
#         # Store the data into Redis
#         key = f"user:{data['id']}"
#         self.redis_client.set(key, json.dumps(data))

#     def run(self, producer):
#         while True:
#             data = producer.get_data()
#             if data is None:
#                 print("No more data to process.")
#                 break

#             transformed_data = self.transform(data)
#             self.load_to_redis(transformed_data)
#             print(f"Processed and stored data: {transformed_data}")

#             # Simulate delay
#             time.sleep(1)

# if __name__ == "__main__":
#     producer = SimpleProducer()
#     consumer = ETLConsumer()
#     consumer.run(producer)



import requests
import redis
import json
import time

class APIProducer:
    def __init__(self, api_url):
        self.api_url = api_url

    def get_data(self):
        response = requests.get(self.api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()

class ETLConsumer:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    def transform(self, data):
        # Example transformation: add a new field "status"
        for item in data:
            item['status'] = 'active' if item.get('completed') else 'inactive'
        return data

    def load_to_redis(self, data):
        for item in data:
            key = f"task:{item['id']}"
            self.redis_client.set(key, json.dumps(item))

    def run(self, producer):
        while True:
            data = producer.get_data()
            if not data:
                print("No more data to process.")
                break

            transformed_data = self.transform(data)
            self.load_to_redis(transformed_data)
            print(f"Processed and stored {len(transformed_data)} records.")

            # Simulate delay (if you want a continuous process)
            time.sleep(10)

if __name__ == "__main__":
    # Example API URL for JSONPlaceholder's todos endpoint
    api_url = "https://jsonplaceholder.typicode.com/todos"
    producer = APIProducer(api_url)
    consumer = ETLConsumer()
    consumer.run(producer)
