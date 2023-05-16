import requests
from confluent_kafka import Producer
import os
import redis
from urllib.parse import urlparse

# Kafka configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = os.getenv('KAFKA_TOPIC', 'upload')

# Redis configuration
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_db = int(os.getenv('REDIS_DB', 0))
web_link = 'https://cdn2.thecatapi.com/images/0XYvRd7oD.jpg'


def listen_for_new_items():
    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    # Create a pub/sub object
    pubsub = r.pubsub()

    # Subscribe to a channel
    pubsub.subscribe('upload')

    # Process incoming messages
    for message in pubsub.listen():
        if message['type'] == 'message':
            # New item added, print the message
            print("\nNew item:", message['data'].decode())
            return message['data']


# Check text to URL
def is_url(string):
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def upload_web_content_to_kafka(bootstrap_servers, topic, web_link):
    # Create Kafka producer configuration
    producer_config = {
        'bootstrap.servers': bootstrap_servers
    }

    # Create Kafka producer instance
    producer = Producer(producer_config)

    # Fetch the content from the web link
    response = requests.get(web_link)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the content type
        content_type = response.headers.get('content-type')

        # Get the content data
        content_data = response.content

        # Produce the content to Kafka topic
        producer.produce(topic, key=web_link, value=content_data,
                         headers={'Content-Type': content_type})

        # Flush the producer to ensure the message is sent
        producer.flush()

        # Wait for the message to be delivered (optional)
        producer.poll(1)

        print(f"Content from {web_link} uploaded to Kafka topic '{topic}'")
    else:
        print(
            f"Failed to fetch content from '{web_link}': {response.status_code}")

    # Gracefully close the producer
    producer.flush(timeout=5)
    producer.poll(10)
    producer.flush()


# Usage
while True:
    new_text_from_redis = listen_for_new_items()
    if is_url(new_text_from_redis):
        upload_web_content_to_kafka(
            bootstrap_servers, topic_name, new_text_from_redis)
    else:
        print("Not url: ", new_text_from_redis)
