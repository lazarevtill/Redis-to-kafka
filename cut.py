import requests
from confluent_kafka import Producer


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
        producer.produce(topic, key=web_link, value=content_data, headers={'Content-Type': content_type})

        # Flush the producer to ensure the message is sent
        producer.flush()

        # Wait for the message to be delivered (optional)
        producer.poll(1)

        print(f"Content from '{web_link}' uploaded to Kafka topic '{topic}'")
    else:
        print(f"Failed to fetch content from '{web_link}': {response.status_code}")

    # Gracefully close the producer
    producer.flush(timeout=5)
    producer.poll(10)
    producer.flush()


# Usage example
bootstrap_servers = 'localhost:9092'
topic = 'upload'
web_link = 'https://cdn2.thecatapi.com/images/0XYvRd7oD.jpg'

upload_web_content_to_kafka(bootstrap_servers, topic, web_link)
