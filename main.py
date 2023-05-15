from kafka import KafkaProducer
import os
import redis

# Kafka configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = 'upload'

# Redis configuration
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_db = int(os.getenv('REDIS_DB', 0))
file_key = 'file_path'


# Function to upload file to Kafka topic
def upload_file(file_path_from_redis):
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Open file in binary mode
    with open(file_path_from_redis, 'rb') as file:
        # Read file content
        content = file.read()

        # Publish file content to Kafka topic
        producer.send(topic_name, content)

    # Close Kafka producer
    producer.close()


# Connect to Redis
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Get file path from Redis
file_path = redis_client.get(file_key)

# Check if the file path exists
if file_path:
    # Decode the file path from bytes to string
    file_path = file_path.decode()

    # Check if the file exists
    if os.path.exists(file_path):
        # Upload the file to Kafka topic
        upload_file(file_path)
        print("File uploaded successfully.")
    else:
        print("File not found.")
else:
    print("File path not found in Redis.")
