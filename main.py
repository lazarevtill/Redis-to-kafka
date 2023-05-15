import requests
from kafka import KafkaProducer
import os
import redis
import threading
import boto3

# Kafka configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = os.getenv('KAFKA_TOPIC', 'upload')

# Redis configuration
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_db = int(os.getenv('REDIS_DB', 0))
file_key = os.getenv('FILE_PATH', 'file_path')

# AWS S3 configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID', 'your_access_key_id')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'your_secret_access_key')
aws_region = os.getenv('AWS_REGION', 'us-east-1')
s3_bucket_name = 'your_bucket_name'

# Connect to Redis
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Get the upload type from Redis
upload_type = redis_client.get('upload_type')


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


# Start listening for new items in a separate thread
thread = threading.Thread(target=listen_for_new_items)
thread.start()


# Function to upload file to Kafka topic
def upload_to_kafka(file_path_from_redis):
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


# Get file path from Redis
file_path = redis_client.get(file_key)


def upload_web_link(link):
    # Fetch the content from the web link
    response = requests.get(link)
    content = response.content
    upload_to_kafka(content)


def upload_s3_object(object_key):
    # Create S3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)

    # Download the object from S3
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=object_key)
    content = response['Body'].read()
    upload_to_kafka(content)


# Check if the file path exists
def check_path_to_file(upload_type_from_redis):
    # if file_path:
    #     # Decode the file path from bytes to string
    #     file_path = file_path.decode()
    #
    #     # Check if the file exists
    #     if os.path.exists(file_path):
    #         # Upload the file to Kafka topic
    #         upload_to_kafka(file_path)
    #         print("File upload started.")
    #     else:
    #         print("File not found.")
    # else:
    #     print("File path not found in Redis.")

    if upload_type_from_redis:
        upload_type = upload_type_from_redis.decode()

        if upload_type == 'file':
            # Get file paths from Redis
            file_paths = redis_client.lrange(file_key, 0, -1)

            if file_paths:
                for file_path in file_paths:
                    file_path = file_path.decode()
                    if os.path.exists(file_path):
                        upload_to_kafka(file_path)
                        print("File uploaded successfully:", file_path)
                    else:
                        print("File not found:", file_path)
            else:
                print("File paths not found in Redis.")

        elif upload_type == 's3':
            # Get S3 object keys from Redis
            s3_object_keys = redis_client.lrange('s3_object_keys', 0, -1)

            if s3_object_keys:
                for s3_object_key in s3_object_keys:
                    s3_object_key = s3_object_key.decode()
                    upload_s3_object(s3_object_key)
                    print("S3 object uploaded successfully:", s3_object_key)
            else:
                print("S3 object keys not found in Redis.")

        elif upload_type == 'web':
            # Get web links from Redis
            web_links_ = redis_client.lrange('web_links', 0, -1)
        if web_links_:
            for web_link in web_links_:
                web_link = web_link.decode()
                upload_web_link(web_link)
                print("Web link content uploaded successfully:", web_link)
            else:
                print("Web links not found in Redis.")

        else:
            print("Invalid upload type.")
    else:
        print("Upload type not found in Redis.")
