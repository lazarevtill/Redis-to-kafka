import sys

import kafka
import os
import redis
import boto3
import requests

# Kafka configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic_name = 'upload'

# Redis configuration
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_db = int(os.getenv('REDIS_DB', 0))
file_key = 'file_path'

# AWS S3 configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID', 'your_access_key_id')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'your_secret_access_key')
aws_region = os.getenv('AWS_REGION', 'us-east-1')
s3_bucket_name = 'your_bucket_name'


# Function to upload content to Kafka topic
def upload_to_kafka(content):
    # Create Kafka producer
    producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Publish content to Kafka topic
    producer.send(topic_name, content)

    # Close Kafka producer
    producer.close()


# Function to upload file to Kafka topic
def upload_file(file_path):
    # Open file in binary mode
    with open(file_path, 'rb') as file:
        # Read file content
        content = file.read()
        upload_to_kafka(content)


# Function to upload S3 object to Kafka topic
def upload_s3_object(object_key):
    # Create S3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             region_name=aws_region)

    # Download the object from S3
    response = s3_client.get_object(Bucket=s3_bucket_name, Key=object_key)
    content = response['Body'].read()
    upload_to_kafka(content)


# Function to upload web link content to Kafka topic
def upload_web_link(link):
    # Fetch the content from the web link
    response = requests.get(link)
    content = response.content
    upload_to_kafka(content)


# Connect to Redis
redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Check Redis connection
try:
    redis_client.ping()
    print("Connected to Redis successfully.")
except redis.ConnectionError:
    print("Failed to connect to Redis.")
    sys.exit()


# Check Kafka connection
def check_kafka_connection():
    try:
        # Create a Kafka admin client
        admin_client = kafka.KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        # Fetch cluster metadata to check the connection
        metadata = admin_client.list_topics()

        # Check if the metadata is available
        if metadata:
            print("Connected to Kafka successfully.")
        else:
            print("Failed to connect to Kafka.")
            sys.exit()
    except:
        print("Kafka connection error")
        sys.exit()


# Call the function to check Kafka connection
check_kafka_connection()

# Get the upload type from Redis
upload_type = redis_client.get('upload_type')

if upload_type:
    upload_type = upload_type.decode()

    if upload_type == 'file':
        # Get file paths from Redis
        file_paths = redis_client.lrange(file_key, 0, -1)

        if file_paths:
            for file_path in file_paths:
                file_path = file_path.decode()
                if os.path.exists(file_path):
                    upload_file(file_path)
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
    web_links = redis_client.lrange('web_links', 0, -1)

    if web_links:
        for web_link in web_links:
            web_link = web_link.decode()
            upload_web_link(web_link)
            print("Web link content uploaded successfully:", web_link)
        else:
            print("Web links not found in Redis.")
    else:
        print("Invalid upload type.")
else:
    print("Upload type not found in Redis.")
