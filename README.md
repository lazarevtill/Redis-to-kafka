# Self-experiment in microservice 

This simple code gets url from Redis and uploads content from it to a Kafka "Upload" topic.

The Script "publish_to_redis.py" was written for testing needs (check that all ideas work)
# Usage

For use (play with) this code you need to build docker image for service and use "docker-compose.yaml" to set up Redis and Kafka (the simplest instances) with "redis-to-kafka" microservice\
#### Build docker image from Dockerfile 
```
docker build -t redis-to-kafka . 
```
#### Run everything together 
``` 
docker compose up -d 
```

After that, you can run "publish_to_redis.py" to easily send sth to redis and check the service is working

```
python3 publish_to_redis.py
```
# .env

For further use, almost all vars were excluded from code use add and have fun!