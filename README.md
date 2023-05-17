# Self-experiment in microservice 

This simple code gets url from Redis and uploads content from it to a Kafka "Upload" topic.

The Script "publish_to_redis.py" was written for testing needs (check that all ideas work)
# Usage

For use (play with) this code you need to use "docker-compose.yaml" to set up Redis and Kafka (the simplest instances) \
``` 
docker compose up -d 
```
And build docker image from Dockerfile, run it 
```
docker build -t redis-to-kafka . 
docker run redis-to-kafka -d
```
After that, you can run "publish_to_redis.py" to easily send sth to redis and check the service is working

```
python3 publish_to_redis.py
```
# .env

For further use, almost all vars were excluded from code use add and have fun!