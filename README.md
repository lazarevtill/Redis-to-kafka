# Self experiment in microservice 

This simple code get url from Redis and upload content from it to Kafka "Upload" topic.

Script "publish_to_redis.py" was written for testing needs (check that all ideas works)
# Usage

For use (play with) this code you need to use "docker-compose.yaml" to set up Redis and Kafka (simplest instances) \
``` 
docker compose up -d 
```
And build docker image from Dockerfile, run it 
```
docker build -t redis-to-kafka . 
docker run redis-to-kafka -d
```
After that you can run "publish_to_redis.py" to easy send sth to redis and check service is working

```
python3 publish_to_redis.py
```