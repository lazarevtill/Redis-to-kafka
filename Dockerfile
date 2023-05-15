FROM python:3.9
LABEL authors="lazarev"
# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt main.py ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code


# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV REDIS_DB=0

# Run the Python script
CMD [ "python", "main.py" ]