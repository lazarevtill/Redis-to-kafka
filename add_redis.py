import sys

import redis
import threading


def listen_for_new_items():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Create a pub/sub object
    pubsub = r.pubsub()

    # Subscribe to a channel
    pubsub.subscribe('upload')

    # Start listening for new messages in a separate thread
    # thread = pubsub.run_in_thread(sleep_time=0.001)

    # Process incoming messages
    for message in pubsub.listen():
        if message['type'] == 'message':
            # New item added, print the message
            print("\nNew item:", message['data'].decode())


def add_item_to_redis(item):
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    while True:
        # Prompt the user to enter an item
        item = input("Enter a new item (or 'q' to quit): ")

        if item == 'q':
            sys.exit(0)

        # Publish the new item to the channel
        r.publish('upload', item)


# Start listening for new items in a separate thread
thread = threading.Thread(target=listen_for_new_items)
thread.start()

# Example usage: Add items to Redis
# add_item_to_redis('Item 1')
# add_item_to_redis('Item 2')
