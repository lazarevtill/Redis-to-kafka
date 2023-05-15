import redis


def add_item_to_redis():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    while True:
        # Prompt the user to enter an item
        item = input("Enter a new item (or 'q' to quit): ")

        if item == 'q':
            break

        # Publish the new item to the channel
        r.publish('upload', item)


# Start adding items to Redis
add_item_to_redis()
