from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
import json

# Kafka Consumer configuration
def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

consumer_config=read_config()
consumer = Consumer(consumer_config)

# MongoDB connection
client = MongoClient(
    "<use connection string from mongodb>")
db = client['order_db']
orders_collection = db['orders']


def consume_orders():
    try:
        # Subscribe to the 'orders' topic
        consumer.subscribe(['orders'])

        while True:
            # Poll Kafka for messages
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Decode the message value (order data)
            order = json.loads(msg.value().decode('utf-8'))
            print(f"Received order: {order}")

            # Insert order into MongoDB
            orders_collection.insert_one(order)
            print("Order stored in MongoDB")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the consumer
        consumer.close()


# Consume and store orders
if __name__ == '__main__':
    consume_orders()
