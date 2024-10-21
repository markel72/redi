from confluent_kafka import Producer
import json
import time

# Kafka Producer configuration
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

# Initialize Producer



def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Order data to be sent
orders = [
    {"order_id": 1, "product": "Laptop", "quantity": 2, "price": 1200.00},
    {"order_id": 2, "product": "Headphones", "quantity": 5, "price": 50.00},
    {"order_id": 3, "product": "Monitor", "quantity": 1, "price": 250.00}
]


def publish_orders(topic, config):
    producer = Producer(config)
    for order in orders:
        # Convert order to JSON
        message = json.dumps(order)

        # Send message to Kafka
        producer.produce(topic=topic, key=str(order["order_id"]), value=message, callback=acked)

        # Force delivery of messages to Kafka
        producer.poll(1)
        time.sleep(1)  # Sleep for 1 second between messages
    producer.flush()

# Publish orders to Kafka topic 'orders'
if __name__ == '__main__':
    config = read_config()
    topic = "orders"
    publish_orders(topic, config)
