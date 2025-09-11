# kafka_json_producer.py

import json
import random
import time
from kafka import KafkaProducer

def create_json_message():
    """
    Generates a dictionary with random data in the specified format.
    """
    data = {
        "player_car_index": random.randint(0, 9),
        "fuel_in_tank": round(random.uniform(0, 100), 2),
        "event_ts": time.time()
    }
    return data

def json_serializer(data):
    """
    Serializes a dictionary to a JSON string and encodes it to UTF-8 bytes.
    This is required by the Kafka producer.
    """
    return json.dumps(data).encode('utf-8')

def produce_messages():
    """
    Connects to a Kafka broker and continuously sends JSON messages.
    """
    # Define your Kafka broker and topic.
    # Make sure your Kafka broker is running and accessible at this address.
    bootstrap_servers = 'localhost:9092'
    topic_name = 'car_status_telemetry'

    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=json_serializer
        )

        print(f"Connected to Kafka broker at {bootstrap_servers}")
        print(f"Producing messages to topic '{topic_name}'...")

        # Produce messages in a continuous loop.
        while True:
            message = create_json_message()
            
            # Send the message to the Kafka topic.
            producer.send(topic_name, value=message)
            
            print(f"Sent: {message}")
            
            # Wait for 1 second before sending the next message.
            time.sleep(1)
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if producer:
            # Ensure the producer is closed and all messages are sent.
            producer.close()
            print("Producer closed.")

if __name__ == '__main__':
    produce_messages()