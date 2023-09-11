from kafka import KafkaProducer
import json
import datetime
import time
# Define the Kafka broker address and topic name
bootstrap_servers = 'localhost:9092'  # Change this to your Kafka broker address
topic_name = 'example_topic'  # Change this to your desired topic name

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

try:
    # Create a Python dictionary with your JSON data
    

    # Produce the JSON data to the Kafka topic

    while True:

        message_key = b'key'  # You can set a message key (optional)
        json_data = {
            "key1": "value1" + f"{datetime.datetime.now()}",
          
        }



        producer.send(topic_name, key=message_key, value=json_data)

        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()

        print(f"JSON Message sent to topic '{topic_name}': {json.dumps(json_data)}")

        time.sleep(10)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the producer to release its resources
    producer.close()
