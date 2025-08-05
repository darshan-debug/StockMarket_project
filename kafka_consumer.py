from kafka import KafkaConsumer
import json
import time
import os
import datetime

# --- Configuration ---
KAFKA_BROKERS = ['localhost:9092']
TOPIC_NAME = 'quickstart-events.stockmarket.transactions' # by default , the DB,collection name get added to topic name, by connnector
CONSUMER_GROUP_ID = 'file_writer_consumer_group_CG1'

# I will store the messages in this directory
OUTPUT_DIR = 'kafka_data_stream'
os.makedirs(OUTPUT_DIR, exist_ok=True) 

# naming convention for unloaded mss from kafka
FILE_PREFIX = "kafka_messages_"
FILE_EXTENSION = ".json" # Assuming your Kafka messages are JSON

# msgs collected in this duration(maxx 100) are written to a single file
FILE_ROTATION_INTERVAL_SECONDS = 60

def write_to_file(messages, file_path):
    """Appends messages to file, one JSON object per line."""
    with open(file_path, 'a', encoding='utf-8') as f:
        for msg in messages:             
            f.write(msg + '\n')

def run_kafka_to_file_writer():
    """consumer code to read messages from Kafka and write them to a file."""
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest', # when consuming for the 1st time, reset to earliest available offset
            group_id=CONSUMER_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"Kafka consumer initialized for topic: {TOPIC_NAME}")
        print(f"Writing messages to directory: {os.path.abspath(OUTPUT_DIR)}")

        current_file_path = None
        last_rotation_time = time.time()
        message_buffer = [] # Buffer to hold messages before writing to file

        while True:
            # Poll for messages with a timeout
            raw_messages = consumer.poll(timeout_ms=1000) # Poll for 1 second

            if raw_messages:
                for tp, messages in raw_messages.items():
                    for message in messages:
                        msg_dict = json.loads(message.value)
                        msg_dict['received_time'] = datetime.datetime.now().isoformat()  # Add received time
                        msg_dict=json.dumps(msg_dict)  # Convert to string for writing                        
                        message_buffer.append(msg_dict)
                        print(msg_dict) # for debugging

            # if buffer is full or 60 seconds have passed, then write to file
            if time.time() - last_rotation_time >= FILE_ROTATION_INTERVAL_SECONDS or len(message_buffer) >= 100: # Write every 100 messages or after specified interval
                if message_buffer:
                    timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    current_file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{timestamp_str}{FILE_EXTENSION}")
                    write_to_file(message_buffer, current_file_path)
                    print(f"Wrote {len(message_buffer)} messages to {current_file_path}")
                    message_buffer = [] # Clear buffer
                last_rotation_time = time.time()
            
            # if msgs are coming very slowly, then flush every 5 seconds, useful while testing
            elif message_buffer and time.time() - last_rotation_time >= 5: 
                 timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                 current_file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{timestamp_str}{FILE_EXTENSION}")
                 write_to_file(message_buffer, current_file_path)
                 print(f"Wrote {len(message_buffer)} buffered messages to {current_file_path}")
                 message_buffer = []
                 last_rotation_time = time.time()


    except KeyboardInterrupt:
        print("\nStopping Kafka to file writer...")
    except Exception as e:
        print(f"An error occurred in Kafka to file writer: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        print("Kafka consumer closed.")
        # Ensure any remaining messages in buffer are written before exit
        if message_buffer:
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S_final")
            final_file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{timestamp_str}{FILE_EXTENSION}")
            write_to_file(message_buffer, final_file_path)
            print(f"Wrote remaining {len(message_buffer)} messages to {final_file_path}")

if __name__ == '__main__':
    run_kafka_to_file_writer()