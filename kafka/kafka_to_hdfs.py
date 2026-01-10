import json
import random
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer
from hdfs import InsecureClient
from hdfs.util import HdfsError

# ================= CONFIGURATION =================

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'traffic-events'
KAFKA_GROUP_ID = 'hdfs-consumer-group'

HDFS_NAMENODE_URL = 'http://localhost:9870'   # Accessed from host machine
HDFS_USER = 'root'
RAW_BASE_PATH = '/data/raw/traffic'

BATCH_SIZE = 50       # Write every 50 messages
WRITE_TIMEOUT = 30    # Or every 30 seconds maximum

# ================= HDFS & PARTITION MANAGEMENT =================

def get_hdfs_client():
    """Create and return an HDFS client"""
    return InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)

def get_partition_path(event):
    """
    Build HDFS partition path based on event date and zone
    Example: /data/raw/traffic/date=2026-01-10/zone=Zone_A
    """
    event_time = datetime.fromisoformat(event['event_time'])
    date = event_time.strftime('%Y-%m-%d')
    zone = event['zone'].replace(' ', '_')
    return f"{RAW_BASE_PATH}/date={date}/zone={zone}"

def ensure_directory(client, path):
    """Create directory in an idempotent way"""
    try:
        client.status(path)
    except HdfsError:
        client.makedirs(path)

# ================= MAIN CONSUMER LOGIC =================

def main():
    print(">>> Starting Kafka → HDFS consumer...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',  # use 'earliest' to reprocess history
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f">>> Connected to Kafka topic: {KAFKA_TOPIC}")

    hdfs_client = get_hdfs_client()

    batch = defaultdict(list)   # {partition_path: [json_lines]}
    last_write = datetime.now()

    try:
        for message in consumer:
            event = message.value

            partition_path = get_partition_path(event)
            batch[partition_path].append(json.dumps(event))

            total_messages = sum(len(lines) for lines in batch.values())

            # Flush if batch size or timeout reached
            if total_messages >= BATCH_SIZE or \
               (datetime.now() - last_write).seconds >= WRITE_TIMEOUT:

                flush_batch(hdfs_client, batch)
                batch.clear()
                last_write = datetime.now()

    except KeyboardInterrupt:
        print("\n>>> Consumer stopped by user.")
    except Exception as e:
        print(f">>> Unexpected error: {e}")
    finally:
        if batch:
            flush_batch(hdfs_client, batch)
        consumer.close()
        print(">>> Kafka consumer closed.")

# ================= HDFS WRITE FUNCTION =================

def flush_batch(client, batch):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    for partition_path, lines in batch.items():
        ensure_directory(client, partition_path)

        # Unique filename for each flush operation
        filename = f"{partition_path}/events_{timestamp}_{random.randint(0,9999)}.jsonl"

        try:
            with client.write(filename, encoding='utf-8') as writer:
                for line in lines:
                    writer.write(line + '\n')

            print(f">>> Written {len(lines)} events → {filename}")

        except Exception as e:
            print(f">>> Write error {filename}: {e}")

# ================= ENTRY POINT =================

if __name__ == "__main__":
    main()
