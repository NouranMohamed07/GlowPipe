"""
GlowPipe Streaming Consumer (test / debug only)
Prints messages from both Kafka topics to stdout.
The real processing is done by Spark Structured Streaming (stream_job.py).

Run:  python consumer.py
      python consumer.py --topic user_events
      python consumer.py --topic product_safety
"""

import json
import argparse
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "kafka:9092"
TOPICS          = ["user_events", "product_safety"]


def run(topics: list, group_id: str = "glowpipe-debug-consumer"):
    print(f"[consumer] Subscribing to: {topics}")
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("[consumer] Listening... (Ctrl+C to stop)\n")
    count = 0
    try:
        for msg in consumer:
            count += 1
            print(f"[{msg.topic}] partition={msg.partition} offset={msg.offset}")
            print(f"  key  : {msg.key}")
            print(f"  value: {json.dumps(msg.value, indent=2)}")
            print()
    except KeyboardInterrupt:
        print(f"\n[consumer] Stopped. Total received: {count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GlowPipe debug consumer")
    parser.add_argument("--topic", default="both",
                        help="Topic name or 'both' (default: both)")
    args = parser.parse_args()

    topics = TOPICS if args.topic == "both" else [args.topic]
    run(topics)
