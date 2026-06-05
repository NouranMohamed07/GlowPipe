"""
GlowPipe — Ingredient Alert Producer (Final)
==============================================
Fires cross-product ingredient concern signals when the same ingredient
appears flagged across multiple products in a short window.

Events → Kafka topic: ingredient_alerts

This is the signal that batch CANNOT detect — it only emerges when you
see the same ingredient flagged across N products within minutes.
A daily batch job aggregates across the full day and misses the spike.

Scenarios:
  normal — random ingredients flagged occasionally (~5% chance of spike)
  spike  — Phenoxyethanol suddenly appears across 3-6 products simultaneously

Run:
  python ingredient_alert_producer.py
  python ingredient_alert_producer.py --scenario spike --rate 20
"""

import json, random, time, uuid, argparse, os
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC           = "ingredient_alerts"

SKIN_TYPES = ["oily", "dry", "sensitive", "combination", "normal"]

# Real product IDs from silver layer
PRODUCT_POOL = [
    "00081be045421488", "000aab28bcb1a753", "0014163e436e9610",
    "0016f758502b121a", "002333f784158dd4", "002599a6ae95f42f",
    "002a1ba668fe7386", "002aeae15fe0708c", "0005ee23557e944c",
    "00062bbc4abc1b96", "0007a97f12302f55", "00083fb0e2c41332",
]

# Real ingredient names from silver layer — concern ingredients only
CONCERN_INGREDIENTS = [
    "Phenoxyethanol",         # main spike ingredient
    "Fragrance",
    "Sodium Lauryl Sulfate",
    "Parabens",
    "Alcohol Denat",
    "Titanium Dioxide",
    "Coconut Acid",
]

SIGNAL_SOURCES = ["user_reports", "inci_api", "safety_db"]


def make_alert(scenario: str) -> dict:
    if scenario == "spike" or (scenario == "normal" and random.random() < 0.05):
        ingredient  = "Phenoxyethanol"
        concern_lvl = "high"
        products    = random.sample(PRODUCT_POOL, k=random.randint(3, 6))
    else:
        ingredient  = random.choice(CONCERN_INGREDIENTS)
        concern_lvl = random.choice(["low", "medium", "high"])
        products    = random.sample(PRODUCT_POOL, k=random.randint(1, 3))

    return {
        "alert_id":           str(uuid.uuid4()),
        "ingredient_name":    ingredient,
        "concern_level":      concern_lvl,
        "affected_products":  products,
        "affected_count":     len(products),
        "skin_types_at_risk": random.sample(SKIN_TYPES, k=random.randint(1, 3)),
        "signal_source":      random.choice(SIGNAL_SOURCES),
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }


def build_producer():
    for attempt in range(1, 11):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all", retries=3,
            )
            print(f"[ingredient_alert] Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[ingredient_alert] Attempt {attempt}/10 failed: {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")


def run(scenario: str = "normal", rate: int = 20):
    producer = build_producer()
    interval = 60.0 / rate
    sent = 0

    print(f"[ingredient_alert] scenario={scenario} | rate={rate}/min | Topic: {TOPIC}")

    try:
        while True:
            alert = make_alert(scenario)
            producer.send(TOPIC, key=alert["ingredient_name"], value=alert)
            sent += 1
            if sent % 20 == 0:
                producer.flush()
                print(f"[ingredient_alert] {sent} alerts sent | ts={datetime.now().strftime('%H:%M:%S')}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[ingredient_alert] Stopped. Total: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="normal", choices=["normal", "spike"])
    parser.add_argument("--rate", type=int, default=20)
    args = parser.parse_args()
    run(args.scenario, args.rate)
