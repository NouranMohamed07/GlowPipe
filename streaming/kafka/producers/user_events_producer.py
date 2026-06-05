"""
GlowPipe — User Events Producer (Final)
=========================================
Streams user interaction events using REAL product data from the silver layer.
Models realistic shopping journeys through 8 session templates.

Events → Kafka topic: user_events

Scenarios:
  normal  — realistic funnel traffic, all session types
  viral   — product 00081be045421488 (HydroPeptide) gets 60% of traffic

Run:
  python user_events_producer.py
  python user_events_producer.py --scenario viral --rate 200
"""

import json, random, time, uuid, argparse, os
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "user_events"

# ── Real products from silver layer (24,109 total — sampled here) ──────────────
# Columns: product_id, product_name, brand_name, price, rating, source
REAL_PRODUCTS = [
    {"product_id": "00081be045421488",  "product_name": "Hydropeptide Solar Defense Tinted Spf 30", "brand_name": "HydroPeptide",     "price": 3114.11, "rating": 4.18, "source": "dermstore",   "category": "sunscreen"},
    {"product_id": "000aab28bcb1a753",  "product_name": "Serum Hidratante Facial",                  "brand_name": "Creamy",            "price": 2764.83, "rating": 0.0,  "source": "datasheet",   "category": "serum"},
    {"product_id": "0014163e436e9610",  "product_name": "Epicuren Discovery Apricot Cream Cleanser","brand_name": "Epicuren Discovery","price": 3589.79, "rating": 3.81, "source": "dermstore",   "category": "cleanser"},
    {"product_id": "0016f758502b121a",  "product_name": "Energy Boost Hydrogel Eye Patches",        "brand_name": "Catrice",           "price": 2927.15, "rating": 0.0,  "source": "datasheet",   "category": "eye_cream"},
    {"product_id": "001acdf18775b03b1", "product_name": "Spf 50+ Hydrating Glow Mist",              "brand_name": "Naked Sundays",     "price": 2079.35, "rating": 0.0,  "source": "datasheet",   "category": "sunscreen"},
    {"product_id": "001cf06a6d5ae795",  "product_name": "Uv Defense Sun Fluid Spf50",               "brand_name": "Ottie",             "price": 2222.18, "rating": 0.0,  "source": "datasheet",   "category": "sunscreen"},
    {"product_id": "002333f784158dd4",  "product_name": "Cicaful Calming Gel",                      "brand_name": "Beplain",           "price": 14990.0, "rating": 4.08, "source": "skincarisma", "category": "moisturizer"},
    {"product_id": "002599a6ae95f42f",  "product_name": "Calm Down Skinpair R-Cover Cream",         "brand_name": "Somethinc",         "price": 159918.5,"rating": 3.56, "source": "skincarisma", "category": "moisturizer"},
    {"product_id": "002a1ba668fe7386",  "product_name": "Reenergie Lift Multi-Action Sunscreen",    "brand_name": "LANCOME",           "price": 99.0,    "rating": 0.0,  "source": "cosmetics",   "category": "sunscreen"},
    {"product_id": "002aeae15fe0708c",  "product_name": "Makeup Remover Wipes",                     "brand_name": "Honest Beauty",     "price": 2030.92, "rating": 0.0,  "source": "datasheet",   "category": "cleanser"},
    {"product_id": "0005ee23557e944c",  "product_name": "Morning Drizzle Waterdrop Cream",          "brand_name": "Some By Mi",        "price": 890.0,   "rating": 4.2,  "source": "skincarisma", "category": "moisturizer"},
    {"product_id": "00062bbc4abc1b96",  "product_name": "Clean Dew Aloe Foam Cleanser",             "brand_name": "Tony Moly",         "price": 450.0,   "rating": 4.5,  "source": "skincarisma", "category": "cleanser"},
    {"product_id": "0007a97f12302f55",  "product_name": "Deep Cleansing Face Wash",                 "brand_name": "Neutrogena",        "price": 320.0,   "rating": 4.1,  "source": "dermstore",   "category": "cleanser"},
    {"product_id": "00083fb0e2c41332",  "product_name": "Bt21 Baby Cooky Pure Solution Moisturizer","brand_name": "Klean Beauty",      "price": 1100.0,  "rating": 3.9,  "source": "skincarisma", "category": "moisturizer"},
    {"product_id": "000ce062815ebe3f",  "product_name": "Prunus Amygdalus Dulcis Sweet Almond Oil", "brand_name": "The Ordinary",      "price": 250.0,   "rating": 4.6,  "source": "datasheet",   "category": "oil"},
    {"product_id": "00013284401c4991",  "product_name": "Macadamia Integrifolia Seed Oil High",     "brand_name": "Josie Maran",       "price": 1250.0,  "rating": 4.8,  "source": "skincarisma", "category": "oil"},
    {"product_id": "0001c896498fc2b8",  "product_name": "Leptospermum Scoparium Manuka Branch Oil", "brand_name": "Antipodes",         "price": 980.0,   "rating": 4.4,  "source": "skincarisma", "category": "oil"},
]

SKIN_TYPES    = ["oily", "dry", "sensitive", "combination", "normal"]
CONCERNS      = ["acne", "aging", "dryness", "sensitivity", "hyperpigmentation", "redness"]
VIRAL_PRODUCT = "00081be045421488"

# Realistic budget ranges per skin type (sensitive spends more)
SKIN_BUDGET = {
    "sensitive":   (800,  5000),
    "dry":         (500,  4000),
    "oily":        (200,  2500),
    "combination": (300,  3000),
    "normal":      (200,  2500),
}

# Skin type → preferred product categories
SKIN_PREFERENCES = {
    "oily":        ["cleanser", "serum", "sunscreen"],
    "dry":         ["moisturizer", "oil", "serum"],
    "sensitive":   ["moisturizer", "cleanser"],
    "combination": ["moisturizer", "cleanser", "serum"],
    "normal":      ["moisturizer", "serum", "sunscreen", "eye_cream"],
}

# 8 realistic session journey templates
JOURNEY_TEMPLATES = [
    ["view", "view", "add_to_cart", "purchase"],              # happy path
    ["search", "view", "view", "view", "add_to_cart", "purchase"],  # research heavy
    ["view", "add_to_cart", "abandon"],                       # price abandon
    ["search", "view", "view", "wishlist"],                   # window shopping
    ["view", "purchase"],                                     # impulse buy
    ["view", "purchase", "feedback"],                         # buy and review
    ["view", "view", "add_to_cart", "abandon"],               # hesitant
    ["search", "view", "view", "view", "view", "wishlist"],   # long research
]

EVENT_WEIGHTS = {
    "view": 0.52, "search": 0.18, "add_to_cart": 0.12,
    "wishlist": 0.08, "purchase": 0.06, "feedback": 0.04,
}


class UserSession:
    def __init__(self):
        self.user_id    = f"user_{random.randint(1, 9999):04d}"
        self.skin_type  = random.choice(SKIN_TYPES)
        self.concerns   = random.sample(CONCERNS, k=random.randint(1, 3))
        self.session_id = str(uuid.uuid4())[:8]
        self.budget     = round(random.uniform(*SKIN_BUDGET[self.skin_type]), 2)
        self.journey    = random.choice(JOURNEY_TEMPLATES)

    def preferred_products(self) -> list:
        cats = SKIN_PREFERENCES.get(self.skin_type, ["moisturizer"])
        preferred = [p for p in REAL_PRODUCTS if p["category"] in cats]
        return preferred if preferred else REAL_PRODUCTS

    def next_event(self) -> str:
        return random.choices(
            list(EVENT_WEIGHTS.keys()),
            weights=list(EVENT_WEIGHTS.values())
        )[0]


def make_event(session: UserSession, scenario: str) -> dict:
    # Viral: force popular product into 60% of events
    if scenario == "viral" and random.random() < 0.6:
        product = next(p for p in REAL_PRODUCTS if p["product_id"] == VIRAL_PRODUCT)
    else:
        product = random.choice(session.preferred_products())

    event_type   = session.next_event()
    over_budget  = product["price"] > session.budget

    # Price abandonment: over-budget cart/purchase → abandon
    if over_budget and event_type in ("add_to_cart", "purchase"):
        event_type = "abandon"

    return {
        "event_id":      str(uuid.uuid4()),
        "session_id":    session.session_id,
        "event_type":    event_type,
        # Real silver layer fields
        "product_id":    product["product_id"],
        "product_name":  product["product_name"],
        "brand_name":    product["brand_name"],
        "price":         product["price"],
        "silver_rating": product["rating"],
        "source":        product["source"],
        "category":      product["category"],
        # User context
        "user_id":       session.user_id,
        "skin_type":     session.skin_type,
        "skin_concerns": session.concerns,
        "budget":        session.budget,
        "over_budget":   over_budget,
        "rating":        round(random.uniform(1.0, 5.0), 1) if event_type == "feedback" else None,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
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
            print(f"[user_events] Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return p
        except Exception as e:
            print(f"[user_events] Attempt {attempt}/10 failed: {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")


def run(scenario: str = "normal", rate: int = 200):
    producer = build_producer()
    interval = 60.0 / rate
    sessions = [UserSession() for _ in range(20)]
    sent = 0

    print(f"[user_events] scenario={scenario} | rate={rate}/min | {len(REAL_PRODUCTS)} real products")

    try:
        while True:
            # Rotate sessions occasionally (users arrive and leave)
            if random.random() < 0.02:
                sessions[random.randint(0, len(sessions)-1)] = UserSession()

            session = random.choice(sessions)
            event   = make_event(session, scenario)
            producer.send(TOPIC, key=event["product_id"], value=event)
            sent += 1

            if sent % 100 == 0:
                producer.flush()
                print(f"[user_events] {sent} events | ts={datetime.now().strftime('%H:%M:%S')}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[user_events] Stopped. Total: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="normal", choices=["normal", "viral"])
    parser.add_argument("--rate", type=int, default=200)
    args = parser.parse_args()
    run(args.scenario, args.rate)
