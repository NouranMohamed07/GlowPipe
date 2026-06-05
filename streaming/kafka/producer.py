"""
GlowPipe Streaming Producer — Enhanced
=======================================
Simulates 5 real business scenarios that REQUIRE streaming:

  Scenario 1 — Viral product spike
    A product suddenly gets 10x normal traffic (TikTok effect).
    Streaming detects this in 5 min. Batch would miss it for 23 hours.

  Scenario 2 — Allergen outbreak
    Multiple users with sensitive skin report the same product as unsafe
    within minutes. Triggers a real-time alert before more users are harmed.

  Scenario 3 — Session-level personalisation
    A single user views → adds to cart → rates → abandons in one session.
    Streaming updates their skin profile in real time for the next query.

  Scenario 4 — Price-sensitivity signal
    Users consistently abandon products above a certain price point for
    their skin type. Retailers can adjust pricing within the hour.

  Scenario 5 — Ingredient concern spike
    Suddenly many products being flagged for the same ingredient
    (e.g. a new study drops about fragrance). Streaming surfaces this
    cross-product pattern in minutes.

Topics produced:
  user_events      — user interactions with products
  product_safety   — INCI API safety check results per product + skin type
  ingredient_alerts— cross-product ingredient concern signals  (NEW)

Run:
  python producer.py                          # all scenarios, 200 events/min
  python producer.py --scenario viral         # force viral spike scenario
  python producer.py --scenario allergen      # force allergen outbreak
  python producer.py --rate 50               # slower for debugging
"""

import json
import random
import time
import uuid
import argparse
from datetime import datetime, timezone

from kafka import KafkaProducer

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:9092"

SKIN_TYPES  = ["oily", "dry", "sensitive", "combination", "normal"]
CATEGORIES  = ["moisturizer", "serum", "cleanser", "sunscreen", "toner", "eye_cream", "mask"]
SOURCES     = ["dermstore", "skincarisma", "datasheet", "cosmetics"]
CONCERNS    = ["acne", "aging", "dryness", "sensitivity", "hyperpigmentation", "redness"]

# Real product IDs from the silver layer
PRODUCT_POOL = [
    "00081be045421488", "000aab28bcb1a753", "0014163e436e9610",
    "0016f758502b121a", "001acdf18775b03b1", "001cf06a6d5ae795",
    "002333f784158dd4", "002599a6ae95f42f", "002a1ba668fe7386",
    "002aeae15fe0708c", "0005ee23557e944c", "00062bbc4abc1b96",
    "0007a97f12302f55", "00083fb0e2c41332", "00081be045421488",
]

# One product is secretly "going viral" — rotates every 10 minutes
VIRAL_PRODUCT_ID   = "00081be045421488"   # HydroPeptide Solar Defense
OUTBREAK_PRODUCT   = "002333f784158dd4"   # Cicaful Calming Gel (ironic for sensitive skin)

# Ingredients of concern (mirror your silver layer ingredient names)
CONCERN_INGREDIENTS = [
    "Phenoxyethanol", "Fragrance", "Sodium Lauryl Sulfate",
    "Parabens", "Alcohol Denat", "Titanium Dioxide", "Coconut Acid",
]

# Realistic session funnel weights: most users just view, few buy
EVENT_FUNNEL = {
    "view":         0.52,
    "search":       0.18,
    "add_to_cart":  0.12,
    "wishlist":     0.08,
    "purchase":     0.06,
    "feedback":     0.04,
}

# Skin type → price sensitivity (sensitive skin users spend more)
SKIN_PRICE_RANGE = {
    "sensitive":   (15.0, 180.0),
    "dry":         (12.0, 150.0),
    "oily":        (8.0,  90.0),
    "combination": (10.0, 120.0),
    "normal":      (8.0,  100.0),
}

# ── User session simulation ────────────────────────────────────────────────────

class UserSession:
    """
    Simulates a realistic user journey through the app.
    A session has: skin_type, concerns, a sequence of events,
    and a price threshold above which they abandon.
    """
    def __init__(self):
        self.user_id   = f"user_{random.randint(1, 9999):04d}"
        self.skin_type = random.choice(SKIN_TYPES)
        self.concerns  = random.sample(CONCERNS, k=random.randint(1, 3))
        self.session_id = str(uuid.uuid4())[:8]
        price_range    = SKIN_PRICE_RANGE[self.skin_type]
        self.budget    = round(random.uniform(*price_range), 2)
        self.viewed    = []

    def next_event_type(self) -> str:
        return random.choices(
            list(EVENT_FUNNEL.keys()),
            weights=list(EVENT_FUNNEL.values()),
            k=1
        )[0]

# ── Event builders ─────────────────────────────────────────────────────────────

def make_user_event(
    scenario: str = "normal",
    session: UserSession = None,
) -> dict:
    if session is None:
        session = UserSession()

    event_type = session.next_event_type()
    price_range = SKIN_PRICE_RANGE[session.skin_type]
    price = round(random.uniform(*price_range), 2)

    # Scenario 1: viral product gets 10× more traffic
    if scenario == "viral" or (scenario == "normal" and random.random() < 0.15):
        product_id = VIRAL_PRODUCT_ID
    else:
        product_id = random.choice(PRODUCT_POOL)

    # Price abandonment signal (Scenario 4)
    if price > session.budget and event_type in ("add_to_cart", "purchase"):
        event_type = "abandon"          # new event type — price too high

    session.viewed.append(product_id)

    return {
        "event_id":    str(uuid.uuid4()),
        "session_id":  session.session_id,
        "event_type":  event_type,
        "product_id":  product_id,
        "user_id":     session.user_id,
        "skin_type":   session.skin_type,
        "skin_concerns": session.concerns,
        "category":    random.choice(CATEGORIES),
        "price":       price,
        "budget":      session.budget,
        "over_budget": price > session.budget,
        "rating":      round(random.uniform(1.0, 5.0), 1) if event_type == "feedback" else None,
        "source":      random.choice(SOURCES),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


def make_safety_event(scenario: str = "normal") -> dict:
    skin_type  = random.choice(SKIN_TYPES)

    # Scenario 2: allergen outbreak — sensitive skin + same product = many alerts
    if scenario == "allergen" or (scenario == "normal" and random.random() < 0.08):
        product_id        = OUTBREAK_PRODUCT
        skin_type         = "sensitive"
        compat_score      = random.randint(10, 45)   # consistently bad
        allergens_present = random.sample(CONCERN_INGREDIENTS, k=random.randint(2, 4))
        is_safe           = False
    else:
        product_id        = random.choice(PRODUCT_POOL)
        compat_score      = random.randint(30, 100)
        allergens_present = random.sample(CONCERN_INGREDIENTS, k=random.randint(0, 2))
        is_safe           = compat_score >= 60 and len(allergens_present) == 0

    return {
        "event_id":             str(uuid.uuid4()),
        "product_id":           product_id,
        "skin_type":            skin_type,
        "compatibility_score":  compat_score,
        "allergens_detected":   allergens_present,
        "allergen_count":       len(allergens_present),
        "is_safe":              is_safe,
        "overall_safety_score": random.randint(30, 100),
        "pregnancy_safe":       random.choice([True, False]),
        "fungal_acne_safe":     random.choice([True, False]),
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }


def make_ingredient_alert(scenario: str = "normal") -> dict:
    """
    Scenario 5: cross-product ingredient concern spike.
    When the same ingredient appears as problematic across many products
    in a short window, this fires a cross-product signal.
    This is something batch CANNOT detect until the next day's run.
    """
    # During ingredient spike scenario, concentrate on one ingredient
    if scenario == "ingredient_spike" or (scenario == "normal" and random.random() < 0.05):
        ingredient  = "Phenoxyethanol"        # suddenly spiking
        concern_lvl = "high"
        product_ids = random.sample(PRODUCT_POOL, k=random.randint(3, 6))
    else:
        ingredient  = random.choice(CONCERN_INGREDIENTS)
        concern_lvl = random.choice(["low", "medium", "high"])
        product_ids = random.sample(PRODUCT_POOL, k=random.randint(1, 3))

    return {
        "alert_id":          str(uuid.uuid4()),
        "ingredient_name":   ingredient,
        "concern_level":     concern_lvl,
        "affected_products": product_ids,
        "affected_count":    len(product_ids),
        "skin_types_at_risk": random.sample(SKIN_TYPES, k=random.randint(1, 3)),
        "signal_source":     random.choice(["user_reports", "inci_api", "safety_db"]),
        "timestamp":         datetime.now(timezone.utc).isoformat(),
    }


# ── Producer ───────────────────────────────────────────────────────────────────

def build_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            print(f"[producer] Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as exc:
            print(f"[producer] Attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")


def run(scenario: str = "normal", rate: int = 200):
    """
    scenario: 'normal' | 'viral' | 'allergen' | 'ingredient_spike'
              In 'normal' mode all scenarios fire probabilistically.
              Force a specific scenario for demo purposes.
    rate    : total events per minute
    """
    producer = build_producer()
    interval = 60.0 / rate
    sent     = 0
    sessions = [UserSession() for _ in range(20)]   # pool of 20 concurrent sessions

    print(f"[producer] scenario={scenario}  rate={rate}/min  interval={interval:.3f}s")
    print(f"[producer] Viral product  : {VIRAL_PRODUCT_ID}")
    print(f"[producer] Outbreak product: {OUTBREAK_PRODUCT}")

    try:
        while True:
            # Rotate sessions occasionally (simulates users leaving and new ones arriving)
            if random.random() < 0.02:
                sessions[random.randint(0, len(sessions)-1)] = UserSession()

            session = random.choice(sessions)

            # Always send a user event
            ue = make_user_event(scenario=scenario, session=session)
            producer.send("user_events", key=ue["product_id"], value=ue)

            # ~60% of the time also send a safety check (not every view triggers INCI)
            if random.random() < 0.6:
                se = make_safety_event(scenario=scenario)
                producer.send("product_safety", key=se["product_id"], value=se)

            # ~10% of the time send an ingredient-level signal
            if random.random() < 0.10:
                ia = make_ingredient_alert(scenario=scenario)
                producer.send("ingredient_alerts", key=ia["ingredient_name"], value=ia)

            sent += 1
            if sent % 100 == 0:
                producer.flush()
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[producer] {sent} events sent  ts={ts}  scenario={scenario}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[producer] Stopped. Total sent: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GlowPipe Kafka producer")
    parser.add_argument(
        "--scenario",
        default="normal",
        choices=["normal", "viral", "allergen", "ingredient_spike"],
        help=(
            "normal          = all scenarios fire probabilistically (default)\n"
            "viral           = force product spike scenario\n"
            "allergen        = force allergen outbreak scenario\n"
            "ingredient_spike= force cross-product ingredient concern scenario"
        ),
    )
    parser.add_argument("--rate", type=int, default=200,
                        help="Events per minute (default 200)")
    args = parser.parse_args()
    run(scenario=args.scenario, rate=args.rate)
