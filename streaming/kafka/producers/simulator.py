"""
GlowPipe — Demo Simulator (Final)
===================================
Starts all three producers simultaneously in daemon threads.
One command fires the entire streaming layer.

Combined rate: ~300 events/min across 3 Kafka topics:
  user_events       → 200 events/min  (user shopping sessions)
  product_safety    →  80 events/min  (INCI API safety checks)
  ingredient_alerts →  20 events/min  (cross-product signals)

Demo scenarios — switch live during presentation:
  normal   — realistic traffic across all producers
  viral    — product 00081be045421488 gets 60% of traffic
  allergen — Cicaful Calming Gel repeatedly flagged → outbreak fires in Grafana
  spike    — Phenoxyethanol appears across 3-6 products simultaneously

Run:
  python simulator.py
  python simulator.py --scenario viral
  python simulator.py --scenario allergen
  python simulator.py --scenario spike
  python simulator.py --rate 500        # stress test
"""

import argparse
import threading
import time
from datetime import datetime

import user_events_producer
import safety_checker_producer
import ingredient_alert_producer


def run_thread(name: str, target_fn, args: tuple) -> threading.Thread:
    """Start a producer in a daemon thread with automatic crash recovery."""
    def wrapper():
        while True:
            try:
                print(f"[simulator] Starting {name}...")
                target_fn(*args)
            except Exception as exc:
                print(f"[simulator] {name} crashed: {exc} — restarting in 10s")
                time.sleep(10)

    t = threading.Thread(target=wrapper, name=name, daemon=True)
    t.start()
    return t


def run(scenario: str = "normal", rate: int = 300):
    # Map top-level scenario to each producer's scenario name
    user_scenario       = "viral"    if scenario == "viral"    else "normal"
    safety_scenario     = "allergen" if scenario == "allergen" else "normal"
    ingredient_scenario = "spike"    if scenario == "spike"    else "normal"

    # Rate split
    user_rate       = int(rate * 0.67)   # ~200
    safety_rate     = int(rate * 0.27)   # ~80
    ingredient_rate = int(rate * 0.07)   # ~20

    print(f"""
╔══════════════════════════════════════════════════════════╗
║         GlowPipe Streaming Layer — Demo Simulator        ║
║                                                          ║
║  user_events        → {user_rate:<3} events/min                ║
║  product_safety     → {safety_rate:<3} events/min                ║
║  ingredient_alerts  → {ingredient_rate:<3} events/min                ║
║  Total              → ~{rate} events/min                  ║
║                                                          ║
║  Scenario  : {scenario:<46}║
║  Started at: {datetime.now().strftime('%H:%M:%S'):<46}║
╚══════════════════════════════════════════════════════════╝
""")

    threads = [
        run_thread("user_events",       user_events_producer.run,       (user_scenario, user_rate)),
        run_thread("safety_checker",    safety_checker_producer.run,    (safety_scenario, safety_rate)),
        run_thread("ingredient_alerts", ingredient_alert_producer.run,  (ingredient_scenario, ingredient_rate)),
    ]

    # Stagger startup so Kafka isn't overwhelmed
    time.sleep(2)

    print(f"[simulator] All {len(threads)} producers running. Ctrl+C to stop.\n")

    try:
        while True:
            alive = sum(1 for t in threads if t.is_alive())
            print(f"[simulator] {datetime.now().strftime('%H:%M:%S')} | {alive}/{len(threads)} producers active")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n[simulator] Shutting down all producers...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GlowPipe demo simulator — runs all producers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
scenarios:
  normal   regular traffic (default)
  viral    forces product 00081be045421488 to dominate trending panel
  allergen forces allergen outbreak on product 002333f784158dd4
  spike    forces Phenoxyethanol to spike across multiple products
        """
    )
    parser.add_argument(
        "--scenario", default="normal",
        choices=["normal", "viral", "allergen", "spike"],
    )
    parser.add_argument(
        "--rate", type=int, default=300,
        help="Total events per minute (default 300)"
    )
    args = parser.parse_args()
    run(args.scenario, args.rate)
