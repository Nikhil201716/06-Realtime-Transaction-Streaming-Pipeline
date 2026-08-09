"""
producer.py
------------
Simulates a live transaction feed and XADDs each event to the Redis
Stream `transactions:raw` in real time (not a batch dump) - this is the
"event source" half of the pipeline, standing in for what a real
payments switch or point-of-sale gateway would publish.

Emits mostly normal transactions at a steady baseline rate, with a
handful of scheduled FRAUD SCENARIOS injected at random points during
the run:

  - card_testing:        a burst of 5-8 tiny transactions in <1s gaps
                          (an attacker probing whether a stolen card
                          number still works, before attempting a big
                          purchase)
  - stolen_card_spree:    3-5 large transactions from a foreign country
                          on an unrecognized device, in quick succession
  - account_takeover:      a device change followed by a burst of
                          escalating-amount transactions from the
                          account's own home country (the sneakier
                          pattern - it doesn't trip an obvious
                          new-country flag, only velocity + new-device
                          + amount anomaly)

Each event carries a `scenario_label` field for LATER EVALUATION ONLY
(scripts/evaluate_detection.py) - pipeline/consumer.py's scoring logic
never reads this field, so the rules are judged fairly, the same way
they'd have to work against a real, unlabeled live feed.

Usage:
    python scripts/producer.py --duration 180 --rate 2.5
"""

import argparse
import json
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

import sys
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from pipeline.redis_client import get_client, STREAM_RAW  # noqa: E402

DATA_DIR = ROOT / "data"
MERCHANTS = [f"MERCH{i:04d}" for i in range(500)]
FOREIGN_COUNTRIES = ["Nigeria", "Russia", "Ukraine", "Vietnam", "Romania", "Indonesia"]


def load_accounts():
    df = pd.read_csv(DATA_DIR / "accounts.csv")
    df["typical_categories"] = df["typical_categories"].str.split(",")
    return df


def make_txn_id():
    return f"TXN{uuid.uuid4().hex[:12]}"


def normal_event(account, rng):
    amount = max(1.0, rng.lognormal(np.log(account.avg_transaction_amount), 0.35))
    category = rng.choice(account.typical_categories)
    return {
        "transaction_id": make_txn_id(),
        "account_id": account.account_id,
        "amount": round(float(amount), 2),
        "country": account.home_country,
        "device_id": account.primary_device_id,
        "category": category,
        "merchant_id": rng.choice(MERCHANTS),
        "event_ts": time.time(),
        "scenario_label": "normal",
    }


def card_testing_events(account, rng):
    events = []
    n = int(rng.integers(5, 9))
    for _ in range(n):
        events.append({
            "transaction_id": make_txn_id(),
            "account_id": account.account_id,
            "amount": round(float(rng.uniform(1.0, 5.0)), 2),
            "country": account.home_country,
            "device_id": account.primary_device_id,
            "category": rng.choice(["Online Retail", "Entertainment", "Electronics"]),
            "merchant_id": rng.choice(MERCHANTS),
            "event_ts": None,   # filled in at send time
            "scenario_label": "card_testing",
            "_delay": rng.uniform(0.2, 0.9),
        })
    return events


def stolen_card_spree_events(account, rng):
    events = []
    n = int(rng.integers(3, 6))
    fake_device = f"DEV{rng.integers(100000, 999999)}"
    foreign_country = rng.choice(FOREIGN_COUNTRIES)
    for _ in range(n):
        events.append({
            "transaction_id": make_txn_id(),
            "account_id": account.account_id,
            "amount": round(float(account.avg_transaction_amount * rng.uniform(7, 15)), 2),
            "country": foreign_country,
            "device_id": fake_device,
            "category": rng.choice(["Electronics", "Travel", "Online Retail"]),
            "merchant_id": rng.choice(MERCHANTS),
            "event_ts": None,
            "scenario_label": "stolen_card_spree",
            "_delay": rng.uniform(1.0, 3.0),
        })
    return events


def account_takeover_events(account, rng):
    events = []
    n = int(rng.integers(4, 7))
    fake_device = f"DEV{rng.integers(100000, 999999)}"
    for i in range(n):
        multiplier = rng.uniform(2.0, 4.0) * (1 + i * 0.3)   # escalating amounts
        events.append({
            "transaction_id": make_txn_id(),
            "account_id": account.account_id,
            "amount": round(float(account.avg_transaction_amount * multiplier), 2),
            "country": account.home_country,             # deliberately NOT foreign - the sneaky part
            "device_id": fake_device,
            "category": rng.choice(account.typical_categories),
            "merchant_id": rng.choice(MERCHANTS),
            "event_ts": None,
            "scenario_label": "account_takeover",
            "_delay": rng.uniform(0.8, 2.0),
        })
    return events


SCENARIO_BUILDERS = {
    "card_testing": card_testing_events,
    "stolen_card_spree": stolen_card_spree_events,
    "account_takeover": account_takeover_events,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=180, help="seconds to run the producer")
    parser.add_argument("--rate", type=float, default=2.5, help="baseline normal events per second")
    parser.add_argument("--n-scenarios", type=int, default=6, help="number of fraud scenario bursts to inject")
    parser.add_argument("--seed", type=int, default=123)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    accounts = load_accounts()
    client = get_client()
    client.ping()
    print(f"Connected to Redis. Streaming to '{STREAM_RAW}' for {args.duration}s at ~{args.rate} events/sec...")

    # Pre-schedule scenario injection points across the run
    scenario_times = sorted(rng.uniform(10, args.duration - 10, size=args.n_scenarios))
    scenario_types = rng.choice(list(SCENARIO_BUILDERS.keys()), size=args.n_scenarios)
    schedule = list(zip(scenario_times, scenario_types))
    print(f"Scheduled {args.n_scenarios} fraud scenarios at t={[round(t,1) for t in scenario_times]}s")

    start = time.time()
    sent, flagged_scenarios = 0, 0
    next_scenario_idx = 0

    while time.time() - start < args.duration:
        elapsed = time.time() - start

        # Fire a scheduled scenario if it's due
        if next_scenario_idx < len(schedule) and elapsed >= schedule[next_scenario_idx][0]:
            _, scenario_type = schedule[next_scenario_idx]
            account = accounts.iloc[rng.integers(0, len(accounts))]
            events = SCENARIO_BUILDERS[scenario_type](account, rng)
            print(f"  [t={elapsed:.1f}s] Injecting scenario '{scenario_type}' "
                  f"for {account.account_id} ({len(events)} events)")
            for e in events:
                delay = e.pop("_delay")
                e["event_ts"] = time.time()
                client.xadd(STREAM_RAW, {k: str(v) for k, v in e.items()})
                sent += 1
                time.sleep(delay)
            next_scenario_idx += 1
            flagged_scenarios += 1
            continue

        # Otherwise, emit a normal event at the baseline rate
        account = accounts.iloc[rng.integers(0, len(accounts))]
        event = normal_event(account, rng)
        client.xadd(STREAM_RAW, {k: str(v) for k, v in event.items()})
        sent += 1
        time.sleep(1.0 / args.rate)

    print(f"\nDone. Sent {sent:,} events ({flagged_scenarios} fraud scenario bursts) "
          f"to Redis Stream '{STREAM_RAW}' over {args.duration}s.")


if __name__ == "__main__":
    main()
