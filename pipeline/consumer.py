"""
consumer.py
------------
The real-time stream processor: reads transaction events off the Redis
Stream via a CONSUMER GROUP (XREADGROUP / XACK) - not a naive poll-and-
delete - so message processing is at-least-once and acknowledged
individually, the same durability pattern a real production consumer
would use (if this process crashes mid-batch, un-ACKed messages are
still claimable by a replacement consumer instead of being lost).

For each event:
  1. Look up the account's historical profile (loaded once, in memory).
  2. Update a Redis SORTED SET sliding-window velocity counter for that
     account (ZADD + ZREMRANGEBYSCORE + ZCARD) - a standard, idiomatic
     Redis pattern for real-time rate/velocity tracking.
  3. Score the transaction (pipeline/scoring_rules.py).
  4. Record end-to-end latency (now vs. the event's own timestamp) -
     the actual, measured proof this is real-time, not batch.
  5. Write the scored result to DuckDB and XACK the message.

Usage:
    python pipeline/consumer.py --duration 200
(run concurrently with scripts/producer.py, or after it - either order
is safe since the consumer group starts reading from the beginning)
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from pipeline.redis_client import (get_client, STREAM_RAW, CONSUMER_GROUP, CONSUMER_NAME,  # noqa: E402
                                     VELOCITY_KEY_PREFIX, VELOCITY_WINDOW_SECONDS)
from pipeline.scoring_rules import score_transaction  # noqa: E402

DATA_DIR = ROOT / "data"
DB_PATH = ROOT / "database" / "streaming.duckdb"

SCHEMA = """
CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    amount DOUBLE,
    country TEXT,
    device_id TEXT,
    category TEXT,
    merchant_id TEXT,
    event_ts DOUBLE,
    processed_ts DOUBLE,
    latency_ms DOUBLE,
    recent_count INTEGER,
    risk_score INTEGER,
    risk_level TEXT,
    reasons TEXT,
    scenario_label TEXT
);
"""


def ensure_consumer_group(client: redis.Redis):
    try:
        client.xgroup_create(STREAM_RAW, CONSUMER_GROUP, id="0", mkstream=True)
        print(f"Created consumer group '{CONSUMER_GROUP}' on stream '{STREAM_RAW}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists - resuming.")
        else:
            raise


def update_velocity(client: redis.Redis, account_id: str, ts: float) -> int:
    key = f"{VELOCITY_KEY_PREFIX}{account_id}"
    pipe = client.pipeline()
    pipe.zadd(key, {f"{ts}:{account_id}": ts})
    pipe.zremrangebyscore(key, 0, ts - VELOCITY_WINDOW_SECONDS)
    pipe.zcard(key)
    pipe.expire(key, VELOCITY_WINDOW_SECONDS * 2)
    _, _, count, _ = pipe.execute()
    return int(count)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=200, help="seconds to run before exiting")
    parser.add_argument("--idle-timeout", type=int, default=15,
                         help="stop early if no new events arrive for this many seconds")
    args = parser.parse_args()

    accounts_df = pd.read_csv(DATA_DIR / "accounts.csv").set_index("account_id")
    accounts = accounts_df.to_dict(orient="index")
    print(f"Loaded {len(accounts):,} account profiles.")

    client = get_client()
    client.ping()
    ensure_consumer_group(client)

    DB_PATH.parent.mkdir(exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(SCHEMA)

    start = time.time()
    last_event_time = time.time()
    processed, flagged = 0, 0
    latencies = []

    print(f"Consumer running (duration={args.duration}s, idle_timeout={args.idle_timeout}s)...")

    while time.time() - start < args.duration:
        resp = client.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME,
                                   {STREAM_RAW: ">"}, count=10, block=1000)
        if not resp:
            if time.time() - last_event_time > args.idle_timeout:
                print(f"No new events for {args.idle_timeout}s - stopping.")
                break
            continue

        for _stream_name, messages in resp:
            for msg_id, fields in messages:
                last_event_time = time.time()
                account_id = fields.get("account_id")
                account = accounts.get(account_id)
                if account is None:
                    client.xack(STREAM_RAW, CONSUMER_GROUP, msg_id)
                    continue

                event = {
                    "amount": float(fields["amount"]),
                    "country": fields["country"],
                    "device_id": fields["device_id"],
                    "category": fields["category"],
                }
                event_ts = float(fields["event_ts"])

                recent_count = update_velocity(client, account_id, event_ts)
                result = score_transaction(event, account, recent_count)

                processed_ts = time.time()
                latency_ms = (processed_ts - event_ts) * 1000
                latencies.append(latency_ms)

                conn.execute("""
                    INSERT OR REPLACE INTO transaction_scores VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, [
                    fields["transaction_id"], account_id, event["amount"], event["country"],
                    event["device_id"], event["category"], fields["merchant_id"], event_ts,
                    processed_ts, latency_ms, recent_count, result["risk_score"],
                    result["risk_level"], " | ".join(result["reasons"]), fields.get("scenario_label", ""),
                ])

                processed += 1
                if result["risk_level"] in ("High", "Medium"):
                    flagged += 1
                    print(f"  [{result['risk_level']}] {fields['transaction_id']} "
                          f"({account_id}, ${event['amount']:.2f}, {event['country']}) "
                          f"score={result['risk_score']} - {result['reasons'][0] if result['reasons'] else ''}")

                client.xack(STREAM_RAW, CONSUMER_GROUP, msg_id)

    conn.close()
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
    print(f"\nDone. Processed {processed:,} events, flagged {flagged:,} (Medium/High risk).")
    print(f"Latency: avg={avg_latency:.0f}ms, p95={p95_latency:.0f}ms (event timestamp -> scored+written)")


if __name__ == "__main__":
    main()
