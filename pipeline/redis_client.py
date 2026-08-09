"""
redis_client.py
----------------
Shared connection + naming constants for every part of this pipeline
that talks to Redis. Redis was compiled from the official source
(download.redis.io) and run directly - no Docker, no Windows service
installer - because the RAM-friendlier Windows path (Memurai) hit an
installer bug on this machine (see docs/architecture.md). WSL already
had gcc/make available, so a from-source build was the most reliable
path forward without needing sudo/apt.
"""

import os
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

STREAM_RAW = "transactions:raw"
CONSUMER_GROUP = "fraud-scorers"
CONSUMER_NAME = "scorer-1"

VELOCITY_KEY_PREFIX = "velocity:"       # sorted-set sliding window per account
VELOCITY_WINDOW_SECONDS = 60


def get_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
