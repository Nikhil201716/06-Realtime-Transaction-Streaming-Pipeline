"""
generate_reference_data.py
----------------------------
Generates the ACCOUNT REFERENCE data the real-time fraud-scoring rules
engine compares every live transaction against: each account's typical
spend amount, home country, primary device, and usual merchant
categories. Without this "known normal" baseline, a streaming processor
can't tell a normal $40 grocery purchase from a genuinely anomalous one -
this is exactly the kind of slowly-changing reference data a real-time
system joins against on every event.

This is a ONE-TIME batch step (reference data doesn't need to be
streamed) - scripts/producer.py and pipeline/consumer.py both read this
file directly.

No real customer or account data is used anywhere in this project.

Output: data/accounts.csv
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

SEED = 7
rng = np.random.default_rng(SEED)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

N_ACCOUNTS = 2000

COUNTRIES = ["USA", "Canada", "UK", "Germany", "India", "Australia", "Brazil", "Japan"]
COUNTRY_WEIGHTS = [0.35, 0.10, 0.12, 0.09, 0.12, 0.07, 0.08, 0.07]

CATEGORIES = ["Grocery", "Restaurants", "Fuel", "Electronics", "Travel", "Entertainment",
              "Utilities", "Apparel", "Pharmacy", "Online Retail"]

RISK_TIERS = pd.DataFrame([("Standard", 0.80), ("New Account", 0.12), ("Premium", 0.08)],
                           columns=["tier", "weight"])

account_ids = [f"ACC{300000 + i}" for i in range(N_ACCOUNTS)]
home_countries = rng.choice(COUNTRIES, size=N_ACCOUNTS, p=COUNTRY_WEIGHTS)

# Average transaction size varies a lot by account (this heterogeneity is
# exactly why a flat "flag anything over $500" rule would be naive - a
# premium account's normal $800 purchase and a student account's normal
# $15 purchase need different baselines).
avg_amounts = rng.lognormal(mean=3.9, sigma=0.7, size=N_ACCOUNTS).clip(8, 900)
std_amounts = avg_amounts * rng.uniform(0.25, 0.45, size=N_ACCOUNTS)   # typical spend variability

primary_devices = [f"DEV{rng.integers(100000, 999999)}" for _ in range(N_ACCOUNTS)]

# Each account has 2-4 categories it normally shops in
typical_categories = [
    ",".join(rng.choice(CATEGORIES, size=int(rng.integers(2, 5)), replace=False))
    for _ in range(N_ACCOUNTS)
]

risk_tiers = rng.choice(RISK_TIERS.tier, size=N_ACCOUNTS, p=RISK_TIERS.weight)
opened_dates = [(datetime(2026, 7, 30) - timedelta(days=int(d))).date().isoformat()
                for d in rng.integers(15, 2200, size=N_ACCOUNTS)]

accounts = pd.DataFrame({
    "account_id": account_ids,
    "home_country": home_countries,
    "avg_transaction_amount": avg_amounts.round(2),
    "std_transaction_amount": std_amounts.round(2),
    "primary_device_id": primary_devices,
    "typical_categories": typical_categories,
    "risk_tier": risk_tiers,
    "account_opened_date": opened_dates,
})

accounts.to_csv(DATA_DIR / "accounts.csv", index=False)
print(f"Generated {len(accounts):,} account profiles -> {DATA_DIR / 'accounts.csv'}")
print(accounts.head(3).to_string(index=False))
