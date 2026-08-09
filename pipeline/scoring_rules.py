"""
scoring_rules.py
------------------
The rules-based fraud scoring logic, kept separate from the Redis
plumbing in consumer.py so it can be unit-tested and read on its own.

4 rules, each contributing points to a 0-100 risk score, and each
producing a plain-English reason string - which is what makes the
downstream AI explanation feature possible without an LLM having to
invent anything: the "why" is already computed here, in code, and the
LLM's job (pipeline/explain.py) is only to turn a list of factual
reasons into a readable narrative, not to guess at causes itself.

IMPORTANT: this function only ever sees the fields a real fraud system
would actually have at transaction time (amount, country, device,
category, timestamp, and the account's own historical profile) - it
never sees `scenario_label`, which exists purely for later evaluation
(scripts/evaluate_detection.py). That separation is what makes the
evaluation honest.
"""

RISK_THRESHOLDS = {"High": 60, "Medium": 30}


def score_transaction(event: dict, account: dict, recent_count: int) -> dict:
    """
    event: dict with amount, country, device_id, category, account_id
    account: dict with avg_transaction_amount, std_transaction_amount,
             home_country, primary_device_id
    recent_count: number of transactions from this account in the last
                  60 seconds (INCLUDING this one), from the Redis
                  sliding-window sorted set - see consumer.py
    """
    score = 0
    reasons = []

    # Rule 1: velocity
    if recent_count >= 4:
        score += 35
        reasons.append(
            f"High velocity: {recent_count} transactions from this account in the last 60 seconds "
            f"(a normal account rarely exceeds 2-3)."
        )

    # Rule 2: amount anomaly (z-score vs. this account's own historical baseline)
    std = max(account["std_transaction_amount"], 1.0)
    z = (event["amount"] - account["avg_transaction_amount"]) / std
    if z > 6:
        score += 40
        reasons.append(
            f"Severe amount anomaly: ${event['amount']:.2f} is {z:.1f} standard deviations above "
            f"this account's typical ${account['avg_transaction_amount']:.2f} transaction."
        )
    elif z > 3:
        score += 20
        reasons.append(
            f"Amount anomaly: ${event['amount']:.2f} is {z:.1f} standard deviations above this "
            f"account's typical ${account['avg_transaction_amount']:.2f} transaction."
        )

    # Rule 3: new/foreign country
    if event["country"] != account["home_country"]:
        score += 25
        reasons.append(
            f"Transaction country ({event['country']}) differs from the account's home country "
            f"({account['home_country']})."
        )

    # Rule 4: unrecognized device
    if event["device_id"] != account["primary_device_id"]:
        score += 20
        reasons.append(
            f"Transaction made from an unrecognized device (expected the account's usual device, "
            f"saw a different one)."
        )

    score = min(score, 100)
    if score >= RISK_THRESHOLDS["High"]:
        risk_level = "High"
    elif score >= RISK_THRESHOLDS["Medium"]:
        risk_level = "Medium"
    else:
        risk_level = "Low"

    return {"risk_score": score, "risk_level": risk_level, "reasons": reasons}
