"""
explain.py
-----------
Turns a flagged transaction's rule-based reasons (already fully
computed, factual strings from scoring_rules.py) into a short, readable
fraud-analyst-style narrative using a local Ollama model - the same
qwen2.5:0.5b used in Project 5, for the same reason (this machine's ~6GB
RAM budget, and so this project costs nothing to run for anyone, ever).

Deliberately NOT a RAG/retrieval setup like Project 5's copilot - there's
no ambiguity to retrieve context for here. The rules already produced
the exact, factual reasons; the LLM's only job is turning a bullet list
into a paragraph a human reads faster than a bullet list, and suggesting
one concrete next action. It is given the reasons as fact and instructed
not to invent anything beyond them.
"""

import ollama

OLLAMA_MODEL = "qwen2.5:0.5b"

PROMPT_TEMPLATE = """A fraud detection system flagged a transaction with risk level {risk_level} \
(score {risk_score}/100). The SPECIFIC reasons it was flagged are listed below - these are the \
only facts you know; do not invent any other reason.

Transaction: ${amount:.2f} at a {category} merchant in {country}, account {account_id}.

Reasons flagged:
{reasons_bullets}

Write a 2-3 sentence summary a fraud analyst could read in 5 seconds, explaining why this was \
flagged and suggesting one concrete next action (e.g. "hold for manual review", "contact the \
cardholder to verify", "likely safe to auto-approve if this pattern repeats legitimately"). \
Do not repeat the raw numbers unnecessarily - write it as a person would explain it verbally."""


def explain_alert(transaction: dict) -> str:
    """transaction: a dict/row with amount, category, country, account_id,
    risk_level, risk_score, reasons (list of strings)."""
    reasons_bullets = "\n".join(f"- {r}" for r in transaction["reasons"]) or "- (no specific reasons - low risk)"

    prompt = PROMPT_TEMPLATE.format(
        risk_level=transaction["risk_level"],
        risk_score=transaction["risk_score"],
        amount=transaction["amount"],
        category=transaction["category"],
        country=transaction["country"],
        account_id=transaction["account_id"],
        reasons_bullets=reasons_bullets,
    )

    response = ollama.chat(
        model=OLLAMA_MODEL,
        messages=[{"role": "user", "content": prompt}],
        options={"temperature": 0.2, "num_predict": 220},
    )
    return response["message"]["content"].strip()


if __name__ == "__main__":
    demo = {
        "account_id": "ACC300123", "amount": 1450.00, "category": "Electronics",
        "country": "Russia", "risk_level": "High", "risk_score": 85,
        "reasons": [
            "Severe amount anomaly: $1450.00 is 8.2 standard deviations above this account's "
            "typical $120.00 transaction.",
            "Transaction country (Russia) differs from the account's home country (USA).",
            "Transaction made from an unrecognized device (expected the account's usual device, "
            "saw a different one).",
        ],
    }
    print(explain_alert(demo))
