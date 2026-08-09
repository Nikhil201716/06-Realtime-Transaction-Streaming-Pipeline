"""
evaluate_detection.py
-----------------------
Measures how well the rules engine actually did, using the
`scenario_label` ground-truth field the producer attached to each event
purely for this evaluation - and which pipeline/scoring_rules.py never
sees or uses while scoring. This is the same "inject a known incident,
then measure detection against it" pattern used in every prior project
in this portfolio, applied here to a real-time detection rate instead
of a batch data-quality check.

Ground truth: any event with scenario_label != "normal" is fraud.
Prediction: any event with risk_level in (Medium, High) is flagged.

Output: reports/detection_evaluation.json + .md
"""

import json
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "database" / "streaming.duckdb"
REPORTS_DIR = ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

conn = duckdb.connect(str(DB_PATH), read_only=True)
df = conn.execute("SELECT * FROM transaction_scores").fetchdf()
conn.close()

df["is_fraud"] = df["scenario_label"] != "normal"
df["is_flagged"] = df["risk_level"].isin(["Medium", "High"])

tp = int(((df.is_fraud) & (df.is_flagged)).sum())
fp = int(((~df.is_fraud) & (df.is_flagged)).sum())
fn = int(((df.is_fraud) & (~df.is_flagged)).sum())
tn = int(((~df.is_fraud) & (~df.is_flagged)).sum())

precision = tp / (tp + fp) if (tp + fp) else 0
recall = tp / (tp + fn) if (tp + fn) else 0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

by_scenario = df[df.is_fraud].groupby("scenario_label").agg(
    total=("transaction_id", "count"),
    caught=("is_flagged", "sum"),
).reset_index()
by_scenario["catch_rate"] = (by_scenario.caught / by_scenario.total * 100).round(1)

latency_stats = {
    "avg_latency_ms": round(df.latency_ms.mean(), 1),
    "p95_latency_ms": round(df.latency_ms.quantile(0.95), 1),
    "max_latency_ms": round(df.latency_ms.max(), 1),
}

summary = {
    "total_events": len(df),
    "total_fraud_events": int(df.is_fraud.sum()),
    "total_flagged": int(df.is_flagged.sum()),
    "confusion_matrix": {"true_positive": tp, "false_positive": fp, "false_negative": fn, "true_negative": tn},
    "precision": round(precision, 3),
    "recall": round(recall, 3),
    "f1_score": round(f1, 3),
    "by_scenario": by_scenario.to_dict(orient="records"),
    "latency": latency_stats,
}

with open(REPORTS_DIR / "detection_evaluation.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

with open(REPORTS_DIR / "detection_evaluation.md", "w", encoding="utf-8") as f:
    f.write("# Real-Time Fraud Detection: Evaluation Against Known-Injected Scenarios\n\n")
    f.write(f"**Precision: {precision:.1%} · Recall: {recall:.1%} · F1: {f1:.3f}**\n\n")
    f.write(f"- True Positives (fraud, correctly flagged): {tp}\n")
    f.write(f"- False Positives (normal, incorrectly flagged): {fp}\n")
    f.write(f"- False Negatives (fraud, MISSED): {fn}\n")
    f.write(f"- True Negatives (normal, correctly not flagged): {tn}\n\n")
    f.write("## Catch rate by scenario type\n\n")
    f.write(by_scenario.to_string(index=False) + "\n\n")
    f.write("## Processing latency (event timestamp -> scored + written to warehouse)\n\n")
    for k, v in latency_stats.items():
        f.write(f"- {k}: {v} ms\n")

print(json.dumps(summary, indent=2))
print(f"\nSaved to {REPORTS_DIR / 'detection_evaluation.json'} and .md")
