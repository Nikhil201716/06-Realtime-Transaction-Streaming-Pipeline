"""
generate_preview_images.py
---------------------------
Renders static PNG chart previews (matplotlib) straight from the real
pipeline run - database/streaming.duckdb and reports/detection_evaluation.json
- for the README. This build environment has no display to screenshot the
live Streamlit app, so these are real charts built from the exact same data
the dashboard reads.

Output: ../screenshots/*.png
"""

import json
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "database" / "streaming.duckdb"
REPORTS_DIR = ROOT / "reports"
OUT_DIR = ROOT / "screenshots"
OUT_DIR.mkdir(exist_ok=True)

NAVY, ACCENT, RED, GOLD = "#1F3A5F", "#2E6F40", "#C0392B", "#E1A100"

conn = duckdb.connect(str(DB_PATH), read_only=True)
df = conn.execute("SELECT * FROM transaction_scores").fetchdf()
conn.close()

with open(REPORTS_DIR / "detection_evaluation.json", encoding="utf-8") as f:
    ev = json.load(f)

# ------------------------------------------------------------------
# 1. KPI summary
# ------------------------------------------------------------------
fig, axes = plt.subplots(1, 5, figsize=(15, 2.2))
cards = [
    ("Events Processed", f"{len(df):,}"),
    ("Flagged (Med/High)", f"{ev['total_flagged']}"),
    ("Precision", f"{ev['precision']:.0%}"),
    ("Recall", f"{ev['recall']:.0%}"),
    ("Avg Latency", f"{ev['latency']['avg_latency_ms']:.0f} ms"),
]
for ax, (label, value) in zip(axes, cards):
    ax.axis("off")
    ax.add_patch(plt.Rectangle((0, 0), 1, 1, color=NAVY, transform=ax.transAxes, zorder=0))
    ax.text(0.5, 0.68, label, ha="center", va="center", color="white", fontsize=10, transform=ax.transAxes)
    ax.text(0.5, 0.32, value, ha="center", va="center", color="white", fontsize=15, fontweight="bold", transform=ax.transAxes)
fig.suptitle("Real-Time Fraud Monitor - Key Metrics (real pipeline run)", fontsize=12, color=NAVY, y=1.08)
plt.tight_layout()
plt.savefig(OUT_DIR / "01_kpi_summary.png", dpi=150, bbox_inches="tight")
plt.close()

# ------------------------------------------------------------------
# 2. Risk level distribution + Catch rate by scenario
# ------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
counts = df.risk_level.value_counts().reindex(["Low", "Medium", "High"]).fillna(0)
axes[0].bar(counts.index, counts.values, color=[ACCENT, GOLD, RED])
axes[0].set_title(f"Risk Level Distribution ({len(df)} events)", color=NAVY, fontweight="bold")
axes[0].set_ylabel("Transactions")

scen_df = pd.DataFrame(ev["by_scenario"])
axes[1].bar(scen_df.scenario_label, scen_df.catch_rate, color=NAVY)
axes[1].set_title("Catch Rate by Injected Fraud Scenario", color=NAVY, fontweight="bold")
axes[1].set_ylabel("Catch Rate %")
axes[1].set_ylim(0, 110)
for i, v in enumerate(scen_df.catch_rate):
    axes[1].text(i, v + 2, f"{v}%", ha="center", fontsize=9)
plt.tight_layout()
plt.savefig(OUT_DIR / "02_risk_and_catch_rate.png", dpi=150, bbox_inches="tight")
plt.close()

# ------------------------------------------------------------------
# 3. Processing latency distribution (the real-time-ness proof)
# ------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 4.5))
ax.hist(df.latency_ms, bins=30, color=NAVY, alpha=0.85)
ax.axvline(ev["latency"]["avg_latency_ms"], color=GOLD, linestyle="--", linewidth=2,
           label=f"avg = {ev['latency']['avg_latency_ms']:.1f} ms")
ax.axvline(ev["latency"]["p95_latency_ms"], color=RED, linestyle="--", linewidth=2,
           label=f"p95 = {ev['latency']['p95_latency_ms']:.1f} ms")
ax.set_xlabel("Latency: event timestamp -> scored + written to DuckDB (ms)")
ax.set_ylabel("Count")
ax.set_title("End-to-End Processing Latency (measured, not simulated)", color=NAVY, fontweight="bold")
ax.legend()
plt.tight_layout()
plt.savefig(OUT_DIR / "03_latency_distribution.png", dpi=150, bbox_inches="tight")
plt.close()

print("Saved 3 preview images to", OUT_DIR)
