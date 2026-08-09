"""
streamlit_app.py
-----------------
Live-monitoring dashboard for the real-time fraud-scoring pipeline, plus
an evaluation tab and an on-demand AI explanation for any flagged
transaction. Reads directly from database/streaming.duckdb - the table
pipeline/consumer.py writes to as it processes the Redis Stream live.

Run with:
    streamlit run dashboard/streamlit_app.py

For the "live" feel, enable "Live Mode" in the sidebar while
scripts/producer.py + pipeline/consumer.py are running concurrently in
other terminals - the dashboard will poll and refresh automatically.
"""

import json
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.explain import explain_alert  # noqa: E402

DB_PATH = ROOT / "database" / "streaming.duckdb"
REPORTS_DIR = ROOT / "reports"

st.set_page_config(page_title="Real-Time Fraud Monitor", layout="wide", page_icon="⚡")


def load_data():
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("SELECT * FROM transaction_scores ORDER BY event_ts DESC").fetchdf()
    conn.close()
    if len(df):
        df["reasons_list"] = df["reasons"].apply(lambda s: [r.strip() for r in s.split("|")] if s else [])
    return df


st.sidebar.title("Controls")
live_mode = st.sidebar.checkbox("🔴 Live Mode (auto-refresh every 3s)", value=False)
st.sidebar.caption("Run `python scripts/producer.py` and `python pipeline/consumer.py` in separate "
                    "terminals, then enable Live Mode here to watch the feed update in real time.")

df = load_data()

st.title("⚡ Real-Time Transaction Fraud Monitor")
st.caption("Redis Streams (consumer group, XREADGROUP/XACK) → rules-based scoring → DuckDB. "
           "Every number below is read live from what the streaming consumer has actually processed.")

tab_monitor, tab_eval = st.tabs(["📡 Live Monitor", "🎯 Detection Evaluation"])

# ============================================================================
# TAB 1: Live Monitor
# ============================================================================
with tab_monitor:
    if df.empty:
        st.info("No events processed yet. Start the producer and consumer, then refresh.")
    else:
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Events Processed", f"{len(df):,}")
        flagged = df[df.risk_level.isin(["Medium", "High"])]
        k2.metric("Flagged (Medium/High)", f"{len(flagged):,}")
        k3.metric("High Risk", f"{(df.risk_level == 'High').sum():,}")
        k4.metric("Avg Latency", f"{df.latency_ms.mean():.0f} ms")
        k5.metric("P95 Latency", f"{df.latency_ms.quantile(0.95):.0f} ms")

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Risk Level Distribution")
            counts = df.risk_level.value_counts().reindex(["Low", "Medium", "High"]).fillna(0)
            fig1 = px.bar(x=counts.index, y=counts.values,
                           color=counts.index,
                           color_discrete_map={"Low": "#2E6F40", "Medium": "#E1A100", "High": "#C0392B"},
                           labels={"x": "", "y": "Transactions"})
            fig1.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)

        with c2:
            st.subheader("Processing Latency Distribution")
            fig2 = px.histogram(df, x="latency_ms", nbins=40, labels={"latency_ms": "Latency (ms)"})
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)

        st.divider()

        st.subheader("🚨 Flagged Transaction Feed")
        show_cols = ["transaction_id", "account_id", "amount", "country", "device_id",
                     "category", "risk_score", "risk_level", "reasons"]
        flagged_sorted = flagged[show_cols].sort_values("risk_score", ascending=False) if len(flagged) else flagged
        st.dataframe(flagged_sorted, use_container_width=True, height=280)

        st.subheader("🤖 Explain a Flagged Transaction")
        if len(flagged):
            txn_options = flagged_sorted["transaction_id"].tolist()
            selected_txn = st.selectbox("Pick a transaction to explain", txn_options)
            if st.button("Generate AI Explanation"):
                row = df[df.transaction_id == selected_txn].iloc[0]
                with st.spinner("Generating explanation locally (Ollama qwen2.5:0.5b)..."):
                    explanation = explain_alert({
                        "account_id": row.account_id, "amount": row.amount, "category": row.category,
                        "country": row.country, "risk_level": row.risk_level,
                        "risk_score": row.risk_score, "reasons": row.reasons_list,
                    })
                st.success(explanation)
        else:
            st.caption("No flagged transactions yet.")

    if live_mode:
        time.sleep(3)
        st.rerun()

# ============================================================================
# TAB 2: Detection Evaluation
# ============================================================================
with tab_eval:
    eval_path = REPORTS_DIR / "detection_evaluation.json"
    if not eval_path.exists():
        st.info("Run `python scripts/evaluate_detection.py` after the pipeline finishes to see results here.")
    else:
        with open(eval_path, encoding="utf-8") as f:
            ev = json.load(f)

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Precision", f"{ev['precision']:.1%}")
        k2.metric("Recall", f"{ev['recall']:.1%}")
        k3.metric("F1 Score", f"{ev['f1_score']:.3f}")
        k4.metric("Total Fraud Events (ground truth)", ev["total_fraud_events"])

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Confusion Matrix")
            cm = ev["confusion_matrix"]
            cm_df = pd.DataFrame([
                ["Actual Fraud", cm["true_positive"], cm["false_negative"]],
                ["Actual Normal", cm["false_positive"], cm["true_negative"]],
            ], columns=["", "Flagged", "Not Flagged"]).set_index("")
            st.dataframe(cm_df, use_container_width=True)

        with c2:
            st.subheader("Catch Rate by Scenario Type")
            scen_df = pd.DataFrame(ev["by_scenario"])
            if len(scen_df):
                fig3 = px.bar(scen_df, x="scenario_label", y="catch_rate",
                               labels={"scenario_label": "", "catch_rate": "Catch Rate %"},
                               color="catch_rate", color_continuous_scale="RdYlGn", range_color=[0, 100])
                fig3.update_layout(height=350)
                st.plotly_chart(fig3, use_container_width=True)
                st.dataframe(scen_df, use_container_width=True)

        st.divider()
        st.subheader("Latency")
        st.json(ev["latency"])
