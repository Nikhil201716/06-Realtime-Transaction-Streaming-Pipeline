# Real-Time Fraud Detection: Evaluation Against Known-Injected Scenarios

**Precision: 97.2% · Recall: 79.5% · F1: 0.875**

- True Positives (fraud, correctly flagged): 35
- False Positives (normal, incorrectly flagged): 1
- False Negatives (fraud, MISSED): 9
- True Negatives (normal, correctly not flagged): 370

## Catch rate by scenario type

   scenario_label  total  caught  catch_rate
 account_takeover      6       6       100.0
     card_testing     23      14        60.9
stolen_card_spree     15      15       100.0

## Processing latency (event timestamp -> scored + written to warehouse)

- avg_latency_ms: 6.2 ms
- p95_latency_ms: 21.1 ms
- max_latency_ms: 30.4 ms
