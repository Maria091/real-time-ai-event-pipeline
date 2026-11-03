# Locally verified demonstration (synthetic-only)

These figures were obtained by executing the actual Python pipeline and tests in a sandbox on October 25, 2026. They are **not** claims of a deployed cloud pipeline or of real-world fraud performance.

## Commands

```bash
python -m pytest -q
python -m event_pipeline.cli demo --count 150 --database data/clean-demo.sqlite --export data/clean-alerts.csv
python -m event_pipeline.cli evaluate --count 1000 --seed 73
```

## Observed outputs

| Check | Observed result |
|---|---:|
| Automated tests | 14 passed |
| Offline synthetic demo | 150 ingested events |
| ML anomaly alerts in offline demo | 16 |
| Total simulated monetary amount | $43,518.39 |
| Holdout evaluation samples (synthetic labels) | 1,000 |
| True positives | 108 |
| False positives | 12 |
| True negatives | 880 |
| False negatives | 0 |
| Synthetic precision | 90.0% |
| Synthetic recall | 100.0% |
| Synthetic F1 | 94.7% |

The generator creates high-amount, cross-border outliers by design. The model is therefore tested on the same general *kind* of anomalies that the simulator makes, even though the holdout sample IDs/data differ from the training sample. Do **not** extrapolate these scores to real payment processing, unseen anomaly classes, or human-reviewed fraud outcomes. Your own output may differ with software versions, clock hour and changed model parameters.

The SQLite pipeline and Lambda payload adapter were tested locally. Kafka broker, AWS MSK/Lambda infrastructure, Snowflake writes, live OpenAI API, and dbt execution against Snowflake **were not** exercised in this sandbox because those external services/credentials were unavailable.
