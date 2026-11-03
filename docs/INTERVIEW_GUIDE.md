# How to discuss the project in an interview

## 45-second explanation

"I built a near-real-time analytics pipeline that generates synthetic transactions, streams them through Kafka, uses a trained Isolation Forest model to detect unusual patterns, and writes enriched records to an analytics data store. The local consumer uses SQLite, while the cloud implementation uses an AWS Lambda Kafka trigger and Snowflake. I modeled the Snowflake raw data into hourly analytics and anomaly-alert marts with dbt, and included unit tests, replay-safe writes, CI, and deployment documentation."

## What each technology does

| Technology | Purpose |
|---|---|
| Python | Generates events, validates the schema, executes model inference, and handles ingestion |
| Kafka / Redpanda | Buffers event streams in a topic and supports consumer groups |
| AWS Lambda | Optionally processes Kafka batches without maintaining a server; integrates with MSK |
| scikit-learn | Isolation Forest infers atypical events based on synthetic baseline transactions |
| Snowflake | Stores raw and AI-enriched records for cloud analysis |
| dbt | Tests and transforms source data into report-ready views/tables |
| GitHub Actions | Runs repeatable tests and an offline pipeline smoke test |

## Questions interviewers may ask

**Why Kafka?** It decouples transaction producers from the processing service, allows durable buffering, and provides partitioning and replay.

**What is the delivery guarantee?** Kafka/Lambda can redeliver. The consumer commits offsets after persistence and the Snowflake sink uses `MERGE` keyed by event ID; local SQLite uses a unique ID. This provides *idempotent effects for serial retries*, not distributed exactly-once transactions. Concurrent writes and upstream duplicate IDs still require stronger production controls.

**What is "real-time" here?** Kafka consumption and Lambda inference process incoming events promptly. dbt marts are refreshed **when dbt runs**, not continuously; the project is near-real-time ingestion plus scheduled analytics transformation. No hard latency SLA has been measured.

**Where is GenAI used?** A separate, optional OpenAI Responses API call summarizes anonymized synthetic anomaly alerts into a concise operational brief. This is *not* in the streaming ingestion path and is off by default, so pipeline reliability and cost do not depend on the LLM.

**How is the model trained?** An Isolation Forest trained offline with 2,000 normal synthetic transactions. It uses log amount, category-relative amount, cyclical time, channel and a country flag. The compact feature design reduces irrelevant feature noise. The `risk_score` is uncalibrated, and the explanatory reason codes are heuristic rather than formal explainability (SHAP).

**How is quality measured?** `event-pipeline evaluate` reports synthetic holdout confusion counts, precision, recall and F1. The labels are simulator-defined, so these are illustrative test results, not real-world detection accuracy.

**How do you test it?** Contract validation tests, model save/reload reproducibility, SQLite duplicate suppression, Lambda Kafka-event decoding, a full offline simulation, and CI.

**What would you improve in production?** Secrets rotation and private networking, dead-letter topic, schema registry, batch writes and throughput benchmarks, model evaluation with real labeled data and drift monitoring, orchestrated dbt runs, and cloud cost controls.

## Resume bullet (use after you have executed the project)

- Developed a Python/Kafka event-processing pipeline with Isolation Forest anomaly detection, an AWS Lambda ingestion option, idempotent Snowflake writes, and dbt analytics marts; implemented unit tests and GitHub Actions CI.

**Do not claim a production AWS/Snowflake deployment, verified throughput, or real business results until you have performed and measured them.**
