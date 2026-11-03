# Optional cloud deployment: Amazon MSK -> AWS Lambda -> Snowflake

**The repository does not provision AWS or Snowflake accounts. This deployment requires existing accounts, permissions, billing controls, networking, and a reachable MSK topic. These resources can incur charges.**

## 1. Set up Snowflake

1. Select a Snowflake role allowed to create a database and schemas.
2. Run `sql/setup.sql` in a Snowflake SQL worksheet.
3. Create or select a small warehouse, set auto-suspend/auto-resume, and grant the Lambda Snowflake user only INSERT/MERGE privileges on `EVENT_ANALYTICS.RAW.RAW_EVENTS`, plus USAGE on its database, schema and warehouse.
4. In AWS Secrets Manager create a secret JSON payload such as:

```json
{"account":"your-org-your-account", "user":"PIPELINE_WRITER", "password":"YOUR_SECRET", "warehouse":"COMPUTE_WH", "database":"EVENT_ANALYTICS", "schema":"RAW", "role":"PIPELINE_WRITER_ROLE"}
```

Do not commit a real secret to the repository. Prefer credential rotation/key-pair or scoped OAuth for longer-lived deployments.

## 2. Existing Amazon MSK topic

Use an accessible Amazon MSK cluster with a `transaction-events` topic. Configure IAM/SASL authentication as appropriate, ensure Lambda can reach MSK's brokers, and grant the function role the required cluster/topic/consumer-group permissions. For end-to-end Snowflake writes, Lambda needs egress to the Snowflake endpoint (NAT or approved network path if in a private VPC). Do not expose local Redpanda ports publicly.

Important: the Docker Compose Redpanda broker is *only* a local development server and **is not** the existing MSK cluster required by the SAM example. For externally hosted Kafka, adapt the event source mapping to the AWS self-managed Kafka option, security configuration, and network access.

## 3. Deploy the containerized Lambda with AWS SAM

Install Docker, AWS CLI, and AWS SAM CLI; configure an AWS identity with necessary IAM, ECR, CloudFormation, Lambda, MSK and Secrets Manager privileges.

From the repository root run:

```bash
sam build --template-file aws/template.yaml
sam deploy --guided --resolve-image-repos \
  --parameter-overrides \
  ParameterKey=MskClusterArn,ParameterValue=YOUR_MSK_CLUSTER_ARN \
  ParameterKey=KafkaTopic,ParameterValue=transaction-events \
  ParameterKey=SnowflakeSecretArn,ParameterValue=YOUR_SECRET_ARN
```

The template uses `Dockerfile.lambda`: it installs project dependencies, trains a **synthetic-only model** inside the image, and invokes `event_pipeline.lambda_handler.handler` for Kafka batches. The image ships sklearn dependencies without relying on ZIP package limits.

The SAM example is a **deployment starting point**, not an already-deployed environment. You may need additional MSK IAM policies and route/security-group adjustments, depending on your environment. Deploy first in a disposable sandbox account with spending limits.

## 4. Run dbt against Snowflake

```bash
python -m pip install 'dbt-snowflake>=1.8,<3'
# Configure SNOWFLAKE_* environment variables (use your secret manager)
# Copy dbt/profiles.yml.example to ~/.dbt/profiles.yml or point --profiles-dir at a secure profile directory.
cd dbt
dbt debug
dbt run
dbt test
```

For locally stored `.env` variables, export them into the shell before invoking dbt: `dbt` reads environment variables but does not automatically load `.env` files. `dbt run` must be scheduled (e.g., separate GitHub Actions/Step Functions/other orchestrator) to keep marts fresh; the included CI deliberately does not touch cloud credentials.

## 5. Validate and control cost

1. Publish synthetic transactions to your *MSK* topic (not the local container's Redpanda broker).
2. Watch CloudWatch logs, Kafka event-source metrics, and the Snowflake `RAW_EVENTS` count.
3. Run dbt models and query `ANALYTICS.FCT_HOURLY_EVENTS` and `ANALYTICS.FCT_ANOMALY_ALERTS`.
4. Set billing alarms/warehouse auto-suspend; delete unused MSK, Lambda, ECR, and Snowflake compute resources.

## Production enhancements

Dead-letter handling for invalid events; batch Snowflake COPY/MERGE ingestion; schema registry/versioning; secrets rotation; DLQ/retry caps; partition sizing; latency/throughput measurements; cloud-native orchestration and drift/evaluation dashboards. None of these enhancements are implied to be implemented by the project today.
