"""Configuration loaded at runtime; no credentials in source control."""
import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def model_path() -> Path:
    return Path(env("MODEL_PATH", "models/anomaly_detector.joblib"))


def kafka_servers() -> str:
    return env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")


def kafka_topic() -> str:
    return env("KAFKA_TOPIC", "transaction-events")


def snowflake_credentials() -> dict:
    """Optional AWS Secrets Manager credentials; env vars suffice locally.

    Secrets Manager JSON keys: account, user, password, warehouse, database,
    schema, role. A missing secret field falls back to its environment value.
    """
    creds = {
        "account": env("SNOWFLAKE_ACCOUNT"),
        "user": env("SNOWFLAKE_USER"),
        "password": env("SNOWFLAKE_PASSWORD"),
        "warehouse": env("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": env("SNOWFLAKE_DATABASE", "EVENT_ANALYTICS"),
        "schema": env("SNOWFLAKE_SCHEMA", "RAW"),
        "role": env("SNOWFLAKE_ROLE"),
    }
    arn = env("SNOWFLAKE_SECRET_ARN")
    if arn:
        import boto3  # installed with [aws] or [all]
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=arn)
        data = json.loads(secret["SecretString"])
        creds.update({k: str(v) for k, v in data.items() if k in creds and v is not None})
    required = ["account", "user", "password"]
    missing = [k for k in required if not creds[k]]
    if missing:
        raise ValueError("Missing Snowflake settings: " + ", ".join(missing))
    return {k: v for k, v in creds.items() if v}
