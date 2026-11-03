"""AWS MSK/self-managed Kafka -> AWS Lambda batch payload adapter.

AWS Lambda retries Kafka batches on exceptions; Snowflake MERGE makes replays
idempotent. Do not silently acknowledge failed inserts.
"""
import base64
import json
from .ai import load_detector
from .config import env, model_path
from .events import TransactionEvent
from .processor import EventProcessor
from .sinks import make_sink


def extract_events(lambda_event: dict) -> list[TransactionEvent]:
    parsed = []
    records = lambda_event.get("records")
    if not isinstance(records, dict):
        raise ValueError("Lambda event must contain a Kafka 'records' mapping")
    for partition_records in records.values():
        for record in partition_records:
            raw = base64.b64decode(record["value"], validate=True)
            parsed.append(TransactionEvent.from_json(raw))
    return parsed


def handler(event: dict, context) -> dict:
    events = extract_events(event)
    if not events:
        return {"processed": 0, "ai_alerts": 0}
    sink = make_sink(env("SINK", "snowflake"), env("SQLITE_PATH", "data/events.sqlite"))
    try:
        processor = EventProcessor(load_detector(str(model_path())), sink)
        results = processor.process_many(events)
        return {"processed": len(results), "ai_alerts": sum(x.is_anomaly for x in results)}
    finally:
        sink.close()
