"""Durable sinks with idempotency keyed by event_id."""
import json
import sqlite3
from pathlib import Path
from .ai import AnomalyResult
from .events import TransactionEvent

SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    event_time TEXT NOT NULL,
    user_id TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    amount REAL NOT NULL,
    country TEXT NOT NULL,
    channel TEXT NOT NULL,
    device_id TEXT NOT NULL,
    is_simulated_anomaly INTEGER NOT NULL,
    ai_is_anomaly INTEGER NOT NULL,
    ai_score REAL NOT NULL,
    ai_reasons TEXT NOT NULL,
    event_payload TEXT NOT NULL,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_raw_events_time ON raw_events(event_time);
CREATE INDEX IF NOT EXISTS idx_raw_events_alert ON raw_events(ai_is_anomaly);
"""


class SQLiteSink:
    def __init__(self, path: str = "data/events.sqlite"):
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path_obj))
        self.conn.executescript(SQLITE_DDL)

    def upsert(self, event: TransactionEvent, result: AnomalyResult) -> None:
        # INSERT OR IGNORE makes at-least-once Kafka/Lambda retries safe for duplicate IDs.
        self.conn.execute("""
            INSERT OR IGNORE INTO raw_events
            (event_id,event_time,user_id,merchant_category,amount,country,channel,
            device_id,is_simulated_anomaly,ai_is_anomaly,ai_score,ai_reasons,event_payload)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (event.event_id, event.event_time, event.user_id, event.merchant_category,
              event.amount, event.country, event.channel, event.device_id,
              int(event.is_simulated_anomaly), int(result.is_anomaly), result.risk_score,
              json.dumps(list(result.reasons)), event.to_json()))
        self.conn.commit()

    def summary(self) -> dict:
        row = self.conn.execute("SELECT COUNT(*),COALESCE(SUM(ai_is_anomaly),0),COALESCE(SUM(amount),0) FROM raw_events").fetchone()
        return {"events": row[0], "ai_alerts": row[1], "total_amount": round(row[2], 2)}

    def alerts(self, limit: int = 10) -> list[tuple]:
        return self.conn.execute("""
            SELECT event_time,event_id,ROUND(amount,2),country,ROUND(ai_score,1),ai_reasons
            FROM raw_events WHERE ai_is_anomaly=1 ORDER BY ai_score DESC LIMIT ?
        """, (limit,)).fetchall()

    def close(self) -> None:
        self.conn.close()


class SnowflakeSink:
    """Snowflake MERGE prevents duplicate event IDs across Kafka retry batches."""
    # The raw table must already exist: run sql/setup.sql.
    MERGE_SQL = """
        MERGE INTO RAW_EVENTS target USING (
          SELECT %s AS EVENT_ID,
                 TO_TIMESTAMP_TZ(%s) AS EVENT_TIME,
                 %s AS USER_ID, %s AS MERCHANT_CATEGORY, %s AS AMOUNT,
                 %s AS COUNTRY, %s AS CHANNEL, %s AS DEVICE_ID,
                 %s AS IS_SIMULATED_ANOMALY, %s AS AI_IS_ANOMALY,
                 %s AS AI_SCORE, %s AS AI_REASONS,
                 PARSE_JSON(%s) AS EVENT_PAYLOAD
        ) src
        ON target.EVENT_ID = src.EVENT_ID
        WHEN NOT MATCHED THEN INSERT (
          EVENT_ID,EVENT_TIME,USER_ID,MERCHANT_CATEGORY,AMOUNT,COUNTRY,
          CHANNEL,DEVICE_ID,IS_SIMULATED_ANOMALY,AI_IS_ANOMALY,
          AI_SCORE,AI_REASONS,EVENT_PAYLOAD
        ) VALUES (
          src.EVENT_ID,src.EVENT_TIME,src.USER_ID,src.MERCHANT_CATEGORY,src.AMOUNT,
          src.COUNTRY,src.CHANNEL,src.DEVICE_ID,src.IS_SIMULATED_ANOMALY,
          src.AI_IS_ANOMALY,src.AI_SCORE,src.AI_REASONS,src.EVENT_PAYLOAD
        )
    """

    def __init__(self, credentials: dict | None = None):
        import snowflake.connector  # optional [snowflake] extra
        from .config import snowflake_credentials
        self.conn = snowflake.connector.connect(**(credentials or snowflake_credentials()))

    def upsert(self, event: TransactionEvent, result: AnomalyResult) -> None:
        values = (event.event_id, event.event_time, event.user_id,
                  event.merchant_category, event.amount, event.country, event.channel,
                  event.device_id, event.is_simulated_anomaly, result.is_anomaly,
                  result.risk_score, json.dumps(list(result.reasons)), event.to_json())
        with self.conn.cursor() as cursor:
            cursor.execute(self.MERGE_SQL, values)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def make_sink(kind: str, sqlite_path: str = "data/events.sqlite"):
    if kind == "sqlite":
        return SQLiteSink(sqlite_path)
    if kind == "snowflake":
        return SnowflakeSink()
    raise ValueError("sink must be either sqlite or snowflake")
