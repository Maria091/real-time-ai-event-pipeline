"""Validated, versioned synthetic transaction event contracts."""
import json
import math
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any
from uuid import UUID

CATEGORIES = ("grocery", "travel", "electronics", "dining", "fuel", "retail")
CHANNELS = ("web", "mobile", "store")


@dataclass(frozen=True)
class TransactionEvent:
    event_id: str
    event_time: str
    user_id: str
    merchant_category: str
    amount: float
    country: str
    channel: str
    device_id: str
    is_simulated_anomaly: bool = False
    schema_version: int = 1

    def __post_init__(self):
        try:
            UUID(self.event_id)
            ts = datetime.fromisoformat(self.event_time.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                raise ValueError("missing timezone")
        except (TypeError, ValueError) as exc:
            raise ValueError("event_id must be UUID and event_time must be timezone-aware ISO8601") from exc
        if not self.user_id or not self.device_id:
            raise ValueError("user_id and device_id are required")
        if self.merchant_category not in CATEGORIES or self.channel not in CHANNELS:
            raise ValueError("invalid category or channel")
        if not isinstance(self.amount, (int, float)) or not math.isfinite(self.amount) or self.amount <= 0:
            raise ValueError("amount must be finite and greater than zero")
        if not isinstance(self.country, str) or len(self.country) != 2 or not self.country.isalpha():
            raise ValueError("country must be a 2-letter country code")
        if not isinstance(self.is_simulated_anomaly, bool) or self.schema_version != 1:
            raise ValueError("invalid anomaly label or schema version")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TransactionEvent":
        if not isinstance(payload, dict):
            raise ValueError("event payload must be a JSON object")
        allowed = set(cls.__dataclass_fields__)
        # Extra keys are tolerated for forward compatibility.
        return cls(**{k: v for k, v in payload.items() if k in allowed})

    @classmethod
    def from_json(cls, value: bytes | str) -> "TransactionEvent":
        return cls.from_dict(json.loads(value))

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))
