"""Reproducible, fully synthetic sample transactions; no real customer data."""
import random
from datetime import datetime, timedelta, timezone
from uuid import UUID
from .events import TransactionEvent, CATEGORIES, CHANNELS


def generate_events(count: int = 100, anomaly_fraction: float = 0.12, seed: int = 42,
                    start: datetime | None = None) -> list[TransactionEvent]:
    if count < 0 or not (0 <= anomaly_fraction <= 1):
        raise ValueError("count must be >= 0; anomaly_fraction must be between 0 and 1")
    rng = random.Random(seed)
    start = start or datetime.now(timezone.utc) - timedelta(minutes=max(count, 1))
    events = []
    for idx in range(count):
        anomalous = rng.random() < anomaly_fraction
        category = rng.choice(CATEGORIES)
        base = {"grocery": 55, "travel": 170, "electronics": 110, "dining": 42, "fuel": 45, "retail": 75}[category]
        amount = round(max(1.0, rng.lognormvariate(0.0, 0.45) * base), 2)
        country = rng.choices(["US", "CA", "GB"], weights=[94, 4, 2])[0]
        if anomalous:
            amount = round(amount * rng.uniform(12, 35), 2)
            country = rng.choice(["BR", "NG", "SG", "AU"])
        event_time = start + timedelta(seconds=idx * 2)
        event_id = str(UUID(int=rng.getrandbits(128), version=4))
        events.append(TransactionEvent(
            event_id=event_id, event_time=event_time.isoformat(),
            user_id=f"user_{rng.randrange(1, 121):04d}", merchant_category=category,
            amount=amount, country=country, channel=rng.choice(CHANNELS),
            device_id=f"dev_{rng.randrange(1, 81):04d}",
            is_simulated_anomaly=anomalous,
        ))
    return events
