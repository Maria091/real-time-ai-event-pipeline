"""Explainable Isolation Forest anomaly detector trained on synthetic baseline data.

The risk_score is a ranking heuristic, NOT a calibrated fraud probability.
"""
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
import math
import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from .events import TransactionEvent
from .simulation import generate_events


@dataclass(frozen=True)
class AnomalyResult:
    is_anomaly: bool
    risk_score: float
    reasons: tuple[str, ...]


# Synthetic baseline amount by merchant category. Relative amount improves the
# signal-to-noise ratio for travel purchases vs e.g. dining transactions.
BASELINE_AMOUNT = {
    "grocery": 55, "travel": 170, "electronics": 110,
    "dining": 42, "fuel": 45, "retail": 75,
}


def features(event: TransactionEvent) -> list[float]:
    from datetime import datetime, timezone
    hour = datetime.fromisoformat(event.event_time.replace("Z", "+00:00")).astimezone(timezone.utc).hour
    ratio = event.amount / BASELINE_AMOUNT[event.merchant_category]
    # Compact features avoid drowning out amount signals with many one-hot flags.
    return [
        math.log1p(ratio), math.log1p(event.amount),
        math.sin(2 * math.pi * hour / 24), math.cos(2 * math.pi * hour / 24),
        float(event.country == "US"), float(event.channel == "mobile"),
    ]


class AnomalyDetector:
    def __init__(self, scaler: StandardScaler, forest: IsolationForest):
        self.scaler = scaler
        self.forest = forest

    @classmethod
    def train(cls, sample_count: int = 2000, seed: int = 42) -> "AnomalyDetector":
        if sample_count < 50:
            raise ValueError("training requires at least 50 samples")
        # Train on many times of day, rather than a 1-hour producer burst.
        # The fixed epoch makes training reproducible for a given seed.
        from datetime import datetime, timedelta, timezone
        anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
        baseline = generate_events(sample_count, anomaly_fraction=0, seed=seed, start=anchor)
        baseline = [replace(event, event_time=(anchor + timedelta(
            days=index // 24, hours=(index * 13) % 24)).isoformat())
            for index, event in enumerate(baseline)]
        x = np.asarray([features(e) for e in baseline])
        scaler = StandardScaler().fit(x)
        forest = IsolationForest(n_estimators=120, contamination=0.02,
                                 random_state=seed, n_jobs=1).fit(scaler.transform(x))
        return cls(scaler, forest)

    def predict(self, event: TransactionEvent) -> AnomalyResult:
        x = self.scaler.transform(np.asarray([features(event)]))
        decision = float(self.forest.decision_function(x)[0])
        flagged = bool(self.forest.predict(x)[0] == -1)
        # Monotonic display metric: higher indicates more unusual; not a probability.
        score = round(float(np.clip(50.0 - decision * 250.0, 0.0, 100.0)), 1)
        reasons = []
        if event.amount >= 500:
            reasons.append("unusually_high_amount")
        if event.country not in ("US", "CA", "GB"):
            reasons.append("uncommon_country")
        if not reasons and flagged:
            reasons.append("unusual_feature_combination")
        return AnomalyResult(flagged, score, tuple(reasons))

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"version": 3, "scaler": self.scaler, "forest": self.forest}, path)

    @classmethod
    def load(cls, path: str | Path) -> "AnomalyDetector":
        payload = joblib.load(path)  # only load models created by this project/trusted sources
        if payload.get("version") != 3:
            raise ValueError("unsupported model format")
        return cls(payload["scaler"], payload["forest"])


@lru_cache(maxsize=4)
def load_detector(path: str) -> AnomalyDetector:
    return AnomalyDetector.load(path)
