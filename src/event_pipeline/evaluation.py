"""Synthetic holdout evaluation for the anomaly detector (not a real-world benchmark)."""
from dataclasses import dataclass
from .ai import AnomalyDetector
from .events import TransactionEvent


@dataclass(frozen=True)
class DetectionMetrics:
    samples: int
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    f1: float


def evaluate(detector: AnomalyDetector, events: list[TransactionEvent]) -> DetectionMetrics:
    tp = fp = tn = fn = 0
    for event in events:
        flagged = detector.predict(event).is_anomaly
        actual = event.is_simulated_anomaly
        tp += int(flagged and actual)
        fp += int(flagged and not actual)
        tn += int(not flagged and not actual)
        fn += int(not flagged and actual)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return DetectionMetrics(len(events), tp, fp, tn, fn, round(precision, 3), round(recall, 3), round(f1, 3))
