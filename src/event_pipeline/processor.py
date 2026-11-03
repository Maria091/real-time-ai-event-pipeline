"""Shared processing logic used by local Kafka consumer and AWS Lambda."""
from dataclasses import dataclass
from .ai import AnomalyDetector, AnomalyResult
from .events import TransactionEvent


@dataclass
class EventProcessor:
    detector: AnomalyDetector
    sink: object

    def process(self, event: TransactionEvent) -> AnomalyResult:
        prediction = self.detector.predict(event)
        self.sink.upsert(event, prediction)
        return prediction

    def process_many(self, events: list[TransactionEvent]) -> list[AnomalyResult]:
        return [self.process(event) for event in events]
