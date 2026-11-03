from event_pipeline.ai import AnomalyDetector
from event_pipeline.processor import EventProcessor
from event_pipeline.simulation import generate_events
from event_pipeline.sinks import SQLiteSink


def test_local_processing_is_idempotent(tmp_path):
    sink = SQLiteSink(str(tmp_path / 'events.sqlite'))
    try:
        processor = EventProcessor(AnomalyDetector.train(sample_count=150), sink)
        events = generate_events(25, seed=11)
        result = processor.process_many(events)
        assert len(result) == 25
        assert sink.summary()['events'] == 25
        processor.process_many(events)
        assert sink.summary()['events'] == 25
        assert sink.summary()['total_amount'] > 0
    finally:
        sink.close()


def test_alert_results_are_ordered(tmp_path):
    sink = SQLiteSink(str(tmp_path / 'alerts.sqlite'))
    try:
        processor = EventProcessor(AnomalyDetector.train(sample_count=200), sink)
        processor.process_many(generate_events(65, anomaly_fraction=0.4, seed=8))
        alerts = sink.alerts()
        scores = [row[4] for row in alerts]
        assert scores == sorted(scores, reverse=True)
    finally:
        sink.close()
