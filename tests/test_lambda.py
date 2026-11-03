import base64
import pytest
from event_pipeline.lambda_handler import extract_events, handler
from event_pipeline.simulation import generate_events


def make_aws_event(events):
    return {
        'eventSource': 'aws:kafka',
        'records': {
            'transaction-events-0': [
                {'value': base64.b64encode(event.to_json().encode()).decode('ascii'),
                 'topic': 'transaction-events', 'partition': 0, 'offset': idx}
                for idx, event in enumerate(events)
            ]
        }
    }


def test_lambda_decodes_kafka_records():
    events = generate_events(3)
    assert extract_events(make_aws_event(events)) == events


def test_lambda_rejects_bad_payload():
    with pytest.raises(ValueError):
        extract_events({'records': []})


def test_lambda_processes_into_sqlite(tmp_path, monkeypatch):
    from event_pipeline.ai import AnomalyDetector
    model_path = tmp_path / 'detector.joblib'
    AnomalyDetector.train(sample_count=130).save(model_path)
    monkeypatch.setenv('MODEL_PATH', str(model_path))
    monkeypatch.setenv('SINK', 'sqlite')
    monkeypatch.setenv('SQLITE_PATH', str(tmp_path / 'events.sqlite'))
    from event_pipeline.sinks import SQLiteSink
    payload = make_aws_event(generate_events(7, seed=10))
    response = handler(payload, None)
    assert response['processed'] == 7
    assert 0 <= response['ai_alerts'] <= 7
    assert handler(payload, None)['processed'] == 7
    sink = SQLiteSink(str(tmp_path / 'events.sqlite'))
    try:
        assert sink.summary()['events'] == 7
    finally:
        sink.close()
