from dataclasses import replace
from event_pipeline.ai import AnomalyDetector, features
from event_pipeline.simulation import generate_events


def test_model_round_trip(tmp_path):
    detector = AnomalyDetector.train(sample_count=200, seed=7)
    event = generate_events(1, anomaly_fraction=0, seed=5)[0]
    first = detector.predict(event)
    filename = tmp_path / 'model.joblib'
    detector.save(filename)
    second = AnomalyDetector.load(filename).predict(event)
    assert first == second
    assert 0 <= first.risk_score <= 100
    assert isinstance(first.is_anomaly, bool)
    assert len(features(event)) == 6


def test_extreme_amount_returns_explanation():
    detector = AnomalyDetector.train(sample_count=500, seed=3)
    event = generate_events(1, anomaly_fraction=0)[0]
    extreme = replace(event, amount=50000.0, country='NG')
    outcome = detector.predict(extreme)
    assert 'unusually_high_amount' in outcome.reasons
    assert 'uncommon_country' in outcome.reasons
    assert outcome.is_anomaly
