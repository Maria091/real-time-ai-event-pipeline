from event_pipeline.ai import AnomalyDetector
from event_pipeline.evaluation import evaluate
from event_pipeline.simulation import generate_events


def test_synthetic_evaluation_counts_and_metrics():
    metrics = evaluate(AnomalyDetector.train(sample_count=200),
                       generate_events(60, anomaly_fraction=0.25, seed=70))
    assert metrics.samples == 60
    assert metrics.true_positive + metrics.false_positive + metrics.true_negative + metrics.false_negative == 60
    assert 0 <= metrics.precision <= 1
    assert 0 <= metrics.recall <= 1
    assert 0 <= metrics.f1 <= 1
