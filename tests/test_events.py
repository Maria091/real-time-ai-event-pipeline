import pytest
from event_pipeline.events import TransactionEvent
from event_pipeline.simulation import generate_events


def test_synthetic_events_valid_and_repeatable():
    a = generate_events(12, seed=19)
    b = generate_events(12, seed=19, start=__import__('datetime').datetime.fromisoformat(a[0].event_time))
    assert len(a) == len(b) == 12
    assert [x.event_id for x in a] == [x.event_id for x in b]
    assert all(x.amount > 0 for x in a)
    assert all(TransactionEvent.from_json(x.to_json()) == x for x in a)


def test_invalid_amount_rejected():
    payload = vars(generate_events(1)[0]).copy()
    payload['amount'] = -2
    with pytest.raises(ValueError, match='amount'):
        TransactionEvent.from_dict(payload)


def test_invalid_timestamp_rejected():
    payload = vars(generate_events(1)[0]).copy()
    payload['event_time'] = '2026-01-01T10:00:00'
    with pytest.raises(ValueError):
        TransactionEvent.from_dict(payload)


def test_invalid_generator_parameters():
    with pytest.raises(ValueError):
        generate_events(-1)
    with pytest.raises(ValueError):
        generate_events(20, anomaly_fraction=1.2)
