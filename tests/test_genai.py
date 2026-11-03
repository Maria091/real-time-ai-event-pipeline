from types import SimpleNamespace
from event_pipeline.genai import build_alert_snapshot, narrative_without_llm, narrative_with_llm


def test_summary_sanitizes_ids_and_explains_caveat():
    snapshot = build_alert_snapshot(
        {"events": 100, "ai_alerts": 5, "total_amount": 401.23},
        [("2026-10-25", "sensitive-id", 300.0, "US", 85.0, '["unusually_high_amount"]')],
    )
    assert "sensitive-id" not in str(snapshot)
    assert snapshot["top_alerts"][0]["reason_codes"] == ["unusually_high_amount"]
    local = narrative_without_llm(snapshot)
    assert "5.0%" in local
    assert "not evidence of fraud" in local


def test_llm_client_can_be_mocked_without_network():
    class FakeResponses:
        def create(self, **kwargs):
            assert kwargs["max_output_tokens"] == 250
            assert "not confirmed fraud" in kwargs["instructions"].lower()
            return SimpleNamespace(output_text="Mocked summary; no API was called.")
    fake = SimpleNamespace(responses=FakeResponses())
    text = narrative_with_llm({"total_events": 0, "ml_flagged_events": 0}, client=fake)
    assert text.startswith("Mocked")
