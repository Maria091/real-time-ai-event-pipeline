"""CLI: zero-cloud demo, model training, Kafka produce/consume, reporting."""
import argparse
import csv
import logging
import os
from pathlib import Path
from .ai import AnomalyDetector, load_detector
from .config import env, kafka_servers, kafka_topic, model_path
from .processor import EventProcessor
from .simulation import generate_events
from .sinks import SQLiteSink, make_sink


def ensure_model(path: Path) -> AnomalyDetector:
    if not path.is_file():
        print(f"Model missing; training synthetic baseline -> {path}")
        AnomalyDetector.train().save(path)
    return load_detector(str(path))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="event-pipeline", description="Real-time AI event pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    train = sub.add_parser("train-model", help="train an Isolation Forest on synthetic normal events")
    train.add_argument("--path", default=str(model_path()))
    train.add_argument("--samples", type=int, default=2000)
    demo = sub.add_parser("demo", help="run entirely offline using SQLite")
    demo.add_argument("--count", type=int, default=150)
    demo.add_argument("--database", default=env("SQLITE_PATH", "data/events.sqlite"))
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--export", default="data/alerts.csv")
    prod = sub.add_parser("produce", help="publish transactions to Kafka")
    prod.add_argument("--count", type=int, default=100)
    prod.add_argument("--seed", type=int, default=42)
    prod.add_argument("--anomaly-fraction", type=float, default=0.12)
    consume = sub.add_parser("consume", help="consume Kafka events and persist results")
    consume.add_argument("--sink", choices=["sqlite", "snowflake"], default=env("SINK", "sqlite"))
    consume.add_argument("--max-events", type=int)
    report = sub.add_parser("report", help="print local SQLite metrics and AI alerts")
    report.add_argument("--database", default=env("SQLITE_PATH", "data/events.sqlite"))
    narrative = sub.add_parser("ai-summary", help="summarize synthetic alerts; optional LLM narrative")
    narrative.add_argument("--database", default=env("SQLITE_PATH", "data/events.sqlite"))
    narrative.add_argument("--llm", action="store_true", help="send anonymized synthetic metrics to a paid OpenAI API")
    narrative.add_argument("--model", default=env("OPENAI_MODEL", "gpt-4.1-mini"))
    evaluation = sub.add_parser("evaluate", help="measure ML precision/recall on synthetic holdout labels")
    evaluation.add_argument("--count", type=int, default=1000)
    evaluation.add_argument("--seed", type=int, default=73)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "train-model":
        model = AnomalyDetector.train(sample_count=args.samples)
        model.save(args.path)
        print(f"Trained Isolation Forest on {args.samples} synthetic baseline events -> {args.path}")
    elif args.command == "demo":
        model = ensure_model(model_path())
        sink = SQLiteSink(args.database)
        try:
            results = EventProcessor(model, sink).process_many(generate_events(args.count, seed=args.seed))
            print("Offline demo complete:", sink.summary())
            print(f"Generated {args.count} events; {sum(r.is_anomaly for r in results)} classified as anomalies in this run")
            print("Top AI alerts (timestamp, event ID, amount, country, risk score, reasons):")
            for row in sink.alerts(5):
                print(" ", row)
            if args.export:
                path = Path(args.export)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(["event_time", "event_id", "amount", "country", "ai_score", "ai_reasons"])
                    writer.writerows(sink.alerts(100000))
                print("Alerts CSV:", path)
        finally:
            sink.close()
    elif args.command == "produce":
        from .kafka_io import produce
        count = produce(kafka_servers(), kafka_topic(), args.count, args.seed, args.anomaly_fraction)
        print(f"Published {count} synthetic events to Kafka topic {kafka_topic()}")
    elif args.command == "consume":
        from .kafka_io import consume
        sink = make_sink(args.sink, env("SQLITE_PATH", "data/events.sqlite"))
        try:
            processor = EventProcessor(ensure_model(model_path()), sink)
            count = consume(kafka_servers(), kafka_topic(),
                            env("KAFKA_GROUP_ID", "ai-event-pipeline-v1"),
                            processor, args.max_events)
            print(f"Processed {count} events")
        except KeyboardInterrupt:
            print("Stopped consumer")
        finally:
            sink.close()
    elif args.command == "report":
        sink = SQLiteSink(args.database)
        try:
            print("Metrics:", sink.summary())
            for row in sink.alerts(10):
                print(row)
        finally:
            sink.close()
    elif args.command == "evaluate":
        from .evaluation import evaluate
        model = ensure_model(model_path())
        metrics = evaluate(model, generate_events(args.count, anomaly_fraction=0.12, seed=args.seed))
        print("Synthetic-only detector evaluation (NOT production fraud performance):")
        print(metrics)
    elif args.command == "ai-summary":
        from .genai import build_alert_snapshot, narrative_without_llm, narrative_with_llm
        sink = SQLiteSink(args.database)
        try:
            snapshot = build_alert_snapshot(sink.summary(), sink.alerts(10))
        finally:
            sink.close()
        if args.llm:
            print(narrative_with_llm(snapshot, model=args.model))
        else:
            print(narrative_without_llm(snapshot))
            print("To use a real generative AI summary, set OPENAI_API_KEY and rerun with --llm (billable API call).")


if __name__ == "__main__":
    main()
