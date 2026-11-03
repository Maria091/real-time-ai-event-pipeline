"""Kafka producer and worker using confluent-kafka (Redpanda compatible)."""
import logging
from .events import TransactionEvent
from .processor import EventProcessor
from .simulation import generate_events

log = logging.getLogger(__name__)


def ensure_topic(servers: str, topic: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException
    admin = AdminClient({"bootstrap.servers": servers})
    metadata = admin.list_topics(timeout=10)
    if topic not in metadata.topics:
        futures = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        try:
            futures[topic].result(timeout=20)
        except KafkaException as exc:
            if "already exists" not in str(exc).lower():
                raise


def produce(servers: str, topic: str, count: int = 100, seed: int = 42,
            anomaly_fraction: float = 0.12) -> int:
    from confluent_kafka import Producer
    ensure_topic(servers, topic)
    producer = Producer({"bootstrap.servers": servers, "client.id": "ai-event-simulator"})
    errors = []

    def delivered(err, msg):
        if err:
            errors.append(str(err))

    for event in generate_events(count, anomaly_fraction=anomaly_fraction, seed=seed):
        producer.produce(topic, key=event.user_id.encode(), value=event.to_json().encode(),
                         on_delivery=delivered)
        producer.poll(0)
    producer.flush(timeout=30)
    if errors or len(producer) > 0:
        raise RuntimeError(f"Kafka delivery failed: {errors[:3]} (pending={len(producer)})")
    return count


def consume(servers: str, topic: str, group_id: str, processor: EventProcessor,
            max_events: int | None = None) -> int:
    from confluent_kafka import Consumer, KafkaException
    consumer = Consumer({
        "bootstrap.servers": servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    processed = 0
    consumer.subscribe([topic])
    try:
        while max_events is None or processed < max_events:
            message = consumer.poll(timeout=2.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            event = TransactionEvent.from_json(message.value())
            prediction = processor.process(event)
            # Commit only AFTER durable sink write; event_id protects retry duplicates.
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            log.info("event=%s anomaly=%s risk_score=%.1f", event.event_id,
                     prediction.is_anomaly, prediction.risk_score)
    finally:
        consumer.close()
    return processed
