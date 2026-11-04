"""Create the Kafka topics declared in stream-config.yaml, then exit.

Idempotent: topics that already exist are left alone (a warning is printed if
their partition count differs from the config). Exits non-zero if any topic is
missing at the end, so `docker compose` surfaces the failure.
"""
import os
import sys
import time

import yaml
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/config/stream-config.yaml")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
CONNECT_TIMEOUT_S = int(os.environ.get("CONNECT_TIMEOUT_S", "60"))


def load_topics(path):
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    defaults = cfg.get("topic_defaults", {})
    topics = []
    for entry in cfg.get("topics", []):
        merged = {**defaults, **{k: v for k, v in entry.items() if v is not None}}
        topics.append(merged)
    if not topics:
        sys.exit(f"No topics found in {path}")
    return topics


def wait_for_broker(admin):
    deadline = time.time() + CONNECT_TIMEOUT_S
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException as exc:
            if time.time() > deadline:
                sys.exit(f"Kafka not reachable at {BOOTSTRAP}: {exc}")
            print(f"Waiting for Kafka at {BOOTSTRAP} ...")
            time.sleep(3)


def to_new_topic(spec):
    config = {
        "retention.ms": str(int(spec.get("retention_hours", 24)) * 3600 * 1000),
        "cleanup.policy": spec.get("cleanup_policy", "delete"),
        "compression.type": spec.get("compression_type", "producer"),
    }
    return NewTopic(
        spec["name"],
        num_partitions=int(spec.get("partitions", 3)),
        replication_factor=int(spec.get("replication_factor", 1)),
        config=config,
    )


def main():
    topics = load_topics(CONFIG_PATH)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    wait_for_broker(admin)

    futures = admin.create_topics([to_new_topic(t) for t in topics])
    for name, future in futures.items():
        try:
            future.result()
            print(f"created  {name}")
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"exists   {name}")
            else:
                sys.exit(f"failed   {name}: {exc}")

    # Verify: every topic exists with the expected partition count.
    metadata = admin.list_topics(timeout=10).topics
    missing = []
    for spec in topics:
        meta = metadata.get(spec["name"])
        if meta is None or meta.error is not None:
            missing.append(spec["name"])
            continue
        actual = len(meta.partitions)
        wanted = int(spec.get("partitions", 3))
        if actual != wanted:
            print(f"warning  {spec['name']}: has {actual} partitions, config says {wanted}")
    if missing:
        sys.exit(f"Missing topics after creation: {missing}")
    print(f"ok       {len(topics)} topics ready")


if __name__ == "__main__":
    main()
