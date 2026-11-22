"""Kafka replay producer: turns BRONZE trips into a rate-controlled JSON event stream with injected defects."""
from __future__ import annotations

import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from ..common.config import Config, load_config
from ..common.storage import arrow_location
from ..common.utils import get_logger

log = get_logger(__name__)

EVENT_FIELDS = ["record_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "trip_distance",
                "total_amount", "passenger_count", "source_month"]
MALFORMED_KINDS = ["invalid_json", "missing_pickup_ts", "bad_zone", "negative_distance", "non_numeric"]
MALFORMED_KEY_PREFIX = "__malformed__:"


def _ts(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _num(value):
    return None if value is None or pd.isna(value) else value


def to_event(row: dict, produced_at_ms: int) -> dict:
    return {
        "event_id": row["record_id"],
        "pickup_ts": _ts(row["pickup_ts"]),
        "dropoff_ts": _ts(row["dropoff_ts"]),
        "pickup_zone_id": None if pd.isna(row["pickup_zone_id"]) else int(row["pickup_zone_id"]),
        "dropoff_zone_id": None if pd.isna(row["dropoff_zone_id"]) else int(row["dropoff_zone_id"]),
        "trip_distance": _num(row["trip_distance"]),
        "total_amount": _num(row["total_amount"]),
        "passenger_count": _num(row["passenger_count"]),
        "source_month": row["source_month"],
        "produced_at_ms": produced_at_ms,
    }


def make_malformed(event: dict, kind: str) -> bytes:
    bad = dict(event)
    if kind == "invalid_json":
        return (json.dumps(bad)[:-7] + "#!garbage").encode()
    if kind == "missing_pickup_ts":
        bad.pop("pickup_ts")
    elif kind == "bad_zone":
        bad["pickup_zone_id"] = 999
    elif kind == "negative_distance":
        bad["trip_distance"] = -abs(bad.get("trip_distance") or 1.0)
    elif kind == "non_numeric":
        bad["total_amount"] = "twelve dollars"
    else:
        raise ValueError(kind)
    bad["event_id"] = f"malformed-{kind}-{bad['event_id']}"
    return json.dumps(bad).encode()


def load_events(cfg: Config, month: str, max_events: int | None) -> pd.DataFrame:
    fs, location = arrow_location(cfg.path("bronze") / "trips")
    dataset = ds.dataset(location, filesystem=fs, format="parquet", partitioning="hive")
    table = dataset.to_table(filter=ds.field("source_month") == month,
                             columns=[c for c in EVENT_FIELDS if c != "source_month"])
    df = table.to_pandas()
    df["source_month"] = month
    df = df.sort_values("pickup_ts", kind="stable", na_position="first").reset_index(drop=True)
    return df.head(max_events) if max_events else df


def ensure_topic(bootstrap: str, topic: str, partitions: int, replication: int = 1, recreate: bool = False) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing and recreate:
        admin.delete_topics([topic])[topic].result()
        for _ in range(30):
            if topic not in admin.list_topics(timeout=10).topics:
                break
            time.sleep(1)
        existing = {}
    if topic not in existing:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])[topic].result()
        log.info("created topic %s with %d partitions", topic, partitions)


def replay(cfg: Config, rate: float | None = None, max_events: int | None = None, month: str | None = None,
           duration_s: float | None = None, topic: str | None = None, malformed_fraction: float | None = None,
           tag: str | None = None) -> dict:
    from confluent_kafka import Producer

    pcfg, kcfg = cfg.section("producer"), cfg.section("kafka")
    rate = rate if rate is not None else pcfg["events_per_second"]
    max_events = max_events if max_events is not None else pcfg["max_events"]
    month = month or pcfg["replay_month"]
    topic = topic or kcfg["topic"]
    malformed_fraction = pcfg["malformed_fraction"] if malformed_fraction is None else malformed_fraction
    speedup = float(pcfg.get("speedup") or 0)
    ensure_topic(kcfg["bootstrap_servers"], topic, kcfg["partitions"], kcfg["replication_factor"])
    ensure_topic(kcfg["bootstrap_servers"], kcfg["dlq_topic"], 1, kcfg["replication_factor"])

    events = load_events(cfg, month, max_events)
    log.info("replaying %d events from %s to %s at %s", len(events),
             month, topic, f"{speedup}x real time" if speedup else f"{rate:.0f} ev/s")
    producer = Producer({"bootstrap.servers": kcfg["bootstrap_servers"], "linger.ms": 5, "acks": "all",
                         "compression.type": "lz4", "queue.buffering.max.messages": 2_000_000,
                         "batch.num.messages": 10_000})
    rng = random.Random(cfg.section("anomaly")["injection_seed"])
    sent = malformed = duplicates = 0
    seen_ids: set[str] = set()
    t0 = time.time()
    first_event_ts = pd.Timestamp(events["pickup_ts"].iloc[0]) if speedup and len(events) else None
    for row in events.to_dict("records"):
        if duration_s and time.time() - t0 >= duration_s:
            break
        if row["record_id"] in seen_ids:
            duplicates += 1
        seen_ids.add(row["record_id"])
        if speedup and first_event_ts is not None and not pd.isna(row["pickup_ts"]):
            due = (pd.Timestamp(row["pickup_ts"]) - first_event_ts).total_seconds() / speedup
        else:
            due = sent / rate
        wait = t0 + due - time.time()
        if wait > 0.002:
            time.sleep(wait)
        event = to_event(row, int(time.time() * 1000))
        messages = [(str(event["pickup_zone_id"]), json.dumps(event).encode())]
        # malformed copies are extra messages so every real trip still reaches the stream (needed for S11)
        if rng.random() < malformed_fraction:
            kind = MALFORMED_KINDS[malformed % len(MALFORMED_KINDS)]
            messages.append((f"{MALFORMED_KEY_PREFIX}{kind}", make_malformed(event, kind)))
            malformed += 1
        for key, value in messages:
            while True:
                try:
                    producer.produce(topic, key=key.encode(), value=value)
                    break
                except BufferError:
                    producer.poll(0.05)
            sent += 1
        if sent % 10_000 < len(messages):
            producer.poll(0)
    producer.flush(60)
    elapsed = time.time() - t0
    summary = {"tag": tag or cfg.section("streaming").get("run_tag", "default"), "topic": topic, "month": month,
               "target_eps": rate, "events_sent": sent, "malformed_sent": malformed, "valid_sent": sent - malformed,
               "duplicates_sent": duplicates,
               "elapsed_s": round(elapsed, 2), "actual_eps": round(sent / max(elapsed, 1e-9), 1),
               "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    _append_csv(cfg.path("results") / "stream" / "producer_runs.csv", summary)
    log.info("producer done: %s", summary)
    return summary


def _append_csv(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay bronze taxi trips into Kafka")
    ap.add_argument("--rate", type=float, help="events per second (default from config)")
    ap.add_argument("--max-events", type=int)
    ap.add_argument("--month")
    ap.add_argument("--duration", type=float, help="stop after N seconds")
    ap.add_argument("--topic")
    ap.add_argument("--malformed-fraction", type=float)
    ap.add_argument("--tag")
    a = ap.parse_args()
    replay(load_config(), a.rate, a.max_events, a.month, a.duration, a.topic, a.malformed_fraction, a.tag)


if __name__ == "__main__":
    main()
