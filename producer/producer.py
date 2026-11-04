"""Replay one TLC dataset into Kafka as a paced, optionally faulty stream.

    DATASET=green python producer.py
    python producer.py --dataset yellow --dry-run --out yellow.ndjson   # no Kafka needed

Configuration comes from stream-config.yaml, producer/datasets.yaml and env vars
(see config.py). Exit code is 0 only if every message was delivered.
"""
import argparse
import json
import random
import sys
import time
from collections import Counter
from pathlib import Path

from config import load_settings
from faults import Injector
from pacing import Pacer
from replay import iter_records
from serialize import to_message


class KafkaSink:
    def __init__(self, settings):
        from confluent_kafka import Producer

        d = settings.delivery
        self.topic = settings.topic
        self.errors = 0
        self.delivered = 0
        self._producer = Producer({
            "bootstrap.servers": settings.bootstrap,
            "acks": d["acks"],
            "enable.idempotence": d["enable_idempotence"],
            "compression.type": d["compression"],
            "linger.ms": d["linger_ms"],
            "batch.size": d["batch_size_bytes"],
            "retries": d["retries"],
        })

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.errors += 1
            print(f"delivery failed: {err}", file=sys.stderr)
        else:
            self.delivered += 1

    def send(self, key, value, headers):
        while True:
            try:
                self._producer.produce(self.topic, key=key, value=value,
                                       headers=headers or None, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)
        self._producer.poll(0)

    def close(self):
        remaining = self._producer.flush(60)
        if remaining:
            self.errors += remaining
            print(f"{remaining} messages not delivered before timeout", file=sys.stderr)


class NdjsonSink:
    """Dry-run sink: writes one JSON line per message (or just counts when path is None)."""

    def __init__(self, path=None):
        self.errors = 0
        self.delivered = 0
        self._fh = open(path, "w", encoding="utf-8") if path else None

    def send(self, key, value, headers):
        self.delivered += 1
        if self._fh:
            self._fh.write(json.dumps({
                "key": key.decode() if key else None,
                "headers": {k: v.decode() for k, v in (headers or [])},
                "value": value.decode("utf-8", errors="replace"),
            }) + "\n")

    def close(self):
        if self._fh:
            self._fh.close()


def wait_for_kafka(settings):
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.bootstrap})
    deadline = time.time() + settings.connect_retry_s
    while True:
        try:
            if settings.topic in admin.list_topics(timeout=5).topics:
                return
            reason = f"topic {settings.topic} does not exist yet"
        except Exception as exc:  # broker not up yet
            reason = str(exc)
        if time.time() > deadline:
            sys.exit(f"Kafka not ready at {settings.bootstrap}: {reason}")
        print(f"waiting for Kafka: {reason}")
        time.sleep(3)


def input_files(settings):
    files = [settings.data_dir / f"{settings.spec['file_prefix']}_{m}.parquet" for m in settings.months]
    missing = [str(f) for f in files if not f.exists()]
    if missing:
        sys.exit("input file(s) missing: " + ", ".join(missing)
                 + "\nrun: python scripts/download_data.py")
    return files


def run(settings, sink):
    rng = random.Random(settings.seed)
    injector = Injector(settings.spec, settings.rates, settings.quality, rng)
    pacer = Pacer(settings.speedup, settings.start_ts, settings.max_eps)
    malformed_mode = settings.quality["malformed_json"]["mode"]

    source = iter_records(input_files(settings), settings.spec, settings.max_records,
                          settings.sample_fraction, settings.seed, settings.read_batch_rows)
    base_count = {"n": 0}

    def counted(it):
        for rec in it:
            base_count["n"] += 1
            yield rec

    faults, sent, started = Counter(), 0, time.time()
    for rec in injector.stream(counted(source)):
        pacer.wait(rec.ts)
        key, value = to_message(rec, malformed_mode, rng)
        headers = [("fault", ",".join(rec.faults).encode())] if rec.faults else None
        sink.send(key, value, headers)
        sent += 1
        for f in rec.faults:
            faults[f] += 1
        if settings.progress_every and sent % settings.progress_every == 0:
            rate = sent / max(time.time() - started, 1e-9)
            print(f"{settings.dataset}: sent {sent:,} ({rate:,.0f}/s), event time {rec.ts}")

    sink.close()
    elapsed = time.time() - started
    stats = {
        "dataset": settings.dataset,
        "topic": settings.topic,
        "profile": settings.profile,
        "rates": settings.rates,
        "source_records": base_count["n"],
        "messages_sent": sent,
        "delivered": sink.delivered,
        "delivery_errors": sink.errors,
        "faults": dict(sorted(faults.items())),
        "elapsed_seconds": round(elapsed, 2),
        "messages_per_second": round(sent / elapsed, 1) if elapsed else None,
    }
    print(json.dumps(stats))
    if settings.stats_dir:
        settings.stats_dir.mkdir(parents=True, exist_ok=True)
        (settings.stats_dir / f"{settings.dataset}.json").write_text(json.dumps(stats, indent=2))
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", help="yellow | green | fhv | hvfhv (or env DATASET)")
    ap.add_argument("--dry-run", action="store_true", help="do not use Kafka; write NDJSON to --out")
    ap.add_argument("--out", help="NDJSON file for --dry-run (omit to just count)")
    args = ap.parse_args(argv)

    settings = load_settings(args.dataset)
    if args.dry_run:
        sink = NdjsonSink(args.out)
    else:
        wait_for_kafka(settings)
        sink = KafkaSink(settings)
    stats = run(settings, sink)
    return 1 if stats["delivery_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
