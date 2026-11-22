import pandas as pd
import pytest

from mobilitypulse.benchmark import stream_bench
from mobilitypulse.common.config import load_config
from mobilitypulse.ingestion import download


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.payload


def test_ingest_is_idempotent_and_writes_manifest(tmp_path, monkeypatch):
    calls = []

    def fake_get(url, stream, timeout):
        calls.append(url)
        prefix = b"PAR1" if url.endswith(".parquet") else b""
        return FakeResponse(prefix + f"content of {url}".encode())

    monkeypatch.setattr(download.requests, "get", fake_get)
    cfg = load_config(overrides={"paths": {"data_dir": str(tmp_path)}})
    first = download.run_ingest(cfg, ["2024-01", "2024-02"])
    assert [r["status"] for r in first] == ["downloaded"] * 4          # zones, 2 TLC months, 1 NOAA year
    second = download.run_ingest(cfg, ["2024-01", "2024-02"])
    assert [r["status"] for r in second] == ["skipped"] * 4
    assert len(calls) == 4
    manifest = pd.read_parquet(tmp_path / "raw" / "_manifest.parquet")
    assert set(manifest["source"]) == {"taxi_zones", "tlc_yellow", "noaa_isd"} and len(manifest) == 4
    (tmp_path / "raw" / "tlc" / "yellow_tripdata_2024-01.parquet").write_bytes(b"PAR1tampered")
    third = download.run_ingest(cfg, ["2024-01"])
    assert [r["status"] for r in third][1] == "downloaded"


def test_empty_download_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(download.requests, "get", lambda *a, **k: FakeResponse(b""))
    with pytest.raises(IOError):
        download.download("http://example.invalid/x", tmp_path / "x.bin")
    assert not (tmp_path / "x.bin").exists()


@pytest.mark.parametrize("name,payload", [("w.csv", b"<!DOCTYPE html><html>moved</html>"),
                                          ("t.parquet", b"<html>error</html>")])
def test_html_error_pages_are_rejected(tmp_path, monkeypatch, name, payload):
    monkeypatch.setattr(download.requests, "get", lambda *a, **k: FakeResponse(payload))
    with pytest.raises(IOError):
        download.download("http://example.invalid/x", tmp_path / name)
    assert not list(tmp_path.iterdir())


def test_stream_summary_from_logs(tmp_path):
    cfg = load_config(overrides={"paths": {"results": str(tmp_path)}, "benchmark": {"stream_warmup_s": 10}})
    d = tmp_path / "stream"
    d.mkdir()
    t0 = 1_700_000_000_000
    batches = pd.DataFrame({"tag": "x", "batch_id": range(10), "commit_ms": [t0 + i * 5000 for i in range(10)],
                            "rows_updated": 4})
    samples = pd.DataFrame({"tag": "x", "batch_id": [i for i in range(10) for _ in range(4)],
                            "latency_ms": [1000.0, 2000.0, 3000.0, 4000.0] * 10})
    progress = pd.DataFrame({
        "tag": "x", "batch_id": range(10),
        "timestamp": [pd.Timestamp(t0 + i * 5000, unit="ms", tz="UTC").isoformat() for i in range(10)],
        "num_input_rows": 10_000, "input_rows_per_s": 2000.0, "processed_rows_per_s": 4000.0,
        "trigger_ms": [3000] * 9 + [7000], "add_batch_ms": 2000, "lag_events": [100] * 10})
    batches.to_csv(d / "batches.csv", index=False)
    samples.to_csv(d / "latency_samples.csv", index=False)
    progress.to_csv(d / "progress.csv", index=False)

    s = stream_bench.summarize(cfg, "x")
    assert s["latency_p50_s"] == pytest.approx(2.5)
    assert s["batches"] == 8                              # first 2 batches are warm-up
    assert s["batches_within_trigger_pct"] == pytest.approx(87.5)
    assert s["lag_p95_events"] == 100 and s["lag_growth_events"] == 0
    assert stream_bench.summarize(cfg, "missing") == {}
