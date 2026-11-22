# 06 · Viva guide

## 1. Ownership (review / viva accountability)

| Member | Primary area | Files to know line-by-line |
|---|---|---|
| Ritesh T Patil | Scaffold, config, ML + anomaly | `common/*`, `ml/train.py`, `ml/anomaly.py`, `ml/metrics.py` |
| Mohammad Ayan Khan | Ingestion, bronze, data quality, serving | `ingestion/*`, `batch/bronze.py`, `batch/quality.py`, `batch/silver.py`, `serving/*` |
| K Srikar Siddarth | Batch analytics, features, performance | `batch/demand.py`, `batch/features.py`, `batch/analytics.py`, `benchmark/batch_bench.py`, `benchmark/harness.py` |
| Chandana N | Kafka, streaming, reliability | `infra/*`, `streaming/*`, `benchmark/stream_bench.py` |

Everyone should be able to explain the whole data flow in [Data_Pipeline_and_Layers.md](../Data_Pipeline_and_Layers.md).

## 2. Five-minute demo script

1. `make test` — 61 tests, show the reconciliation and stream tests.
2. Show `results/dq/dq_report_2024-01.json` — input = valid + rejected, rejects by reason.
3. Spark UI during `make silver` — the dedup window and zone-hour `groupBy` stages, broadcast join in the SQL tab.
4. `make kafka-up`, `make stream`, `make produce` — watch the Spark UI Structured Streaming tab (input vs processing rate).
5. Dashboard: heatmap → forecast vs actual → anomalies → live stream → scorecard.

## 3. Likely questions

| Question | Answer pointer |
|---|---|
| Why Kafka if the data is historical? | Replayable, partitioned, durable event boundary; lets us measure real streaming latency, backpressure and checkpoint recovery reproducibly. |
| Why is the zone-hour grid densified? | Zero demand is information; row-offset lags equal hour lags only without gaps. |
| How do you avoid leakage? | Time-based split, rolling windows end at *t*, weather observed at *t*, pipeline stages fitted on train only. |
| Why WAPE instead of MAPE? | MAPE explodes on zero/low-demand hours; WAPE weights by volume; MAPE is still reported for y ≥ 10. |
| Exactly-once? | Offsets + state in the checkpoint give exactly-once processing; the parquet sink is at-least-once but the read view (newest `batch_id` per key) is idempotent. |
| Why broadcast the zone table? | 265 rows: shipping it to every task avoids shuffling 3 M+ trips; E3 measures the speedup and shuffle bytes saved. |
| Why does scaling deviate from linear? | Fixed scheduling overhead on small inputs, shuffle growth, skewed zones, spill; see docs/04 §5. |
| What happens to a malformed event? | Fails a rule → DLQ parquet + Kafka DLQ topic with reason and offset; the valid stream is unaffected (separate query). |
| How do you know the stream is correct? | S11: closed hourly windows match the batch `zone_hour_demand` exactly, because both use the same rules. |
| Why UTC session time zone? | TLC times are NY wall-clock without zone; UTC session keeps them unchanged; NOAA UTC is converted once. |
| What would change on a real cluster? | `spark.master`, executor sizing, HDFS/S3 paths in `paths.*`, Kafka replication factor 3; code is unchanged. |
