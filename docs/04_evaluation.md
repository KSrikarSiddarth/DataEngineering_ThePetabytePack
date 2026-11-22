# 04 · Evaluation

All targets are fixed in [configs/targets.yaml](../configs/targets.yaml) (copied from Plan §6). Each pipeline stage
and benchmark writes its numbers to `results/measurements.json`; `make scorecard` compares them with the targets
and writes `results/evaluation_scorecard.md` with **PASS / FAIL / NOT RUN** per target.

## 1. Where every number comes from

| Targets | Produced by | Command |
|---|---|---|
| D1, D2, D3 | `batch/silver.py` — re-reads written silver + quarantine and reconciles against bronze | `make silver` / `make pipeline` |
| D3b | `batch/demand.py` — Σ zone-hour `trip_count` vs silver trip count | `make demand` |
| O3 | `cli.py` features stage timing, or E1 medium | `make features` / `make bench-batch` |
| M1–M9, O1, O2 | `ml/train.py` | `make ml` |
| A1–A5 | `ml/anomaly.py` | `make anomaly` |
| B1–B3 (+O3) | E1 data scaling (small/medium/large) | `make bench-batch` |
| B6 | E2 shuffle partitions {8…256} with AQE coalescing off | `make bench-batch` |
| B4 | E3 broadcast vs sort-merge (`autoBroadcastJoinThreshold=-1`) | `make bench-batch` |
| B5 | E4 five aggregations over a reused DataFrame, cached vs not | `make bench-batch` |
| B7 | E5 same aggregation on Parquet vs CSV copy of one month | `make bench-batch` |
| S1–S4, S6, S10, S10b, S11 | stream main trial (2k ev/s, 4 partitions, 10 min) | `make bench-stream` |
| S5 | rate sweep 1k–20k ev/s | `python -m mobilitypulse.benchmark.stream_bench rates` |
| S7 | partition sweep 1/2/4/8 @ 10k ev/s | `... stream_bench partitions` |
| S8, S9 | kill -9 + restart from checkpoint | `... stream_bench recovery` |
| D4 | `make demo` total time, or manual fresh-clone timing via `scripts/record_measurement.py` | `make demo` |
| D5 | pytest junit + coverage json | `make test` |

## 2. Method rules

* Each batch experiment runs `benchmark.repetitions` (3) times; the **median** is used; all repetitions are kept
  in `results/benchmarks/<experiment>.csv` together with Spark stage metrics (tasks, shuffle read/write bytes,
  spill, input bytes) read from the Spark UI REST API per job group.
* Cache is cleared before every repetition.
* Scaling efficiency for growth factor $k = \text{rows}_B / \text{rows}_A$:
  $\eta = \dfrac{T_A \cdot k}{T_B}$ (1.0 = perfectly linear; > 1 means fixed overheads are amortised).
* Streaming numbers exclude a 60 s warm-up (JIT, Kafka connection, first state-store creation).
* Accuracy is always reported on the **test** split of the best-on-validation model.
* Machine details are printed in the scorecard header; record CPU/RAM in the report.

## 3. Dataset sizes (approved: laptop-friendly)

| Name | Months | Approx. trips |
|---|---|---|
| small | 2024-01 | ~3.0 M |
| medium | 2024-01..03 | ~9.5 M |
| large | 2024-01..06 | ~20 M |

## 4. Reporting a FAIL

For every FAIL the report states: measured value, root cause with evidence (Spark UI stage view, shuffle/spill
numbers, progress CSV), what was tried (e.g. partition count, broadcast hint, trigger interval, model params),
and the residual gap. Targets are never edited after results are seen.

## 5. Interpreting the scaling analysis

Deviation from linear scaling is explained using: fixed job/stage scheduling overhead (dominant on *small*),
shuffle volume (zone-hour aggregation, dedup window), skew (Midtown/JFK/LaGuardia zones), small files, executor
memory and spill, and the single-machine ceiling (all executors share 10 cores) — cf. Amdahl's law for the serial
fraction (driver-side planning, `collect` of DQ counts) versus Gustafson-style scaled workloads.
