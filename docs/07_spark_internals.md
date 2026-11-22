# 07 · Spark internals evidence pack

Four small experiments that show **why** Spark behaves the way it does, not only that the pipeline runs.
Code: [src/mobilitypulse/benchmark/concepts.py](../src/mobilitypulse/benchmark/concepts.py). Tests:
[tests/integration/test_concepts.py](../tests/integration/test_concepts.py).

```bash
make concepts                    # all four
make concepts EXPERIMENTS="c1 c3"
MOBILITYPULSE_DATA_DIR=~/mobilitypulse_sample make concepts   # on the synthetic sample
```

Output goes to `results/concepts/` (CSV tables and saved physical plans). The Spark UI (http://localhost:4040,
Jobs and Stages tabs) shows the same events live while the command runs.

> **Where the numbers below come from.** They were produced on the **synthetic sample data** (449,385 trips, 40
> zones, 2 months) on a 16-core Linux machine, single run each unless stated. The *concepts* hold on any data; the
> *absolute numbers* are not results about real NYC taxi data. Re-run on the real data directory before quoting
> them in the report. The queries are small demonstration queries over silver, not steps of the pipeline.

## C1 · Lazy evaluation and the DAG

Transformations (`filter`, `withColumn`, `join`, `groupBy`, `orderBy`) only build a plan. Spark runs jobs when an
**action** (`count`, `collect`, `write`) is called. The experiment counts Spark jobs per step with the status tracker.

| Step | Kind | Jobs run |
|---|---|---|
| filter, withColumn, join, groupBy, orderBy | transformation | **0** each |
| `explain()` | plan only | **0** |
| `count()` | action | **4** |
| `collect()` | action | 5 |
| `count()` again, nothing cached | action | 4 (recomputed) |

Evidence files: `c1_lazy.csv`, `c1_lazy_plan.txt` (the DAG). The plan shows `PushedFilters` and a `ReadSchema` of
4 columns on the Parquet scan, i.e. the optimiser pruned work across the whole chain, and two `Exchange` nodes (the
shuffles that C2 examines).

## C2 · Narrow vs wide transformations

A narrow step stays inside one partition. A wide step needs a shuffle, which adds a stage boundary. Each query is
written with a `noop` sink (not `count()`, which adds a shuffle of its own) and Adaptive Query Execution is switched
off during the experiment so that stages and tasks have their classic shape. Stage metrics come from the Spark UI
REST API.

| Query | Type | Shuffles | Stages | Shuffle write |
|---|---|---|---|---|
| filter + new column + select | narrow | 0 | **1** | 0 MB |
| zone-hour aggregation (the real one in `batch/demand.py`) | wide | 1 | **2** | 2.2 MB |
| aggregation + global sort | wide | 2 | **4** | 1.4 MB |
| `repartition` by zone | wide | 1 | **2** | 69 MB |

Stages follow shuffles. The aggregation reads 16 MB but shuffles only 2.2 MB because each partition is pre-aggregated
first (the partial `HashAggregate` in the plan); `repartition` has no such shortcut. First-stage tasks (15) are the
Parquet file splits; second-stage tasks (32) are the shuffle partitions.

Files: `c2_narrow_wide.csv`, `c2_narrow_wide_stages.csv` (per stage), `c2_*_plan.txt`. Timings in this table are single
runs and are not the evidence; stage and shuffle counts are.

## C3 · Data skew

One hot key makes one task do most of the work, and the stage waits for it.

The sample data is only mildly skewed (busiest zone 2.6× the median), so a hot zone is **injected on purpose**: 50 % of
trips are deterministically remapped to zone 161, making it 41× the median. This injection is synthetic and must be
described as such. The zone table is small enough to be broadcast, which would hide skew, so broadcasting is disabled
to force a shuffle join. AQE skew thresholds are lowered to 512 KB (defaults 256 MB) so they trigger on small data.

| Variant | Read skew (largest task / median task) | Slowest task | Tasks |
|---|---|---|---|
| natural data, no handling | 6.5× | 352 ms | 32 |
| **skewed, no handling** | **38.9×** | **536 ms** | 32 |
| skewed, AQE skew join | 5.3× | 156 ms | 38 |
| skewed, manual salting (8 salts) | 7.7× | 395 ms | 32 |

AQE split the hot partition (32 → 38 tasks) and the slowest task fell from 536 ms to 156 ms. The read-size ratios are deterministic; the millisecond columns are single runs and vary by tens of ms between runs. Salting also flattened
the load, at the price of duplicating the dimension table 8× and extra code.

**Condition worth knowing:** the AQE skew-join optimisation is skipped when splitting a partition would force an extra
shuffle. Grouping by the *join key* straight after the join reuses the join's partitioning, so AQE leaves the skew
alone (first attempt: no change, still 38.9×). Grouping by a different column lets it split. This is noted in the code.

File: `c3_skew.csv`. On real data the natural skew (Midtown, JFK, LaGuardia) would replace the injection; run it there.

## C4 · Silver scan vs gold lookup

The same question answered by aggregating silver trips and by reading a precomputed gold table. The experiment
checks that both layers return the same answer before comparing cost (`answers_match`).

| Question | Layer | Median time | Stages | Input read |
|---|---|---|---|---|
| trips per borough and month | silver scan | 0.114 s | 2 | 6.81 MB |
| | **gold lookup** | **0.032 s** | 1 | 0.005 MB |
| top-10 zones on one day | silver scan | 0.055 s | 2 | 0.08 MB |
| | gold lookup | 0.065 s | 3 | 0.017 MB |

The first question shows the point: ~3.6× faster and ~1,300× less data, because gold already holds the answer. The
second is a tie (silver was slightly faster in the final run) because silver is partitioned by `pickup_date`, so the day filter prunes to one small
partition; partition pruning is a form of precomputation too. Differences grow with data size; the sample is small.

Detail: gold is built on the dense zone × hour grid, so for a day with fewer than 10 active zones it lists zero-trip
zones that silver cannot see. The comparison filters `trips > 0` on the gold side to stay like-for-like (found when
the first test run disagreed).

Files: `c4_silver_vs_gold.csv` (summary), `c4_silver_vs_gold_runs.csv` (every run).

## Related, from the same theme

* **Caching (concept 8)** is benchmark E4 in [04_evaluation.md](04_evaluation.md); it was rewritten to compare a
  reuse that is expensive to recompute with one that is a cheap scan.
* **Idempotent and incremental processing (concept 11)** and **data quality at scale (concept 12)** are in
  [08_incremental_and_dq.md](08_incremental_and_dq.md).
* **Not done:** concepts 5, 9 and 10 (shuffle over a real network, 1 vs 2 workers, killing a worker) need a second
  node or a Docker Spark cluster; they are the stretch goal E12 in the plan.
