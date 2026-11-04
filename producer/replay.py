"""Read parquet files and yield records in pickup-time order.

TLC files are NOT sorted by pickup time (verified for all four 2026-01 files), so
a replay needs a sort. To keep memory bounded the sort is done on indices:

1. read only the pickup column and compute the sort order for the rows we will use
   (sampling + `limit` applied here, so a capped HVFHV replay never holds 20M rows);
2. stream the file in batches and keep only the selected rows;
3. sort that subset in memory and emit it in batches.

Peak memory is therefore proportional to the selected rows, not the file size.
"""
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from serialize import Record

INT64_MAX = np.iinfo(np.int64).max
WARN_ROWS = 5_000_000


def select_sorted(path, pickup_col, columns, limit=0, fraction=1.0, seed=0, batch_rows=50_000):
    """Return (table, total_rows_in_file): selected rows sorted by pickup time (nulls last)."""
    pf = pq.ParquetFile(path)
    n = pf.metadata.num_rows
    ts = pf.read(columns=[pickup_col]).column(0).combine_chunks()
    key = pc.fill_null(ts.cast(pa.int64()), INT64_MAX).to_numpy(zero_copy_only=False)

    if fraction < 1.0:
        eligible = np.flatnonzero(np.random.default_rng(seed).random(n) < fraction)
    else:
        eligible = np.arange(n)
    order = eligible[np.argsort(key[eligible], kind="stable")]
    if limit and limit > 0:
        order = order[:limit]
    if len(order) > WARN_ROWS:
        print(f"warning: sorting {len(order):,} rows from {path.name} in memory; "
              f"set max_records to bound it", file=sys.stderr)

    selected = np.zeros(n, dtype=bool)
    selected[order] = True
    rank = np.empty(n, dtype=np.int64)
    rank[order] = np.arange(len(order))

    parts, offset = [], 0
    for batch in pf.iter_batches(batch_size=batch_rows, columns=columns):
        mask = selected[offset:offset + batch.num_rows]
        if mask.any():
            parts.append(pa.Table.from_batches([batch]).filter(pa.array(mask)))
        offset += batch.num_rows
    table = pa.concat_tables(parts) if parts else pf.schema_arrow.empty_table().select(columns)

    file_order_idx = np.flatnonzero(selected)
    table = table.take(pa.array(np.argsort(rank[file_order_idx], kind="stable")))
    return table, n


def iter_records(files, spec, limit=0, fraction=1.0, seed=0, batch_rows=50_000):
    """Yield Records across `files` (already in month order), at most `limit` in total (0 = all)."""
    columns = list(spec["columns"])
    emitted = 0
    for i, path in enumerate(files):
        remaining = limit - emitted if limit else 0
        if limit and remaining <= 0:
            return
        table, _ = select_sorted(path, spec["pickup_col"], columns, remaining, fraction, seed + i, batch_rows)
        for batch in table.to_batches(max_chunksize=batch_rows):
            for row in batch.to_pylist():
                key = row.get(spec["key_col"])
                yield Record(row=row, ts=row.get(spec["pickup_col"]), key=None if key is None else str(key))
                emitted += 1
