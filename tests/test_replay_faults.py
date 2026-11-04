import random
from collections import Counter

import pytest

from conftest import T0, write_green_parquet
from faults import Injector
from replay import iter_records, select_sorted


def rates(**kw):
    base = dict(null_rate=0, invalid_rate=0, duplicate_rate=0, late_rate=0,
                out_of_order_rate=0, malformed_json_rate=0, schema_drift_rate=0)
    base.update(kw)
    return base


def injector(spec, stream_cfg, seed=1, **kw):
    return Injector(spec, rates(**kw), stream_cfg["data_quality"], random.Random(seed))


# ---- replay -----------------------------------------------------------------

def test_replay_sorts_by_pickup(tmp_path, green_spec):
    p = tmp_path / "g.parquet"
    write_green_parquet(p, 40)
    ts = [r.ts for r in iter_records([p], green_spec, batch_rows=7)]
    assert len(ts) == 40 and ts == sorted(ts)


def test_replay_limit_takes_earliest(tmp_path, green_spec):
    p = tmp_path / "g.parquet"
    write_green_parquet(p, 40)
    ts = [r.ts for r in iter_records([p], green_spec, limit=10)]
    assert ts == [T0.replace(minute=i) for i in range(10)]


def test_replay_limit_spans_files(tmp_path, green_spec):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    write_green_parquet(a, 6)
    write_green_parquet(b, 6)
    assert len(list(iter_records([a, b], green_spec, limit=9))) == 9


def test_replay_fraction_is_repeatable(tmp_path, green_spec):
    p = tmp_path / "g.parquet"
    write_green_parquet(p, 200)
    one = [r.ts for r in iter_records([p], green_spec, fraction=0.25, seed=3)]
    two = [r.ts for r in iter_records([p], green_spec, fraction=0.25, seed=3)]
    assert one == two and 20 < len(one) < 90


def test_replay_key_is_pickup_zone(tmp_path, green_spec):
    p = tmp_path / "g.parquet"
    write_green_parquet(p, 5)
    table, n = select_sorted(p, "lpep_pickup_datetime", list(green_spec["columns"]))
    assert n == 5
    recs = list(iter_records([p], green_spec))
    assert all(r.key == str(r.row["PULocationID"]) for r in recs)


# ---- faults -----------------------------------------------------------------

def test_clean_profile_changes_nothing(green_spec, stream_cfg, records):
    recs = records(200)
    before = [dict(r.row) for r in recs]
    out = list(injector(green_spec, stream_cfg).stream(iter(recs)))
    assert [r.row for r in out] == before and all(not r.faults for r in out)


def test_null_injection_only_eligible_fields(green_spec, stream_cfg, records):
    eligible = set(stream_cfg["data_quality"]["null_injection"]["eligible_fields"]["green"])
    base = records(1)[0].row
    out = list(injector(green_spec, stream_cfg, null_rate=1.0).stream(iter(records(100))))
    for r in out:
        assert "null_fields" in r.faults
        nulled = {k for k, v in r.row.items() if v is None} - {k for k, v in base.items() if v is None}
        assert nulled and nulled <= eligible
        assert r.row["PULocationID"] is not None and r.row["lpep_pickup_datetime"] is not None


@pytest.mark.parametrize("kind", ["dropoff_before_pickup", "negative_amount", "negative_distance",
                                  "pickup_out_of_range", "unknown_location_id"])
def test_each_invalid_type_breaks_its_rule(green_spec, stream_cfg, records, kind):
    cfg = {**stream_cfg["data_quality"]}
    cfg["invalid_injection"] = {**cfg["invalid_injection"], "types": {kind: 1.0}}
    inj = Injector(green_spec, rates(invalid_rate=1.0), cfg, random.Random(5))
    r = list(inj.stream(iter(records(20))))[0]
    row = r.row
    assert r.faults == [f"invalid:{kind}"]
    if kind == "dropoff_before_pickup":
        assert row["lpep_dropoff_datetime"] < row["lpep_pickup_datetime"]
    elif kind == "negative_amount":
        assert min(row["fare_amount"], row["total_amount"]) < 0
    elif kind == "negative_distance":
        assert row["trip_distance"] < 0
    elif kind == "pickup_out_of_range":
        assert not (T0 <= row["lpep_pickup_datetime"] < T0.replace(month=2))
    else:
        assert 999 in (row["PULocationID"], row["DOLocationID"])


def test_null_injection_never_erases_an_injected_invalid_value(specs, stream_cfg):
    """FHV's DOlocationID is null-eligible; a 999 injected there must survive null injection."""
    from conftest import T0
    from serialize import Record
    spec = specs["fhv"]
    cfg = {**stream_cfg["data_quality"]}
    cfg["invalid_injection"] = {**cfg["invalid_injection"], "types": {"unknown_location_id": 1.0}}
    cfg["null_injection"] = {**cfg["null_injection"], "eligible_fields": {"fhv": ["DOlocationID"]}}
    inj = Injector(spec, rates(invalid_rate=1.0, null_rate=1.0), cfg, random.Random(2))
    for seed in range(40):
        row = {c: None for c in spec["columns"]}
        row.update(pickup_datetime=T0, dropOff_datetime=T0, PUlocationID=10, DOlocationID=20, dispatching_base_num="B1")
        rec = inj.mutate(Record(row=row, ts=T0, key="B1"))
        assert 999 in (rec.row["PUlocationID"], rec.row["DOlocationID"]), f"fault erased (iteration {seed})"


def test_fhv_has_no_fare_faults(specs, stream_cfg):
    inj = injector(specs["fhv"], stream_cfg)
    types = inj.applicable_invalid_types()
    assert "negative_amount" not in types and "negative_distance" not in types
    assert "unknown_location_id" in types


def test_duplicates_add_copies_without_losing_records(green_spec, stream_cfg, records):
    out = list(injector(green_spec, stream_cfg, duplicate_rate=1.0).stream(iter(records(50))))
    assert len(out) == 100
    assert sum("duplicate" in r.faults for r in out) == 50


def test_out_of_order_keeps_every_record(green_spec, stream_cfg, records):
    recs = records(300)
    out = list(injector(green_spec, stream_cfg, out_of_order_rate=0.3).stream(iter(recs)))
    assert Counter(r.ts for r in out) == Counter(r.ts for r in recs)
    assert [r.ts for r in out] != sorted(r.ts for r in out)


def test_late_records_arrive_after_their_delay(green_spec, stream_cfg, records):
    recs = records(400)      # one record per minute, so delays of 5-90 minutes are visible
    out = list(injector(green_spec, stream_cfg, late_rate=0.2).stream(iter(recs)))
    assert len(out) == 400
    late_positions = [i for i, r in enumerate(out) if "late" in r.faults]
    assert late_positions
    seen_max = T0
    lateness = []
    for r in out:
        if "late" in r.faults:
            lateness.append((seen_max - r.ts).total_seconds() / 60)
        seen_max = max(seen_max, r.ts)
    assert max(lateness) >= 5   # at least one record arrived 5+ minutes behind the stream


def test_fault_rates_match_config(green_spec, stream_cfg, records):
    out = list(injector(green_spec, stream_cfg, seed=9, invalid_rate=0.1, null_rate=0.2).stream(iter(records(5000))))
    invalid = sum(any(f.startswith("invalid") for f in r.faults) for r in out) / 5000
    nulls = sum("null_fields" in r.faults for r in out) / 5000
    assert 0.07 < invalid < 0.13 and 0.16 < nulls < 0.24


def test_schema_drift_and_malformed_flags(green_spec, stream_cfg, records):
    out = list(injector(green_spec, stream_cfg, schema_drift_rate=1.0, malformed_json_rate=1.0).stream(iter(records(5))))
    extra = stream_cfg["data_quality"]["schema_drift"]["extra_field_name"]
    assert all(extra in r.row and r.malformed for r in out)
