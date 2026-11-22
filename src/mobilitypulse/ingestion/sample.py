"""Synthetic RAW data generator (TLC-like trips, zone lookup, NOAA ISD CSV) for offline demos and tests.

Produces files with the same names, columns and encodings as the real sources so every downstream
stage runs unchanged. Demand follows a zone x hour-of-day x weekday x rain Poisson model.
"""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..common.config import Config
from ..common.utils import get_logger, month_range, us_holidays
from .download import Manifest, sha256_of

log = get_logger(__name__)

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
# relative pickups by hour of day (weekday), peaks at 8-9 and 17-19
HOUR_PROFILE = np.array([0.45, 0.30, 0.20, 0.15, 0.15, 0.25, 0.55, 0.95, 1.25, 1.15, 1.00, 1.00,
                         1.05, 1.05, 1.10, 1.20, 1.35, 1.50, 1.55, 1.40, 1.20, 1.05, 0.85, 0.65])
WEEKEND_PROFILE = np.array([0.80, 0.70, 0.55, 0.40, 0.25, 0.20, 0.25, 0.35, 0.55, 0.75, 0.95, 1.05,
                            1.10, 1.10, 1.10, 1.10, 1.10, 1.10, 1.10, 1.05, 1.00, 1.00, 0.95, 0.90])


def _zones() -> pd.DataFrame:
    rows = [(1, "EWR", "Newark Airport", "EWR")]
    for zid in range(2, 264):
        rows.append((zid, BOROUGHS[zid % len(BOROUGHS)], f"Synthetic Zone {zid:03d}",
                     "Yellow Zone" if zid % len(BOROUGHS) == 0 else "Boro Zone"))
    rows += [(264, "Unknown", "N/A", "N/A"), (265, "N/A", "Outside of NYC", "N/A")]
    return pd.DataFrame(rows, columns=["LocationID", "Borough", "Zone", "service_zone"])


def _hourly_weather(start: datetime, end: datetime, rng: np.random.Generator) -> pd.DataFrame:
    hours = pd.date_range(start, end, freq="h", inclusive="left")
    n = len(hours)
    doy = hours.dayofyear.to_numpy()
    hod = hours.hour.to_numpy()
    temp = 1.0 + 9 * np.sin(2 * np.pi * (doy - 105) / 365) + 4 * np.sin(2 * np.pi * (hod - 9) / 24) + rng.normal(0, 1.5, n)
    raining = np.zeros(n, dtype=bool)
    state = False
    for i in range(n):
        state = rng.random() < (0.75 if state else 0.03)
        raining[i] = state
    precip = np.where(raining, rng.gamma(1.2, 1.4, n), 0.0)
    return pd.DataFrame({"hour": hours, "temp_c": temp, "precip_mm": precip, "raining": raining,
                         "wind_mps": np.abs(rng.normal(4.5, 2.0, n)), "dew_c": temp - np.abs(rng.normal(4, 2, n)),
                         "vis_m": np.where(raining, rng.integers(2000, 9000, n), 16093)})


def _trips_for_month(month: str, active: np.ndarray, base: np.ndarray, weather: pd.DataFrame,
                     holidays: set, rng: np.random.Generator, sc: dict) -> pd.DataFrame:
    start, end = month_range([month])
    w = weather[(weather["hour"] >= start) & (weather["hour"] < end)].reset_index(drop=True)
    hours = w["hour"]
    weekend = (hours.dt.dayofweek >= 5).to_numpy() | hours.dt.date.isin(holidays).to_numpy()
    profile = np.where(weekend, WEEKEND_PROFILE[hours.dt.hour], HOUR_PROFILE[hours.dt.hour])
    rain = np.where(w["raining"].to_numpy(), 1.25, 1.0)
    lam = base[:, None] * (profile * rain)[None, :]
    counts = rng.poisson(lam)

    z_idx, h_idx = np.nonzero(counts)
    reps = counts[z_idx, h_idx]
    pu_zone = np.repeat(active[z_idx], reps)
    pickup = np.repeat(hours.to_numpy()[h_idx], reps) + rng.integers(0, 3600, reps.sum()).astype("timedelta64[s]")
    n = len(pu_zone)

    distance = np.round(rng.gamma(2.0, 1.5, n) + 0.3, 2)
    speed = np.clip(rng.normal(12, 3, n), 4, 40)
    duration_s = (distance / speed * 3600 + 60).astype("int64")
    dropoff = pickup + duration_s.astype("timedelta64[s]")
    fare = np.round(3 + 2.5 * distance + 0.5 * duration_s / 60, 2)
    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], n).astype("int32"),
        "tpep_pickup_datetime": pickup.astype("datetime64[us]"),
        "tpep_dropoff_datetime": dropoff.astype("datetime64[us]"),
        "passenger_count": pd.array(rng.choice([1, 1, 1, 2, 3, 0], n), dtype="Int64"),
        "trip_distance": distance,
        "RatecodeID": pd.array(np.ones(n, dtype="int64"), dtype="Int64"),
        "store_and_fwd_flag": "N",
        "PULocationID": pu_zone.astype("int32"),
        "DOLocationID": rng.choice(active, n).astype("int32"),
        "payment_type": rng.choice([1, 1, 1, 2], n).astype("int64"),
        "fare_amount": fare,
        "extra": 1.0, "mta_tax": 0.5, "tip_amount": np.round(fare * 0.15, 2), "tolls_amount": 0.0,
        "improvement_surcharge": 1.0, "congestion_surcharge": 2.5, "Airport_fee": 0.0,
    })
    df["total_amount"] = np.round(df["fare_amount"] + 5.0 + df["tip_amount"], 2)

    # inject the same classes of defects seen in the real TLC files
    n_bad = int(n * sc["bad_row_fraction"])
    bad = rng.choice(n, n_bad, replace=False)
    kinds = np.arange(n_bad) % 6
    df.loc[bad[kinds == 0], "total_amount"] = -df.loc[bad[kinds == 0], "total_amount"]
    df.loc[bad[kinds == 1], "PULocationID"] = 264
    df.loc[bad[kinds == 2], "trip_distance"] = 0.0
    sel = bad[kinds == 3]
    df.loc[sel, "tpep_dropoff_datetime"] = df.loc[sel, "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
    sel = bad[kinds == 4]
    df.loc[sel, "tpep_pickup_datetime"] = pd.Timestamp("2009-01-01 00:10:00")
    df.loc[sel, "tpep_dropoff_datetime"] = pd.Timestamp("2009-01-01 00:25:00")
    sel = bad[kinds == 5]
    df.loc[sel, "tpep_dropoff_datetime"] = df.loc[sel, "tpep_pickup_datetime"] + pd.Timedelta(hours=7)
    dups = df.sample(frac=0.002, random_state=int(rng.integers(1 << 31)))
    return pd.concat([df, dups], ignore_index=True)


def _noaa_rows(weather: pd.DataFrame, station: str, rng: np.random.Generator) -> pd.DataFrame:
    local = weather["hour"].dt.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT")
    w = weather.assign(utc=(local.dt.tz_convert("UTC").dt.tz_localize(None) + pd.Timedelta(minutes=51))).dropna(subset=["utc"])

    def enc_signed(v):
        return f"{int(round(v * 10)):+05d},1"

    rows = pd.DataFrame({
        "STATION": station,
        "DATE": w["utc"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "SOURCE": "7",
        "REPORT_TYPE": "FM-15",
        "QUALITY_CONTROL": "V020",
        "WND": [f"{int(d):03d},1,N,{int(round(s * 10)):04d},1" for d, s in zip(rng.integers(0, 360, len(w)), w["wind_mps"])],
        "CIG": "22000,1,9,N",
        "VIS": [f"{int(v):06d},1,9,9" for v in w["vis_m"]],
        "TMP": [enc_signed(t) for t in w["temp_c"]],
        "DEW": [enc_signed(t) for t in w["dew_c"]],
        "SLP": "10132,1",
        "AA1": [f"01,{int(round(p * 10)):04d},9,1" for p in w["precip_mm"]],
    })
    missing = rng.random(len(rows)) < 0.02
    rows.loc[missing, "TMP"] = "+9999,9"
    specials = rows.sample(frac=0.05, random_state=1).copy()
    specials["REPORT_TYPE"] = "FM-16"
    specials["DATE"] = (pd.to_datetime(specials["DATE"]) - pd.Timedelta(minutes=31)).dt.strftime("%Y-%m-%dT%H:%M:%S")
    return pd.concat([rows, specials]).sort_values("DATE").reset_index(drop=True)


def generate_sample(cfg: Config, months: list[str] | None = None, seed: int | None = None) -> dict:
    sc = cfg.section("sample")
    months = sorted(months or sc["months"])
    rng = np.random.default_rng(sc["seed"] if seed is None else seed)
    raw = cfg.path("raw")
    station = cfg.section("sources")["noaa_station"]
    dataset = cfg.section("sources")["tlc_dataset"]
    manifest = Manifest(raw)
    real = manifest.df[manifest.df["status"] != "generated"]
    if not real.empty:
        raise RuntimeError(f"{raw} already holds downloaded (real) data; use a separate MOBILITYPULSE_DATA_DIR "
                           "for synthetic samples (e.g. ~/mobilitypulse_sample)")
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def register(source: str, path, period: str) -> None:
        manifest.upsert({"source": source, "url": "synthetic://mobilitypulse", "local_path": str(path),
                         "period": period, "bytes": path.stat().st_size, "sha256": sha256_of(path),
                         "downloaded_at": now, "status": "generated"})

    zones_path = raw / "reference" / "taxi_zone_lookup.csv"
    zones_path.parent.mkdir(parents=True, exist_ok=True)
    _zones().to_csv(zones_path, index=False)
    register("taxi_zones", zones_path, "static")

    active = np.sort(rng.choice(np.arange(2, 264), size=int(sc["active_zones"]), replace=False))
    base = rng.lognormal(np.log(sc["mean_trips_per_zone_hour"]), 0.7, len(active))
    start, end = month_range(months)
    weather = _hourly_weather(start, end, rng)
    holidays = set(us_holidays(range(start.year, end.year + 1)))

    summary = {"months": months, "active_zones": len(active), "trips": {}}
    for month in months:
        trips = _trips_for_month(month, active, base, weather, holidays, rng, sc)
        path = raw / "tlc" / f"{dataset}_{month}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(trips, preserve_index=False), path)
        register("tlc_yellow", path, month)
        summary["trips"][month] = len(trips)
        log.info("sample %s: %d synthetic trips", month, len(trips))

    noaa = _noaa_rows(weather, station, rng)
    for year, part in noaa.groupby(noaa["DATE"].str[:4]):
        path = raw / "noaa" / f"{station}_{year}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        part.to_csv(path, index=False, quoting=1)
        register("noaa_isd", path, year)
    manifest.save()
    return summary
