"""Row -> JSON message bytes, including deliberately malformed output."""
import json
import math
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Record:
    row: dict
    ts: "datetime | None"          # pickup time, used for ordering and pacing
    key: "str | None"
    faults: list = field(default_factory=list)
    malformed: bool = False
    locked: set = field(default_factory=set)   # columns changed by an invalid-data fault; null injection skips them


def _json_safe(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def row_to_json(row):
    """Valid JSON for one row: ISO-8601 timestamps, NaN/inf -> null, nulls preserved."""
    return json.dumps({k: _json_safe(v) for k, v in row.items()}, separators=(",", ":"), allow_nan=False)


def make_key(value):
    return None if value is None else str(value).encode("utf-8")


def malform(text, mode, rng):
    """Return bytes that are NOT valid JSON (or empty), according to `mode`."""
    if mode == "bad_encoding":
        return text.encode("utf-8") + b"\xff\xfe"
    if mode == "empty_value":
        return b""
    # truncate: cut at a random point; retry shorter if the cut is still valid JSON
    cut = max(1, int(len(text) * rng.uniform(0.3, 0.8)))
    while cut > 1:
        candidate = text[:cut]
        try:
            json.loads(candidate)
        except ValueError:
            return candidate.encode("utf-8")
        cut //= 2
    return b"{"


def to_message(record, malformed_mode, rng):
    """Return (key_bytes_or_None, value_bytes) for a record."""
    text = row_to_json(record.row)
    value = malform(text, malformed_mode, rng) if record.malformed else text.encode("utf-8")
    return make_key(record.key), value
