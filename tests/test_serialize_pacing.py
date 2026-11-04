import json
import random
from datetime import datetime, timedelta

import pytest

from pacing import Pacer
from serialize import Record, make_key, malform, row_to_json, to_message


def test_row_to_json_formats_timestamps_and_nan():
    text = row_to_json({"t": datetime(2026, 1, 2, 3, 4, 5), "x": float("nan"), "y": None, "z": 3})
    assert json.loads(text) == {"t": "2026-01-02T03:04:05", "x": None, "y": None, "z": 3}


def test_make_key():
    assert make_key(None) is None
    assert make_key("12") == b"12"


@pytest.mark.parametrize("mode", ["truncate", "bad_encoding", "empty_value"])
def test_malformed_output_is_not_valid_json(mode):
    text = row_to_json({"a": 1, "b": "hello", "c": [1, 2, 3]})
    out = malform(text, mode, random.Random(1))
    if mode == "empty_value":
        assert out == b""
        return
    with pytest.raises(ValueError):
        json.loads(out.decode("utf-8"))


def test_to_message_normal_and_malformed():
    rec = Record(row={"a": 1}, ts=None, key="7")
    key, value = to_message(rec, "truncate", random.Random(0))
    assert key == b"7" and json.loads(value) == {"a": 1}
    rec.malformed = True
    _, bad = to_message(rec, "truncate", random.Random(0))
    with pytest.raises(ValueError):
        json.loads(bad.decode())


class FakeTime:
    def __init__(self):
        self.now, self.slept = 0.0, []

    def clock(self):
        return self.now

    def sleep(self, s):
        self.slept.append(s)
        self.now += s


def test_pacer_speedup_sleeps_proportionally():
    ft = FakeTime()
    start = datetime(2026, 1, 1)
    p = Pacer(3600, start, clock=ft.clock, sleep=ft.sleep)
    p.wait(start)                             # t=0 -> no sleep
    p.wait(start + timedelta(hours=1))        # 1 simulated hour -> 1 real second
    assert ft.slept == [pytest.approx(1.0)]
    p.wait(start + timedelta(minutes=5))      # earlier than the clock -> send immediately
    assert len(ft.slept) == 1


def test_pacer_speedup_zero_never_sleeps():
    ft = FakeTime()
    p = Pacer(0, clock=ft.clock, sleep=ft.sleep)
    for i in range(5):
        p.wait(datetime(2026, 1, 1) + timedelta(hours=i))
    assert ft.slept == []


def test_pacer_rate_cap():
    ft = FakeTime()
    p = Pacer(0, max_eps=10, clock=ft.clock, sleep=ft.sleep)
    for _ in range(11):
        p.wait(None)
    assert ft.now == pytest.approx(1.0, abs=0.01)   # 11 events at 10/s -> first immediate, 10 more over 1s
