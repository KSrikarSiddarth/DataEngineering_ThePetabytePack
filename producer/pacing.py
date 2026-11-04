"""Replay pacing: map event time to wall-clock time, with an optional rate cap."""
import time
from datetime import datetime


class Pacer:
    """Sleeps so that events are emitted at `speedup` x real time.

    speedup=3600 means one simulated hour passes per real second; 0 disables
    pacing. Events whose time is before the simulated clock (late or
    out-of-order records) are sent immediately. `max_eps` caps the send rate.
    """

    def __init__(self, speedup, start_ts=None, max_eps=0.0, clock=time.monotonic, sleep=time.sleep):
        self.speedup = float(speedup)
        self.start_ts = start_ts
        self.min_gap = 1.0 / max_eps if max_eps and max_eps > 0 else 0.0
        self._clock = clock
        self._sleep = sleep
        self._t0 = None
        self._next_slot = 0.0

    def wait(self, event_ts):
        now = self._clock()
        if self._t0 is None:
            self._t0 = now
        target = now
        if self.speedup > 0 and event_ts is not None:
            if self.start_ts is None:
                self.start_ts = event_ts
            target = self._t0 + (event_ts - self.start_ts).total_seconds() / self.speedup
        if self.min_gap:
            target = max(target, self._next_slot)
            self._next_slot = max(target, now) + self.min_gap
        delay = target - now
        if delay > 0.001:
            self._sleep(delay)
