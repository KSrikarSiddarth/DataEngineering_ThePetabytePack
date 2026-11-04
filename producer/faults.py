"""Fault injection for the stream (null fields, rule violations, duplicates, late and
out-of-order records, malformed JSON, schema drift), driven by stream-config.yaml.

Record-level faults change one record (`Injector.mutate`). Stream-level faults change
delivery order or count (`Injector.stream`). Every injected fault is tagged on
`Record.faults`, and the producer sends the tags as a Kafka header so tests and
Spark reconciliation can tell injected problems from natural ones.
"""
import copy
import heapq
import itertools
from datetime import timedelta

INVALID_TYPES = (
    "dropoff_before_pickup",
    "negative_amount",
    "negative_distance",
    "pickup_out_of_range",
    "unknown_location_id",
)


class Injector:
    def __init__(self, spec, rates, quality, rng):
        self.spec = spec
        self.rates = rates
        self.q = quality
        self.rng = rng
        self.columns = set(spec["columns"])

    # -- record-level -----------------------------------------------------
    def mutate(self, rec):
        r = self.rates
        if self.rng.random() < r["invalid_rate"]:
            self._invalid(rec)
        if self.rng.random() < r["null_rate"]:
            self._nulls(rec)
        if self.rng.random() < r["schema_drift_rate"]:
            rec.row[self.q["schema_drift"]["extra_field_name"]] = self.rng.randint(0, 9)
            rec.faults.append("schema_drift")
        if self.rng.random() < r["malformed_json_rate"]:
            rec.malformed = True
            rec.faults.append("malformed_json")
        return rec

    def applicable_invalid_types(self):
        s = self.spec
        out = []
        if s.get("pickup_col") in self.columns and s.get("dropoff_col") in self.columns:
            out.append("dropoff_before_pickup")
            out.append("pickup_out_of_range")
        if s.get("amount_cols"):
            out.append("negative_amount")
        if s.get("distance_col"):
            out.append("negative_distance")
        if s.get("location_cols"):
            out.append("unknown_location_id")
        return out

    def _invalid(self, rec):
        weights = self.q["invalid_injection"]["types"]
        choices = [t for t in self.applicable_invalid_types() if weights.get(t, 0) > 0]
        if not choices:
            return
        kind = self.rng.choices(choices, weights=[weights[t] for t in choices])[0]
        s, row, rng = self.spec, rec.row, self.rng
        if kind == "dropoff_before_pickup":
            pickup = row.get(s["pickup_col"])
            if pickup is None:
                return
            rec.locked.add(s["dropoff_col"])
            row[s["dropoff_col"]] = pickup - timedelta(minutes=rng.randint(1, 120))
        elif kind == "pickup_out_of_range":
            pickup = row.get(s["pickup_col"])
            if pickup is None:
                return
            rec.locked.add(s["pickup_col"])
            row[s["pickup_col"]] = pickup + timedelta(days=rng.choice([-90, 90, 400]))
        elif kind == "negative_amount":
            col = rng.choice(s["amount_cols"])
            value = row.get(col)
            rec.locked.add(col)
            row[col] = -abs(value) if value else -5.0
        elif kind == "negative_distance":
            col = s["distance_col"]
            value = row.get(col)
            rec.locked.add(col)
            row[col] = -abs(value) if value else -1.0
        elif kind == "unknown_location_id":
            col = rng.choice(s["location_cols"])
            rec.locked.add(col)
            row[col] = self.q["invalid_injection"]["unknown_location_id_value"]
        rec.faults.append(f"invalid:{kind}")

    def _nulls(self, rec):
        fields = [
            f
            for f in self.q["null_injection"]["eligible_fields"].get(self.spec["name"], [])
            if f in self.columns and f != self.spec["key_col"] and f != self.spec["pickup_col"]
            and f not in rec.locked
        ]
        if not fields:
            return
        k = self.rng.randint(1, min(self.q["null_injection"]["max_fields_per_record"], len(fields)))
        for f in self.rng.sample(fields, k):
            rec.row[f] = None
        rec.faults.append("null_fields")

    # -- stream-level -----------------------------------------------------
    def stream(self, records):
        """records (sorted by pickup time) -> same records with order/count faults applied."""
        it = (self.mutate(r) for r in records)
        it = self._late(it)
        it = self._displace(it, self.rates["out_of_order_rate"],
                            self.q["out_of_order"]["shuffle_window_records"], "out_of_order", copy_it=False)
        it = self._displace(it, self.rates["duplicate_rate"],
                            self.q["duplicates"]["max_gap_records"], "duplicate", copy_it=True)
        return it

    def _late(self, it):
        """Hold a record back until the stream has moved `delay` minutes past its event time."""
        cfg = self.q["late_data"]
        heap, tie = [], itertools.count()
        for rec in it:
            if rec.ts is not None:
                while heap and heap[0][0] <= rec.ts:
                    yield heapq.heappop(heap)[2]
                if self.rng.random() < self.rates["late_rate"]:
                    delay = timedelta(minutes=self.rng.uniform(cfg["min_delay_minutes"], cfg["max_delay_minutes"]))
                    rec.faults.append("late")
                    heapq.heappush(heap, (rec.ts + delay, next(tie), rec))
                    continue
            yield rec
        while heap:
            yield heapq.heappop(heap)[2]

    def _displace(self, it, rate, max_gap, tag, copy_it):
        """Move (or, with copy_it, duplicate) a record to appear 1..max_gap records later."""
        copies = self.q["duplicates"]["copies"] if copy_it else 1
        heap, tie, i = [], itertools.count(), 0
        for rec in it:
            i += 1
            chosen = self.rng.random() < rate
            if chosen:
                for _ in range(copies):
                    moved = copy.copy(rec)
                    moved.row = dict(rec.row)
                    moved.faults = rec.faults + [tag]
                    heapq.heappush(heap, (i + self.rng.randint(1, max(1, max_gap)), next(tie), moved))
            if not chosen or copy_it:
                yield rec
            while heap and heap[0][0] <= i:
                yield heapq.heappop(heap)[2]
        while heap:
            yield heapq.heappop(heap)[2]
