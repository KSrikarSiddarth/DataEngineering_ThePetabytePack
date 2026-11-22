"""Builds the report figures from results/ and results_two_years/ (run from the project root)."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / "results" / "benchmarks"
OUT = ROOT / "report" / "figures"
DATA = Path.home() / "mobilitypulse_data"
OUT.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"figure.dpi": 150, "font.size": 9, "axes.grid": True, "grid.alpha": 0.3})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT / name)
    plt.close(fig)


# E1 data scaling
e1 = pd.read_csv(B / "e1_scaling.csv").groupby("variant")[["rows", "seconds", "silver_s", "demand_s", "features_s"]].median()
e1 = e1.loc[["small", "medium", "large"]]
fig, ax = plt.subplots(1, 2, figsize=(8, 3))
ax[0].plot(e1.rows / 1e6, e1.seconds, "o-", label="measured")
ax[0].plot(e1.rows / 1e6, e1.seconds.iloc[0] * e1.rows / e1.rows.iloc[0], "--", label="linear from small")
ax[0].set(xlabel="input trips (millions)", ylabel="bronze -> features (s, median of 3)", title="E1 data scaling")
ax[0].legend()
e1[["silver_s", "demand_s", "features_s"]].plot.bar(stacked=True, ax=ax[1], rot=0)
ax[1].set(xlabel="", ylabel="seconds", title="Time per stage")
save(fig, "e1_scaling.png")

# E2 shuffle partitions
e2 = pd.read_csv(B / "e2_partitions.csv").groupby("variant")["seconds"].median().sort_index()
fig, ax = plt.subplots(figsize=(4.5, 3))
ax.plot(e2.index, e2.values, "o-")
ax.set_xscale("log", base=2)
ax.set(xlabel="spark.sql.shuffle.partitions", ylabel="seconds (median of 3)", title="E2 shuffle partitions (medium)")
save(fig, "e2_partitions.png")

# E3-E5 pairs
pairs = [("e3_join", "sort_merge", "broadcast", "E3 join"), ("e4_cache", "uncached", "cached", "E4 cache"),
         ("e5_format", "csv", "parquet", "E5 format")]
fig, ax = plt.subplots(1, 3, figsize=(8, 2.8))
for a, (exp, base, alt, title) in zip(ax, pairs):
    s = pd.read_csv(B / f"{exp}.csv").groupby("variant")["seconds"].median()
    a.bar([base, alt], [s[base], s[alt]], color=["#999", "#3a7"])
    a.set(title=f"{title}: {s[base] / s[alt]:.2f}x", ylabel="seconds")
save(fig, "e3_e5_pairs.png")

# Streaming rate sweep and partition sweep
r = pd.read_csv(B / "stream_rate_sweep.csv")
fig, ax = plt.subplots(1, 2, figsize=(8, 3))
ax[0].plot(r.rate, r.rate, "--", color="#999", label="input rate")
ax[0].plot(r.rate, r.processed_eps, "o-", label="processed rate")
ax[0].set(xlabel="target events/s", ylabel="events/s", title="S5 rate sweep")
ax[0].legend()
ax[1].plot(r.rate, r.latency_p50_s, "o-", label="p50")
ax[1].plot(r.rate, r.latency_p95_s, "o-", label="p95")
ax[1].set(xlabel="target events/s", ylabel="end-to-end latency (s)", title="Latency vs input rate")
ax[1].legend()
save(fig, "s5_rate_sweep.png")

p = pd.read_csv(B / "stream_partition_sweep.csv")
fig, ax = plt.subplots(figsize=(4.5, 3))
ax.plot(p.partitions, p.latency_p50_s, "o-", label="p50")
ax.plot(p.partitions, p.latency_p95_s, "o-", label="p95")
ax.set_xscale("log", base=2)
ax.set(xlabel="Kafka partitions", ylabel="latency (s) @ 10k ev/s", title="S7 partition sweep")
ax.legend()
save(fig, "s7_partitions.png")

# Models: 3 months vs two years
m3 = pd.read_parquet(DATA / "gold" / "model_metrics")
m2 = pd.read_parquet(ROOT / "results_two_years" / "gold" / "model_metrics")
pick = lambda m: m[(m.split == "test") & (m.segment == "overall")].set_index("model")["wape_pct"]  # noqa: E731
w = pd.DataFrame({"3 months (2024-01..03)": pick(m3), "two years (2022-07..2024-06)": pick(m2)})
w = w.loc[[i for i in ["persistence", "seasonal_naive", "linear_regression", "random_forest", "gbt", "ensemble"] if i in w.index]]
fig, ax = plt.subplots(figsize=(7, 3))
w.plot.bar(ax=ax, rot=20)
ax.set(ylabel="test WAPE (%)", xlabel="", title="Forecast error by model (lower is better)")
save(fig, "models_wape.png")

# Data quality over 24 months
reasons = pd.read_csv(ROOT / "results_two_years" / "dq_rejects_by_reason.csv").set_index("month")
inputs = pd.read_csv(ROOT / "results_two_years" / "dq_summary_by_month.csv").set_index("month")["input_rows"]
pct = reasons.div(inputs, axis=0) * 100
pct = pct[pct.sum().sort_values(ascending=False).index]
fig, ax = plt.subplots(figsize=(8, 3.2))
pct.plot.bar(stacked=True, ax=ax, width=0.85)
ax.axhline(8, color="red", ls="--", lw=1, label="D2 target 8 %")
ax.set(ylabel="% of input rows rejected", xlabel="", title="Rejected rows by reason, 2022-07..2024-06 (78.5 M trips)")
ax.legend(fontsize=7, ncol=3)
save(fig, "dq_monthly.png")

print("figures written to", OUT)
