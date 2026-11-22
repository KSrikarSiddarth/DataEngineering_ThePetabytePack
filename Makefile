# NYC MobilityPulse - developer commands. Run from this directory.
# Paths in this repo contain spaces (OneDrive); every command below avoids $(CURDIR) for that reason.

VENV        ?= $(HOME)/.venvs/mp311
PY          := $(VENV)/bin/python
PYTHON311   ?= python3.11
COMPOSE     := docker compose -f infra/docker-compose.yml
MONTHS      ?= default
SAMPLE_DIR  ?= $(HOME)/mobilitypulse_sample
export JAVA_HOME ?= /usr/lib/jvm/java-17-openjdk-amd64

.PHONY: help setup ingest pipeline bronze silver demand features analytics ml anomaly export demo \
        kafka-up kafka-down kafka-ui kafka-topics stream produce bench-batch bench-stream scorecard \
        api dashboard test test-fast clean-sample concepts incremental

help:
	@grep -E '^[a-z-]+:.*?## ' Makefile | awk -F':.*?## ' '{printf "  %-14s %s\n", $$1, $$2}'
	@echo "  bronze|silver|demand|features|analytics  single batch stage for MONTHS (default: datasets.default)"
	@echo "  ml|anomaly|export                        single ML / serving stage"

setup: ## create the Python 3.11 venv and install the package (editable)
	test -x $(PY) || $(PYTHON311) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev]"

# ---------------------------------------------------------------- batch pipeline (real data)
ingest: ## download TLC/zones/NOAA for MONTHS into raw/
	$(PY) -m mobilitypulse.cli --stage ingest --months $(MONTHS)
pipeline: ## ingest + bronze..export for MONTHS
	$(PY) -m mobilitypulse.cli --stage all --months $(MONTHS)
incremental: ## process only new/unfinished/changed MONTHS (state table in <data>/state), then refresh features + analytics
	$(PY) -m mobilitypulse.cli --stage incremental --months $(MONTHS) $(if $(FORCE),--force)
bronze silver demand features analytics:
	$(PY) -m mobilitypulse.cli --stage $@ --months $(MONTHS)
ml anomaly export:
	$(PY) -m mobilitypulse.cli --stage $@

demo: ## synthetic data end-to-end in SAMPLE_DIR (no internet needed)
	MOBILITYPULSE_DATA_DIR=$(SAMPLE_DIR) $(PY) -m mobilitypulse.cli --stage demo
clean-sample: ## delete the synthetic sample data dir
	rm -rf "$(SAMPLE_DIR)"

# ---------------------------------------------------------------- Kafka + streaming
kafka-up: ## start Kafka (KRaft) and create topics
	$(COMPOSE) up -d --build kafka kafka-init
kafka-ui: ## Kafka + web UI on http://localhost:8080
	$(COMPOSE) --profile ui up -d --build
kafka-down: ## stop Kafka
	$(COMPOSE) --profile ui down
kafka-topics: ## list topics
	docker exec mp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
stream: ## run the Structured Streaming job (Ctrl+C to stop)
	$(PY) -m mobilitypulse.streaming.stream_job
produce: ## replay bronze trips into Kafka (RATE=2000 MAX=200000)
	$(PY) -m mobilitypulse.streaming.producer $(if $(RATE),--rate $(RATE)) $(if $(MAX),--max-events $(MAX))

# ---------------------------------------------------------------- evaluation
bench-batch: ## batch experiments E1-E5 (needs bronze for datasets.large months)
	$(PY) -m mobilitypulse.benchmark.batch_bench
bench-stream: ## streaming experiments (needs Kafka + bronze for producer.replay_month)
	$(PY) -m mobilitypulse.benchmark.stream_bench
concepts: ## Spark internals evidence pack (lazy eval, narrow/wide, skew, silver vs gold) -> results/concepts
	$(PY) -m mobilitypulse.benchmark.concepts $(EXPERIMENTS)
test: ## full test suite with coverage -> results/tests
	mkdir -p results/tests
	$(PY) -m pytest --cov --cov-report=term --cov-report=json:results/tests/coverage.json \
		--junitxml=results/tests/junit.xml
test-fast: ## unit tests only
	$(PY) -m pytest -m "not integration"
scorecard: ## evaluate all targets -> results/evaluation_scorecard.md
	$(PY) -m mobilitypulse.benchmark.scorecard

# ---------------------------------------------------------------- serving
api: ## FastAPI on http://127.0.0.1:8000/docs
	$(PY) -m uvicorn mobilitypulse.serving.api:app --host 127.0.0.1 --port 8000
dashboard: ## Streamlit dashboard on http://127.0.0.1:8501 (localhost only)
	$(PY) -m streamlit run src/mobilitypulse/serving/dashboard.py --server.address 127.0.0.1 --server.port 8501 \
		--server.headless true --browser.gatherUsageStats false
