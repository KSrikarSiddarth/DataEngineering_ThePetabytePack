#!/usr/bin/env bash
# Creates the MobilityPulse topics (idempotent). Runs inside the kafka-init container.
set -euo pipefail
BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"

create() {
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 --config retention.ms=172800000
}

create "${TRIPS_TOPIC:-taxi-trips}" "${TRIPS_PARTITIONS:-4}"
create "${DLQ_TOPIC:-taxi-trips-dlq}" 1

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
