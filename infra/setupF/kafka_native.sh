#!/usr/bin/env bash
# Native single-broker Kafka (KRaft) on VM2 for the SetupF cluster. Replaces the Docker setup (infra/docker-compose.yml).
#   infra/setupF/kafka_native.sh install <VM2_IP>   download Kafka 3.9.0 to ~/kafka, configure, format storage
#   infra/setupF/kafka_native.sh start|stop         start (daemon) / stop the broker
#   infra/setupF/kafka_native.sh topics             create taxi-trips (4 partitions) and taxi-trips-dlq (idempotent)
#   infra/setupF/kafka_native.sh reset              delete all Kafka data (do this together with the stream checkpoint)
#   infra/setupF/kafka_native.sh describe           show the topics
# <VM2_IP> is VM2's private IP: it is what executors and the driver are told to connect to (advertised listener).
set -euo pipefail
KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka}"
DATA_DIR="${KAFKA_DATA_DIR:-$HOME/kafka-data}"
VERSION=3.9.0
HEAP="${KAFKA_HEAP_OPTS:--Xms512m -Xmx1g}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_LOCAL:-localhost:9092}"
TRIPS_TOPIC="${TRIPS_TOPIC:-taxi-trips}"
DLQ_TOPIC="${DLQ_TOPIC:-taxi-trips-dlq}"

format_storage() {
  "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)" \
    -c "$KAFKA_DIR/config/kraft/server.properties"
}

case "${1:-}" in
  install)
    ip="${2:?usage: kafka_native.sh install <VM2_IP>}"
    command -v java >/dev/null || { echo "install Java first: sudo apt install -y openjdk-17-jdk"; exit 1; }
    if [ ! -d "$KAFKA_DIR" ]; then
      wget -q "https://archive.apache.org/dist/kafka/$VERSION/kafka_2.13-$VERSION.tgz" -O /tmp/kafka.tgz
      tar xzf /tmp/kafka.tgz -C "$HOME" && mv "$HOME/kafka_2.13-$VERSION" "$KAFKA_DIR" && rm /tmp/kafka.tgz
    fi
    cat >> "$KAFKA_DIR/config/kraft/server.properties" <<EOP
log.dirs=$DATA_DIR
auto.create.topics.enable=false
advertised.listeners=PLAINTEXT://$ip:9092,CONTROLLER://localhost:9093
EOP
    format_storage
    echo "installed. Start with: $0 start" ;;
  start)
    KAFKA_HEAP_OPTS="$HEAP" "$KAFKA_DIR/bin/kafka-server-start.sh" -daemon "$KAFKA_DIR/config/kraft/server.properties"
    sleep 10; tail -3 "$KAFKA_DIR/logs/server.log" ;;
  stop)
    "$KAFKA_DIR/bin/kafka-server-stop.sh" ;;
  topics)
    for spec in "$TRIPS_TOPIC:${TRIPS_PARTITIONS:-4}" "$DLQ_TOPIC:1"; do
      "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "${spec%%:*}" --partitions "${spec##*:}" --replication-factor 1 --config retention.ms=172800000
    done
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list ;;
  describe)
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe ;;
  reset)
    "$KAFKA_DIR/bin/kafka-server-stop.sh" || true; sleep 5
    rm -rf "$DATA_DIR"; format_storage
    echo "Kafka data deleted. Start with: $0 start, then: $0 topics" ;;
  *) sed -n 2,10p "$0"; exit 2 ;;
esac
