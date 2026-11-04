ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
# Versions of Spark 3.5.3's own Kafka connector dependencies (from its pom)
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake the Kafka connector into the image so nothing is downloaded at runtime.
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    curl -fsSL -o "$JARS/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "$M/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$JARS/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
      "$M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$JARS/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -o "$JARS/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "$M/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

WORKDIR /app
COPY *.py ./

CMD ["python", "-u", "stream_job.py"]
