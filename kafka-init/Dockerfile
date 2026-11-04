ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

RUN pip install --no-cache-dir confluent-kafka==2.6.1 pyyaml==6.0.2

WORKDIR /app
COPY create_topics.py .

CMD ["python", "-u", "create_topics.py"]
