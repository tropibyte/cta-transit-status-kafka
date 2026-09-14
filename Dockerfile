# The project's pinned dependencies (confluent-kafka 1.1.0, faust 1.7.4, pandas 0.24.2) are
# Python 3.7-era and will not install on a modern interpreter, so the application code runs
# in this container rather than on the host.
FROM python:3.7-slim

# confluent-kafka ships a wheel with librdkafka bundled, but a few of faust's 2019-vintage
# transitive dependencies still build from source on slim.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

# pip's 2020+ resolver backtracks for a very long time over pins this old. The legacy
# resolver installs them in seconds and picks the same versions the project was built with.
RUN pip install --no-cache-dir "pip==20.2.4" "setuptools==45.2.0" "wheel==0.34.2"

COPY producers/requirements.txt /tmp/producers-requirements.txt
COPY consumers/requirements.txt /tmp/consumers-requirements.txt
RUN pip install --no-cache-dir -r /tmp/producers-requirements.txt \
 && pip install --no-cache-dir -r /tmp/consumers-requirements.txt

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Kept alive so each piece (simulation, faust, ksql, server) can be started on demand with
# `docker compose exec`, the way the project directions describe running them separately.
CMD ["sleep", "infinity"]
