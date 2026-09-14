"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = os.environ.get(
    "KAFKA_CONNECT_URL", "http://localhost:8083/connectors"
)
CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    # JSON rather than Avro on both sides: Faust consumes this topic
                    # downstream and cannot decode Confluent Avro without extra work.
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    # Kafka Connect runs inside docker-compose, so this is the Docker URL
                    # for Postgres no matter where this script itself is executed from.
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "org.chicago.cta.",
                    # The station list is effectively static reference data, so polling it
                    # every 30 seconds would be pure waste. Once every 10 minutes is plenty.
                    "poll.interval.ms": "600000",
                    "tasks.max": "1",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
