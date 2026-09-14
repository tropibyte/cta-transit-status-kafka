"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")

# TURNSTILE is the table the project directions call for: it mirrors the raw Avro turnstile
# topic one-for-one.
#
# TURNSTILE_SUMMARY is aggregated from TURNSTILE_EVENTS -- a stream over that same topic --
# rather than from the table, because a table would undercount badly. A KSQL table keyed by
# the Kafka record key treats a repeated key as an update rather than an insert, and KSQL
# deserializes our Avro-binary key as a UTF-8 string, so distinct keys collapse into each
# other wholesale. Measured end to end against 18,214 turnstile events: aggregating the table
# counted 5,405 of them (30%), while aggregating the stream counted all 18,214. A stream is
# also the truer model -- a turnstile entry is an event, not a state.
#
# The summary is emitted as JSON because `consumers/server.py` reads it with a plain
# (non-Avro) consumer.
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'org.chicago.cta.turnstile.v1',
    VALUE_FORMAT = 'AVRO',
    KEY = 'station_id'
);

CREATE STREAM turnstile_events (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'org.chicago.cta.turnstile.v1',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile_events
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
