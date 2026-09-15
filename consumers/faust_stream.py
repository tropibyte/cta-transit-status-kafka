"""Defines trends calculations for stations"""
import logging
import os

import faust


logger = logging.getLogger(__name__)


# Faust uses its own `kafka://` URL scheme rather than librdkafka's `PLAINTEXT://`.
BROKER_URL = os.environ.get("FAUST_BROKER_URL", "kafka://localhost:9092")

# Produced by the Kafka Connect JDBC source (topic.prefix `org.chicago.cta.` + table name).
INPUT_TOPIC = "org.chicago.cta.stations"

# Consumed by `consumers/server.py`, which expects this exact name and plain JSON.
OUTPUT_TOPIC = "org.chicago.cta.stations.table.v1"


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=BROKER_URL, store="memory://")
topic = app.topic(INPUT_TOPIC, value_type=Station)
out_topic = app.topic(OUTPUT_TOPIC, partitions=1)

# Pointing the table's changelog at out_topic is what publishes each transformed station:
# writing to the table writes a record to that topic, which the web server then consumes.
table = app.Table(
    "stations_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_stations(stations):
    """Reduces the raw Postgres station rows to the fields the web server actually needs"""
    async for station in stations:
        # The database models the line as three booleans; the UI wants one colour string.
        if station.red is True:
            line = "red"
        elif station.blue is True:
            line = "blue"
        elif station.green is True:
            line = "green"
        else:
            # 19 of the 111 station ids belong to none of the three simulated lines (Brown,
            # Purple, Pink and so on). They are still emitted, with an empty line, so that
            # every station id present in the input topic is represented in the output --
            # skipping them would leave the output topic incomplete.
            #
            # Nothing downstream is disturbed: `consumers/models/lines.py` routes on the
            # line colour and discards anything that is not red, blue or green, so these
            # records never reach the UI.
            line = ""

        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )


if __name__ == "__main__":
    app.main()
