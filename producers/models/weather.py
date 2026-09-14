"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
import os
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = os.environ.get("REST_PROXY_URL", "http://localhost:8082")

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        # The schemas are loaded before the base class runs so that the topic this producer
        # creates and the payload it POSTs to REST Proxy are described by the same files.
        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

        # Weather is produced through REST Proxy rather than the Python client, but the topic
        # is still created up front rather than relying on broker auto-creation. The name is
        # fixed by `consumers/server.py`, which subscribes to it directly.
        super().__init__(
            "org.chicago.cta.weather.v1",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        try:
            resp = requests.post(
                f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
                # The Avro embedded format requires this exact Content-Type. Sending plain
                # application/json makes REST Proxy reject the schemas with a 415.
                headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
                data=json.dumps(
                    {
                        # REST Proxy wants the schemas as JSON *strings*, not as nested
                        # objects, hence the inner json.dumps on each.
                        "key_schema": json.dumps(Weather.key_schema),
                        "value_schema": json.dumps(Weather.value_schema),
                        "records": [
                            {
                                "key": {"timestamp": self.time_millis()},
                                "value": {
                                    "temperature": self.temp,
                                    "status": self.status.name,
                                },
                            }
                        ],
                    }
                ),
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("failed to post weather data to rest proxy: %s", e)
            return

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
