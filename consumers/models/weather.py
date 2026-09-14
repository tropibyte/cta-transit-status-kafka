"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            # Produced through REST Proxy in Avro, so the consumer hands back a dict whose
            # keys are the field names from schemas/weather_value.json.
            value = message.value()
            self.temperature = value["temperature"]
            self.status = value["status"]
        except Exception as e:
            logger.error("unable to process weather message: %s", e)
