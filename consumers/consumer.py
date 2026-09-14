"""Defines core consumer functionality"""
import logging
import os

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


# Defaults are the `Host URL` values from the project README; overridable so the same code
# runs unchanged inside the docker-compose network.
BROKER_URL = os.environ.get("BROKER_URL", "PLAINTEXT://localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            # One group per subscription. The four consumers in server.py each track their
            # own offsets; sharing a group id would make them steal partitions from one
            # another.
            "group.id": f"cta.consumer.{topic_name_pattern}",
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # subscribe() accepts a list of topics or regexes; patterns must start with `^`.
        # server.py passes both kinds, and librdkafka distinguishes them by that prefix.
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest is True:
                # `auto.offset.reset` only applies when the group has no committed offset.
                # Rewinding here forces a replay from the start on every restart, which is
                # what makes the full station list show up in the UI immediately.
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except SerializerError as e:
            # Raised by AvroConsumer when a message cannot be decoded against the registry.
            # Dropping it is correct -- blocking the poll loop on one bad record is not.
            logger.error("failed to deserialize message on %s: %s", self.topic_name_pattern, e)
            return 0

        if message is None:
            return 0

        if message.error() is not None:
            logger.error(
                "error consuming from %s: %s", self.topic_name_pattern, message.error()
            )
            return 0

        try:
            self.message_handler(message)
        except Exception as e:
            logger.error(
                "message handler failed for topic %s: %s", message.topic(), e
            )

        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        if self.consumer is None:
            return
        # close() commits final offsets and leaves the consumer group cleanly, which avoids
        # a rebalance stall on the next run.
        self.consumer.close()
        logger.debug("consumer closed for %s", self.topic_name_pattern)
