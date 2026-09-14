"""Producer base-class providing common utilites and functionality"""
import logging
import os
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


# The defaults are the `Host URL` values from the project README. They are read from the
# environment so that the exact same code can also run inside the docker-compose network,
# where the services answer to their container names rather than to localhost.
BROKER_URL = os.environ.get("BROKER_URL", "PLAINTEXT://localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    # A single admin client is shared by every producer. Creating one per station would mean
    # a few hundred connections to the broker for no benefit.
    admin_client = AdminClient({"bootstrap.servers": BROKER_URL})

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            # Batching matters here: the turnstiles emit one event per rider, so a small
            # linger buys a lot of throughput while costing the arrival events ~1s.
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 1000,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if self.topic_name in self._broker_topics():
            logger.debug("topic %s already exists on the broker", self.topic_name)
            return

        futures = Producer.admin_client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic created: %s", topic)
            except Exception as e:
                logger.error("failed to create topic %s: %s", topic, e)

    @classmethod
    def _broker_topics(cls):
        """Returns the topics that already existed on the broker when this process started.

        Fetched once and cached -- otherwise every one of the ~140 station producers would
        issue its own metadata request just to answer the same question.
        """
        if not hasattr(cls, "_cached_broker_topics"):
            cls._cached_broker_topics = set(
                cls.admin_client.list_topics(timeout=5).topics.keys()
            )
        return cls._cached_broker_topics

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is None:
            return
        # flush() blocks until every buffered message has been delivered, so nothing that the
        # simulation produced is lost on Ctrl+C.
        self.producer.flush()
        logger.debug("producer flushed and closed for topic %s", self.topic_name)
