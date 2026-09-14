import os

from confluent_kafka.admin import AdminClient


BROKER_URL = os.environ.get("BROKER_URL", "PLAINTEXT://localhost:9092")


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
