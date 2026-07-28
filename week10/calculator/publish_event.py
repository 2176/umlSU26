"""Publish one ImagePushed event to Kafka. Run by the Week 9 capstone pipeline
inside the kafka-python image (fed to `python -` over stdin), after the pipeline
has built and pushed the image. Values come from the pipeline via env vars.
"""
import json
import os

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("BROKER", "week9-kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
event = {
    "event": "ImagePushed",
    "image": "calculator",
    "tag": os.environ.get("TAG", "0"),
    "registry": "localhost:5001",
    "branch": os.environ.get("BRANCH", "main"),
}
producer.send(os.environ.get("TOPIC", "ci.images"), event).get(timeout=10)
producer.flush()
print("announced to Kafka:", event)
