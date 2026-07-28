"""Announce an ImagePushed event by hand, so you can drive the release gate
without running Jenkins.

    python emit_imagepushed.py 1     # ImagePushed for calculator version 1
"""
import json
import sys

from kafka import KafkaProducer

version = sys.argv[1] if len(sys.argv) > 1 else "1"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode(),
)
event = {"event": "ImagePushed", "image": "calculator", "version": version, "registry": "localhost:5001"}
producer.send("ci.images", event).get(timeout=10)
producer.flush()
print("emitted", event)
