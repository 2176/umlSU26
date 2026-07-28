"""Announce that the pipeline pushed an image, so a consumer can react to it.

This runs in the pipeline's announce step (see the Jenkinsfile), inside the
kafka-python image, after the image has been built and pushed.

TODO (Exercise 10, Part 2): the consumer needs to know WHICH version to test and
promote. Add the build's version to the event below. The pipeline builds
calculator:<build number> and provides that number as the VERSION env var.
"""
import json
import os

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("BROKER", "week10-kafka:29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
event = {
    "event": "ImagePushed",
    "image": "calculator",
    "registry": "localhost:5001",
    # TODO: add the version, for example  "version": os.environ["VERSION"]
}
producer.send(os.environ.get("TOPIC", "ci.images"), event).get(timeout=10)
producer.flush()
print("announced:", event)
