"""Week 10 capstone (skeleton) — the release gate.

It consumes "ready to test" events (`ImagePushed`) from Kafka. For each one it:
  1. reads the message (which image and version is ready to test),
  2. deploys that image (docker pull + run),
  3. runs an acceptance test against it,
  4. promotes it to :latest if the test passes,
  5. does nothing if the test fails.

The docker and HTTP mechanics are written for you (deploy, run_tests, promote,
teardown, dead_letter below). YOUR job is the consumer loop and the three
reliability patterns from Part 1. Look for the TODO markers.

Run it:  python release_gate.py
Feed it: python emit_imagepushed.py <version>   (in another terminal)
"""
import json
import os
import subprocess
import time
import urllib.request

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
IN = "ci.images"           # "ready to test" events arrive here
DLQ = "ci.images.dlq"      # events we give up on go here
REGISTRY = "localhost:5001"
HOST_PORT = 18080          # where we expose the candidate container
MAX_RETRIES = 5

producer = KafkaProducer(bootstrap_servers=BROKER,
                         value_serializer=lambda v: json.dumps(v).encode())


# ===========================================================================
# Provided for you. These do the docker and HTTP work; you should not need to
# change them. Focus on the consumer loop further down.
# ===========================================================================

def deploy(image, version):
    """Pull and start the candidate image, mapped to HOST_PORT.
    Returns True if it started, False if the pull or run failed."""
    ref = f"{REGISTRY}/{image}:{version}"
    name = f"candidate-{image}-{version}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    try:
        subprocess.run(["docker", "pull", ref], check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name,
                        "-p", f"{HOST_PORT}:5000", ref], check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError as err:
        print(f"    deploy failed: {err.stderr.decode().strip()[:120]}")
        return False


def run_tests():
    """The acceptance test: 1 + 2 must equal 3.
    Returns True (passed) or False (ran, but wrong answer).
    Raises OSError if the service is not reachable yet, so your retry loop can
    catch that, back off, and try again."""
    url = f"http://localhost:{HOST_PORT}/sum?a=1&b=2"
    answer = urllib.request.urlopen(url, timeout=3).read().decode().strip()
    print(f"    GET /sum?a=1&b=2 -> {answer!r} (want '3')")
    return answer == "3"


def promote(image, version):
    """Promote the tested image to the released tag: tag :latest and push."""
    src = f"{REGISTRY}/{image}:{version}"
    dst = f"{REGISTRY}/{image}:latest"
    subprocess.run(["docker", "pull", src], check=True, capture_output=True)
    subprocess.run(["docker", "tag", src, dst], check=True)
    subprocess.run(["docker", "push", dst], check=True, capture_output=True)
    print(f"    promoted {image}:{version} to {image}:latest")


def teardown(image, version):
    """Stop and remove the candidate container once you are done testing it."""
    subprocess.run(["docker", "rm", "-f", f"candidate-{image}-{version}"], capture_output=True)


def dead_letter(event, reason):
    """Send an event we are giving up on to the dead-letter topic."""
    producer.send(DLQ, {"original": event, "reason": reason}).get(timeout=10)
    print(f"    dead-lettered ({reason})")


# ===========================================================================
# YOUR WORK starts here: the consumer loop and the reliability patterns.
# ===========================================================================

consumer = KafkaConsumer(
    IN,
    bootstrap_servers=BROKER,
    group_id="release-gate",
    auto_offset_reset="earliest",
    enable_auto_commit=False,          # you commit yourself, AFTER processing
    value_deserializer=lambda b: json.loads(b.decode()),
)

# TODO (idempotency): keep a record of versions you have already handled, so a
# redelivered event is skipped instead of tested and promoted a second time.
# An in-memory set works to start; persisting it to a file (like Part 1's
# ledger.txt) makes it survive a restart.
handled = set()

print("release gate up — waiting for 'ready to test' events. Ctrl-C to stop.")
for msg in consumer:
    event = msg.value
    image = event.get("image")
    version = str(event.get("tag"))
    key = f"{image}:{version}"
    print(f"[event] {key} ready to test")

    # --- Pattern 2: idempotency ------------------------------------------
    # TODO: if `key` is already in `handled`, print a skip line, commit the
    #       offset, and continue. (This is what makes a redelivery harmless.)

    # --- Step 2: deploy the candidate ------------------------------------
    started = deploy(image, version)

    # --- Pattern 3: dead-letter a bad image ------------------------------
    # TODO: if `started` is False, the image will not run. dead_letter(event, ...),
    #       commit the offset, and continue so it does not block later versions.

    # --- Step 3 + Pattern 1: test, with retry and backoff ----------------
    # run_tests() raises OSError while the container is still coming up.
    # TODO: call run_tests() up to MAX_RETRIES times. Catch OSError, back off
    #       (1s, then 2s, then 4s, ...), and try again. Set `result` to:
    #         True  -> the test passed
    #         False -> it ran but gave the wrong answer (a failing build)
    #         None  -> it never became reachable (a dead-letter case)
    result = None   # <-- replace this with your retry loop

    teardown(image, version)

    # --- Steps 4 and 5: promote on pass, nothing on fail -----------------
    # TODO: result is True  -> promote(image, version)
    #       result is False -> do nothing (a failing build is not promoted)
    #       result is None  -> dead_letter(event, "never became testable")

    # --- Record and commit ------------------------------------------------
    # TODO (idempotency): add `key` to `handled` (and persist it if you chose to).
    # TODO (at-least-once): commit the offset now, AFTER all the work above, so a
    #       crash before this point causes the event to be redelivered.
    consumer.commit()
