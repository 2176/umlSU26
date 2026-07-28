"""Week 10 reliability lab — an at-least-once consumer that shows the three
reliability patterns you write by hand this week:

  Pattern 1  retry with backoff   transient failures are retried, not fatal
  Pattern 2  idempotency          a message already applied is skipped
  Pattern 3  dead-letter          a poison message is quarantined, not blocking

The offset is committed AFTER the work, so a crash in between means the message is
redelivered on restart — at-least-once. That redelivery is exactly why the
idempotency check matters.

  IDEMPOTENCY=off python consumer.py   # turn the idempotency check OFF to SEE the
                                       # double-apply bug that at-least-once creates
  RESET=1   python consumer.py    # start the demo OVER: rewind to the first
                                  # message and clear the ledger. A plain run (no
                                  # RESET) RESUMES from the committed offset — that
                                  # resume is what makes a killed order redeliver.
"""
import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
TOPIC = "orders"
DLQ = "orders.dlq"
LEDGER = "ledger.txt"          # the persistent side effect: one line per applied order
MAX_RETRIES = 3
IDEMPOTENCY = os.environ.get("IDEMPOTENCY", "on").lower() != "off"
RESET = os.environ.get("RESET", "").lower() in ("1", "true", "yes")


def applied_ids():
    """Ids already in the ledger — our record of what has been processed."""
    if not os.path.exists(LEDGER):
        return set()
    return {line.split()[0] for line in open(LEDGER) if line.strip()}


def apply(order):
    """The side effect. Append to the ledger and report the running total."""
    with open(LEDGER, "a") as f:
        f.write(f'{order["id"]} {order["amount"]}\n')
    total = sum(float(line.split()[1]) for line in open(LEDGER) if line.strip())
    print(f'  applied {order["id"]} (amount {order["amount"]}) -> ledger total {total:.0f}')


seen = applied_ids()
dlq = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode())
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id="orders-reliable",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode()),
)

print(f"consumer up  (idempotency {'ON' if IDEMPOTENCY else 'OFF'})  — Ctrl-C to stop")

if RESET:
    # Start the demo over: get a partition assignment, rewind to the first message,
    # and forget everything we've applied. (A plain run resumes from the committed
    # offset instead — that resume is what shows a killed order being redelivered.)
    while not consumer.assignment():
        consumer.poll(timeout_ms=500)
    consumer.seek_to_beginning()
    if os.path.exists(LEDGER):
        os.remove(LEDGER)
    seen = set()
    print("RESET: rewound to the start of 'orders' and cleared the ledger")

try:
    for msg in consumer:
        order = msg.value
        oid = order.get("id", "?")

        # Pattern 2 — idempotency: already applied? skip it (but still commit past it).
        if IDEMPOTENCY and oid in seen:
            print(f"  skip {oid}: already processed (idempotent)")
            consumer.commit()
            continue

        # Pattern 1 — retry with backoff for a handler that may fail.
        ok = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                float(order["amount"])    # validate first — a poison (non-numeric) order fails fast here
                apply(order)              # the side effect happens...
                time.sleep(1)             # ...then a slow tail of work: your Ctrl-C window. Kill here and the
                                          # order is applied but NOT yet committed -> redelivered on restart.
                ok = True
                break
            except Exception as err:
                wait = 2 ** (attempt - 1)
                print(f"  retry {oid}: attempt {attempt} failed ({err}); backoff {wait}s")
                time.sleep(wait)

        if ok:
            seen.add(oid)
            consumer.commit()            # commit AFTER the work -> at-least-once
        else:
            # Pattern 3 — dead-letter: quarantine the poison and move on so it does
            # not block everything behind it in the partition.
            dlq.send(DLQ, {"original": order, "reason": f"failed after {MAX_RETRIES} retries"}).get(timeout=10)
            print(f"  DLQ  {oid}: routed to '{DLQ}', moving on")
            consumer.commit()
except KeyboardInterrupt:
    print("\nstopped — any in-flight order was left uncommitted, so it will be redelivered on the next run.")
