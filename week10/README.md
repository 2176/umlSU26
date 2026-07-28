# Week 10 — Building Reliable Systems

Starter code for the Week 10 reliability lab and the running capstone. Everything
runs locally against one Kafka broker; the optional failover demo uses three.

## Prereqs

- Docker running
- A Python virtualenv:

```bash
cd week10
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Start the broker and create the topics

Auto-create is off on purpose (you name your topics deliberately), so create them once:

```bash
docker compose up -d
for t in orders orders.dlq ci.images ci.tests ci.images.dlq; do
  docker exec week10-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$t" \
    --partitions 1 --replication-factor 1 --if-not-exists
done
```

---

## Part 1 — Reliability lab (`producer.py`, `consumer.py`)

`consumer.py` is an at-least-once consumer that commits its offset **after** the
work, and shows the three patterns you write by hand this week: retry with backoff,
idempotency (a dedup check against a `ledger.txt`), and dead-letter routing.

**Redelivery + idempotency.** `RESET=1` starts a demo over (rewind to the first
message, clear the ledger); a plain run **resumes** from where it left off — that
resume is what redelivers a killed order. `DEDUP=off` turns the idempotency check off.

```bash
python producer.py                     # 5 orders
DEDUP=off RESET=1 python consumer.py    # start fresh; kill it (Ctrl-C) mid-batch...
DEDUP=off python consumer.py            # ...resume: the redelivered order applies TWICE
```

Now with idempotency on (the default), the redelivered order is skipped instead:

```bash
RESET=1 python consumer.py              # start fresh; kill mid-batch...
python consumer.py                      # ...resume: the redelivered order is skipped
```

**Dead-letter.** Inject a poison (malformed) message; the consumer retries it with
backoff, gives up, routes it to `orders.dlq`, and keeps going:

```bash
python producer.py --poison
docker exec week10-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning --timeout-ms 4000
```

(`GROUP=<name>` overrides the consumer group if you want a fresh read from the start.)

---

## Part 2 — Capstone: react to your build, reliably

The Week 9 pipeline announced `ImagePushed` and stopped. Here two consumers react:

- **`tester.py`** — on `ImagePushed`, deploys the image (`docker pull` + run) and
  acceptance-tests it (`GET /sum?a=1&b=2` must be `3`), then announces `TestsPassed`
  or `TestsFailed`. Idempotent (skip a tag already tested), retries the check while
  the container starts, dead-letters an image that never becomes testable.
- **`promoter.py`** — on `TestsPassed`, tags the image `:latest` and pushes it.
  Idempotent: never promotes the same tag twice.
- **`app/`** — a tiny Flask `/sum` service (the real, testable image; replaces the
  Week 9 throwaway).
- **`emit_imagepushed.py`** — stands in for the pipeline's announce so you can drive
  the chain without Jenkins.

The chain is **`ImagePushed` → (deploy + test) → `TestsPassed` → (promote)**.

```bash
# build + push the app image to the local registry (as the pipeline would)
docker build -t localhost:5001/calculator:1 app
docker push localhost:5001/calculator:1

python tester.py      # terminal 1
python promoter.py    # terminal 2
python emit_imagepushed.py 1   # terminal 3 -> watch it deploy, test, pass, promote
```

The tester deploys on host port **18080**; change `HOST_PORT` in `tester.py` if that
is taken. Requires the local registry from Week 9 (`localhost:5001`).

---

## Optional — survive a broker failure (Pattern 5)

A separate three-broker cluster. `demo.sh` creates a replication-factor-3 topic,
writes three messages, kills the leader, and reads them back from a survivor.

```bash
docker compose -f cluster-compose.yml -p w10 up -d
./demo.sh
docker compose -f cluster-compose.yml -p w10 down
```

Run this on its own — tear down the single-broker lab first (both want port 9092).

## Teardown

```bash
docker compose down
```
