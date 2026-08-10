# UML DevOps — Summer 2026 (MSIT.5330)

Lab starter files for the course. Each `weekN/` folder holds that week's lab; clone
or download this repo to get the starters referenced in the weekly notes and exercises.

| Folder | Topic |
|--------|-------|
| `week8/` | Why event-driven systems — Kafka "first message" lab + a private registry lab; Exercise 8 starters in `week8/ex8/` |
| `week9/` | Message brokers — capstone step 1: the pipeline publishes an `ImagePushed` event to Kafka |

More weeks are added as the term progresses.

## Quick start (Week 8 Kafka lab)
```bash
cd week8
docker compose up -d
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
python produce.py     # publishes several events to 'orders'
python consume.py     # in a second terminal, reads them back
```
See `week8/README.md` for the full walkthrough (Kafka lab + registry lab).
Week 11 verification: the calculator workflow now runs for repository-wide pushes.
