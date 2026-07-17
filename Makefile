# Fast tier: no Postgres, no third-party test deps, runs in a few seconds.
# This is where the whole failure matrix lives (crashes, zombies, chaos).
test:
	python3 -m unittest discover -s tests -t . -v

# Integration tier: proves the DB-level guarantees (SKIP LOCKED exclusivity,
# transition trigger, ON CONFLICT intake, fencing via raw SQL).
DATABASE_URL ?= postgresql://sluice:sluice@localhost:5433/sluice

db-up:
	docker compose up -d --wait db

db-down:
	docker compose down -v

test-pg: db-up
	DATABASE_URL=$(DATABASE_URL) python3 -m unittest discover -s tests/pg -t . -v

test-all: test test-pg

# Watch a few withdrawals live their whole lives, faults included.
demo:
	python3 scripts/demo.py

api:
	python3 -m sluice.api

.PHONY: test test-pg test-all db-up db-down demo api
