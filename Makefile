HOST   ?= 0.0.0.0
PORT   ?= 6432
CONFIG ?= ./pgcrab.toml

.PHONY: dev
dev:
	cargo run -- --host $(HOST) --port $(PORT) --config $(CONFIG)

.PHONY: setup-db
setup-db:
	./scripts/setup-db.sh

.PHONY: reset-db
reset-db:
	./scripts/reset-db.sh
