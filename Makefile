HOST   ?= 0.0.0.0
PORT   ?= 6543
CONFIG ?= ./pgcrab.toml
USERS  ?= users.toml

.PHONY: dev
dev:
	cargo run -- --host $(HOST) --port $(PORT) --config $(CONFIG) --users $(USERS)
