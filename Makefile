HOST   ?= 0.0.0.0
PORT   ?= 6432
CONFIG ?= ./pgcrab.toml

.PHONY: dev
dev:
	cargo run -- --host $(HOST) --port $(PORT) --config $(CONFIG)
