HOST   ?= 0.0.0.0
PORT   ?= 6432
CONFIG ?= ./pgcrab.toml
LOG    ?= info

.PHONY: dev
dev:
	@if command -v lsof >/dev/null 2>&1; then \
		pids=$$(lsof -tiTCP:$(PORT) -sTCP:LISTEN 2>/dev/null); \
		if [ -n "$$pids" ]; then \
			echo "killing listeners on port $(PORT): $$pids"; \
			kill $$pids >/dev/null 2>&1 || true; \
			for _ in 1 2 3 4 5; do \
				if ! lsof -tiTCP:$(PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
					break; \
				fi; \
				sleep 0.2; \
			done; \
			if lsof -tiTCP:$(PORT) -sTCP:LISTEN >/dev/null 2>&1; then \
				echo "force killing listeners on port $(PORT)"; \
				kill -9 $$pids >/dev/null 2>&1 || true; \
			fi; \
		fi; \
	else \
		echo "lsof not found; skipping port cleanup"; \
	fi
	cargo run -- --host $(HOST) --port $(PORT) --config $(CONFIG) --log $(LOG)

.PHONY: setup-db
setup-db:
	./scripts/setup-db.sh

.PHONY: reset-db
reset-db:
	./scripts/reset-db.sh
