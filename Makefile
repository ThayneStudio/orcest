.PHONY: test test-unit redis-up redis-down lint format lock

# Unit tests only (no Redis needed)
test-unit:
	pytest -m unit

# All tests — starts Redis, runs everything, stops Redis
test: redis-up
	pytest -v; ret=$$?; $(MAKE) redis-down; exit $$ret

# Start Redis in Docker for integration/stress tests
redis-up:
	docker compose up -d redis
	@echo "Waiting for Redis..."
	@timeout 30 bash -c 'until docker compose exec redis redis-cli ping 2>/dev/null | grep -q PONG; do sleep 0.2; done'

redis-down:
	docker compose stop redis
	docker compose rm -f redis

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

lock:
	pip-compile pyproject.toml --output-file requirements.lock --strip-extras
