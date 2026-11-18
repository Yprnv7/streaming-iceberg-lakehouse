.PHONY: up down test lint bench

up:
	docker compose up --build -d

down:
	docker compose down -v

lint:
	ruff check src/ tests/ scripts/
	mypy src/

test:
	pytest tests/ -v --cov=src --cov-fail-under=80

bench:
	python scripts/run_iceberg_benchmarks.py --output docs/BENCHMARKS.md
