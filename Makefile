demo:
	python scripts/demo.py

test:
	PYTHONPATH=src python -m unittest discover -s tests -v

sample:
	docker compose run --rm pipeline taxi-pipeline --sample data/sample/trips.json --start 2024-01-01 --end 2024-01-04
	docker compose run --rm pipeline python scripts/run_warehouse.py

run:
	docker compose run --rm pipeline
	docker compose run --rm pipeline python scripts/run_warehouse.py
