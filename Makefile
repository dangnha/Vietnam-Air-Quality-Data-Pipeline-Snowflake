.PHONY: bootstrap deploy load etl seed demo test clean

PYTHON ?= python

bootstrap:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

deploy:
	$(PYTHON) scripts/deploy.py

seed:
	$(PYTHON) scripts/generate_seed.py

load:
	$(PYTHON) -m python.load.bronze

etl:
	$(PYTHON) -m python.orchestrate.native_pipeline

demo:
	@echo "Run sql/07_snowflake_features_demo.sql in Snowsight or snowsql interactively."

test:
	pytest -q

clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.pyc' -delete
