.PHONY: bootstrap deploy load etl seed demo test clean help

ifeq ($(OS),Windows_NT)
    PYTHON := .venv/Scripts/python
else
    PYTHON := .venv/bin/python
endif

bootstrap:
	python -m venv .venv
	$(PYTHON) -m pip install -r requirements.txt

deploy:
	$(PYTHON) -m scripts.deploy

seed:
	$(PYTHON) scripts/generate_seed.py

load:
	$(PYTHON) -m python.load.bronze

etl:
	$(PYTHON) -m python.orchestrate.native_pipeline

demo:
	@echo "Run sql/07_snowflake_features_demo.sql in Snowsight or snowsql interactively."

test:
	$(PYTHON) -m pytest -q

clean:
ifeq ($(OS),Windows_NT)
	$(PYTHON) -c "import shutil, pathlib; [shutil.rmtree(p) for p in pathlib.Path('.').rglob('__pycache__')]"
else
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.pyc' -delete
endif