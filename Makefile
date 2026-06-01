packages=./wintappy
analytics=./wintappy/analytics

UV_RUN=uv run
DBT_UV_RUN ?= uv run --isolated --dev
DBT_DIR=wintap_dbt
DBT_TARGET ?= dev
DBT_SELECT ?=
DBT_SELECT_ARGS=$(if $(DBT_SELECT),--select $(DBT_SELECT),)
DBT=$(DBT_UV_RUN) --project . dbt

WINTAP_DBT_DATASET ?= $(WINTAP_DATA_ROOT)/parquet
WINTAP_DBT_DATABASE ?= $(WINTAP_DATA_ROOT)/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY ?= $(shell if [ -n "$(WINTAP_DATA_ROOT)" ] && [ -d "$(WINTAP_DATA_ROOT)/parquet/raw_sensor" ]; then find "$(WINTAP_DATA_ROOT)/parquet/raw_sensor" -type d -name 'dayPK=*' 2>/dev/null | sed 's|.*/dayPK=||' | sort | head -1; fi)
WINTAP_DBT_END_DAY ?= $(shell if [ -n "$(WINTAP_DATA_ROOT)" ] && [ -d "$(WINTAP_DATA_ROOT)/parquet/raw_sensor" ]; then find "$(WINTAP_DATA_ROOT)/parquet/raw_sensor" -type d -name 'dayPK=*' 2>/dev/null | sed 's|.*/dayPK=||' | sort | tail -1; fi)
PIDSTAT_DATA_PATH ?= $(WINTAP_DATA_ROOT)/pidstat

export WINTAP_DBT_DATASET
export WINTAP_DBT_DATABASE
export WINTAP_DBT_START_DAY
export WINTAP_DBT_END_DAY
export PIDSTAT_DATA_PATH

fmt:
	$(UV_RUN) black $(packages)
	$(UV_RUN) isort $(packages)

fmt-check:
	$(UV_RUN) black --check $(packages)
	$(UV_RUN) isort --check $(packages)

lint:
	$(UV_RUN) mypy $(packages)
	$(UV_RUN) sqlfluff lint $(analytics)

test:
	$(UV_RUN) pytest

ci: fmt-check lint test dbt-build

venv:
	uv sync --all-extras --dev

build:
	rm -rf dist/
	uv build

clean:
	rm -rf .venv dist build *.egg-info .pytest_cache .mypy_cache
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs

source-install:
	uv sync --dev

setup: venv cleanpynb

requirements:
	uv pip freeze > requirements.txt

cleanpynb:
	$(UV_RUN) nbstripout --install --attributes .gitattributes

dbt-check-config:
	@test -n "$$WINTAP_DBT_DATASET" || (echo "WINTAP_DBT_DATASET is required" && exit 1)
	@test -n "$$WINTAP_DBT_START_DAY" || (echo "WINTAP_DBT_START_DAY is required" && exit 1)
	@test -n "$$WINTAP_DBT_END_DAY" || (echo "WINTAP_DBT_END_DAY is required" && exit 1)
	@if [ "$(DBT_TARGET)" = "ilum" ] || [ "$${WINTAP_DBT_TARGET:-dev}" = "ilum" ]; then \
		test -n "$$ILUM_KYUUBI_HOST" || (echo "ILUM_KYUUBI_HOST is required for DBT_TARGET=ilum" && exit 1); \
	elif [ "$(DBT_TARGET)" = "spark" ] || [ "$${WINTAP_DBT_TARGET:-dev}" = "spark" ]; then \
		test -n "$$SPARK_CONNECT_HOST" || (echo "SPARK_CONNECT_HOST is required for DBT_TARGET=spark" && exit 1); \
	elif [ "$(DBT_TARGET)" = "duckdb_s3" ] || [ "$${WINTAP_DBT_TARGET:-dev}" = "duckdb_s3" ]; then \
		test -n "$$WINTAP_DBT_DATABASE" || (echo "WINTAP_DBT_DATABASE is required" && exit 1); \
		mkdir -p "$$(dirname "$$WINTAP_DBT_DATABASE")"; \
	else \
		test -n "$$WINTAP_DATA_ROOT" || (echo "WINTAP_DATA_ROOT is required" && exit 1); \
		test -n "$$WINTAP_DBT_DATABASE" || (echo "WINTAP_DBT_DATABASE is required" && exit 1); \
		test -d "$$WINTAP_DBT_DATASET/raw_sensor" || (echo "$$WINTAP_DBT_DATASET/raw_sensor does not exist" && exit 1); \
		mkdir -p "$$(dirname "$$WINTAP_DBT_DATABASE")"; \
	fi

print-dbt-config: dbt-check-config
	@echo "WINTAP_DATA_ROOT=$$WINTAP_DATA_ROOT"
	@echo "WINTAP_DBT_DATASET=$$WINTAP_DBT_DATASET"
	@echo "WINTAP_DBT_DATABASE=$$WINTAP_DBT_DATABASE"
	@echo "WINTAP_DBT_START_DAY=$$WINTAP_DBT_START_DAY"
	@echo "WINTAP_DBT_END_DAY=$$WINTAP_DBT_END_DAY"

dbt-debug: dbt-check-config
	$(DBT) debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET)

dbt-build: dbt-check-config
	$(DBT) build --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET) $(DBT_SELECT_ARGS)

dbt-test: dbt-check-config
	$(DBT) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET) $(DBT_SELECT_ARGS)

dbt-docs: dbt-check-config
	$(DBT) docs generate --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET)

qa-pid-hash: dbt-check-config
	duckdb -cmd ".maxwidth 240" "$$WINTAP_DBT_DATABASE" < $(DBT_DIR)/qa/pid_hash_orphan_checks.sql

qa-dashboard: dbt-check-config
	$(DBT_UV_RUN) --project . marimo run notebooks/wintap_dbt_overview.py

.PHONY: fmt fmt-check lint test ci venv build clean source-install setup requirements cleanpynb dbt-check-config print-dbt-config dbt-debug dbt-build dbt-test dbt-docs qa-pid-hash qa-dashboard
