packages=./wintappy
analytics=./wintappy/analytics

UV_RUN=uv run
DBT_DIR=wintap_dbt
DBT=$(UV_RUN) --project . dbt
DBT_VARS ?=
DBT_VARS_ARG=$(if $(DBT_VARS),--vars '$(DBT_VARS)',)

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

dbt-debug:
	$(DBT) debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-build:
	$(DBT) build --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) $(DBT_VARS_ARG)

dbt-test:
	$(DBT) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) $(DBT_VARS_ARG)

dbt-docs:
	$(DBT) docs generate --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

.PHONY: fmt fmt-check lint test ci venv build clean source-install setup requirements cleanpynb dbt-debug dbt-build dbt-test dbt-docs
