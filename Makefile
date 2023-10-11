packages='./wintappy'
analytics='./wintappy/analytics'

fmt:
	pipenv run black $(packages)
	pipenv run isort $(packages)

fmt-check:
	pipenv run black --check $(packages)
	pipenv run isort --check $(packages)

lint: 
	pipenv run mypy $(packages)
	pipenv run sqlfluff lint $(analytics)

test:
	pipenv run pytest 

ci: fmt-check lint test

venv:
	pip3 install pipenv
	pipenv install --dev

build:
	rm -rf dist/
	pipenv run python setup.py sdist

clean:
	pipenv --rm

source-install:
	pipenv run -- pip install -e .

setup: venv source-install cleanpynb

cleanpynb:
	pip install nbstripout
	nbstripout --install --attributes .gitattributes

.PHONY: fmt lint test ci venv setup cleanpynb
