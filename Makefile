packages='./wintappy'

fmt:
	pipenv run black $(packages)
	pipenv run isort $(packages)

lint: 
	pipenv run mypy $(packages)

ci: lint

venv:
	pip3 install --user pipenv
	pipenv install --dev

build:
	rm -rf dist/
	pipenv run python setup.py sdist

clean:
	pipenv --rm

setup: venv cleanpynb

cleanpynb:
	pipenv run nbstripout --install --attributes .gitattributes

.PHONY: fmt lint test ci venv setup cleanpynb
