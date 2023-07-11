packages='./wintappy'

fmt:
	pipenv run black $(packages)
	pipenv run isort $(packages)

fmt-check:
	pipenv run black --check $(packages)
	pipenv run isort --check $(packages)

#lint: 
#	sqlfluff lint $(analytics)
#	pipenv run mypy $(packages)

test:
	pipenv run pytest 

ci: fmt-check

venv:
	pip3 install --user pipenv
	pipenv install --dev

build:
	rm -rf dist/
	pipenv run python setup.py sdist

clean:
	pipenv --rm

source-install:
	pipenv run -- intsall -e .

setup: venv source-install cleanpynb

cleanpynb:
	pipenv run nbstripout --install --attributes .gitattributes

.PHONY: fmt lint test ci venv setup cleanpynb
