analytics='./analytics/'
database ='./database/'

## TODO: running the commands in this file requires that a python environment 
## has already been setup and is activated.
## In the future, we could consider setting up python env for use in CI
## TODO: clean this up
fmt:
	black $(analytics)
	black $(database)
	isort $(analytics)
	isort $(database)


lint: 
	sqlfluff lint $(analytics)
#	pylint $(packages)
	mypy $(analytics)
	mypy $(database)

test:
	pytest

ci: lint test

.PHONY: fmt lint test ci
