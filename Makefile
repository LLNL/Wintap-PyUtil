packages='./analytics/'

## TODO: running the commands in this file requires that a python environment 
## has already been setup and is activated.
## In the future, we could consider setting up python env for use in CI and for ease of 
## getting setup from scratch
fmt:
	black $(packages)
	isort $(packages)

lint: 
	sqlfluff lint $(packages)
#	pylint $(packages)
	mypy $(packages)

test:
	pytest

ci: lint test
