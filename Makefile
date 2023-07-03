packages='./analytics/'

fmt:
	black $(packages)
	isort $(packages)

lint: 
	sqlfluff lint $(packages)
	mypy $(packages)
