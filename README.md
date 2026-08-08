<img width="200" src="https://user-images.githubusercontent.com/50601643/218871643-2d3af433-0923-4786-b5e5-24c6a72e803e.png">

# Wintap-PyUtil
Python utilities for working with Wintap data

# Minimum System Requirements
Python 3.10

# Getting Started

## Installation 

To install wintappy and its dependencies, use `pip` from within your desired python environment:

```bash
$ pip install -e .
```

Import the module in a python notebook or file, or use the commandline tools

```bash
$ rawtorolling --help
usage: rawtorolling.py [-h] [-d DATASET] [-s START] [-e END] [-l LOG_LEVEL]

Convert raw Wintap data into standard form, partitioned by day

options:
  -h, --help            show this help message and exit
  -d DATASET, --dataset DATASET
                        Path to the dataset dir to process
  -s START, --start START
                        Start date (YYYYMMDD)
  -e END, --end END     End date (YYYYMMDD)
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        Logging Level: INFO, WARN, ERROR, DEBUG
```

```python
from wintappy.datautils.rawutil import init_db

connection = init_db()
print(connection.query('select 1'))
```

See `wintappy/examples/` for additional examples.

## Local Development

In order to develop on wintappy, setup a local python environment with the appropriate dependencies. A few examples on setup can be seen below.

### Using Conda

Miniconda can be used to configure a python3.10 environment as well as the required dependencies.

```bash
$ conda create -n wintappy python=3.10
$ conda activate wintappy
$ pip install -r requirements.txt
```

### Using Pipenv

If you have an installation of python3.10, you can use the provided make commands to setup a virtual environment with pipenv.

```bash
# make the environment
$ make venv
# activate the environment
$ pipenv shell
# run command tools 
$ rawtorolling --help
usage: rawtorolling.py [-h] [-d DATASET] [-s START] [-e END] [-l LOG_LEVEL]

Convert raw Wintap data into standard form, partitioned by day

options:
  -h, --help            show this help message and exit
  -d DATASET, --dataset DATASET
                        Path to the dataset dir to process
  -s START, --start START
                        Start date (YYYYMMDD)
  -e END, --end END     End date (YYYYMMDD)
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        Logging Level: INFO, WARN, ERROR, DEBUG
# run lint / test
$ make ci
```

### Updating Requirements

To update requirements, add them to the Pipfile in the appropriate location an re-generate the requirements.txt file:

```bash
$ pipenv requirements > requirements.txt
```

Pipenv can also be used to automatically add dependencies to the Pipfile:

```bash
$ pipenv install requests
```

# Release
LLNL-CODE-837816

