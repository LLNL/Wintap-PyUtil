<img width="200" src="https://user-images.githubusercontent.com/50601643/218871643-2d3af433-0923-4786-b5e5-24c6a72e803e.png">

# Wintap-PyUtil
Python utilities for working with Wintap data

# Minimum System Requirements
Python 3.10

# Getting Started

Setup the python venv and install from source for development testing.

```bash
$ make venv
$ pipenv run -- pip3 install -e .
```

Import the module in a python notebook or file, or use the commandline tools

```bash
$ pipenv run rawtorolling --help
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

# Release
LLNL-CODE-837816
