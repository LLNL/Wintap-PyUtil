from setuptools import setup

install_requires = [
    "altair",
    "boto3",
    "duckdb",
    "dynaconf",
    "fsspec",
    "gitpython",
    "humanfriendly",
    "importlib_resources",
    "ipyfilechooser",
    "jinja2",
    "jinjasql",
    "lxml",
    "magic_duckdb",
    "matplotlib",
    "mitreattack-python",
    "networkx",
    "pandas",
    "pyarrow",
    "python-dotenv",
    "pyyaml",
    "s3fs",
    "toml",
    "tqdm",
]

setup(
    install_requires=install_requires,
)
