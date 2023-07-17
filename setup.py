from setuptools import setup

install_requires = [
    "altair",
    "boto3",
    "duckdb",
    "magic_duckdb",
    "humanfriendly",
    "importlib_resources",
    "ipyfilechooser",
    "jinja2==3.0.3",
    "jinjasql",
    "matplotlib",
    "networkx",
    "pandas",
    "pyarrow",
    "python-dotenv",
    "toml",
    "tqdm"
]

setup(
    install_requires=install_requires,
)
