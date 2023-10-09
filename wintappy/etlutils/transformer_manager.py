from dataclasses import dataclass
from typing import Dict

from jinja2 import Environment, PackageLoader

from wintappy.analytics.utils import QueryAnalytic, load_all
from wintappy.database.wintap_duckdb import WintapDuckDB, WintapDuckDBOptions
from wintappy.datautils import rawutil as ru


# Just a wrapper class to hold some of the commonly used objects in etl utils
@dataclass
class TransformerManager:
    analytics: Dict[str, QueryAnalytic]
    dataset_path: str
    jinja_environment: Environment
    wintap_duckdb: WintapDuckDB

    def __init__(self, current_dataset: str):
        self.dataset_path = current_dataset
        con = ru.init_db(self.dataset_path)
        ## basic setup for what we will use to run analytics
        options = WintapDuckDBOptions(con, self.dataset_path, load_analytics=False)
        self.wintap_duckdb = WintapDuckDB(options)
        self.jinja_environment = Environment(
            loader=PackageLoader("wintappy", package_path="./analytics/mitre_car/")
        )
        self.analytics = load_all(self.jinja_environment)
