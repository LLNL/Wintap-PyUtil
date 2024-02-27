from dataclasses import dataclass
from typing import Dict

from jinja2 import Environment, PackageLoader

from wintappy.analytics.utils import CARAnalytic, load_all
from wintappy.database.wintap_duckdb import WintapDuckDB, WintapDuckDBOptions
from wintappy.datautils import rawutil as ru


# Just a wrapper class to hold some of the commonly used objects in etl utils
@dataclass
class TransformerManager:
    analytics: Dict[str, CARAnalytic]
    dataset_path: str
    jinja_environment: Environment
    wintap_duckdb: WintapDuckDB

    def __init__(self, current_dataset: str, agg_level: str = ""):
        self.dataset_path = current_dataset
        con = None
        if agg_level:
            con = ru.init_db(self.dataset_path, agg_level=agg_level)
            path = f"{self.dataset_path}/{agg_level}"
        else:
            con = ru.init_db(self.dataset_path)
            path = self.dataset_path
        ## basic setup for what we will use to run analytics
        options = WintapDuckDBOptions(con, path, load_analytics=False)
        self.wintap_duckdb = WintapDuckDB(options)
        self.jinja_environment = Environment(
            loader=PackageLoader("wintappy", package_path="./analytics/mitre_car/")
        )
        self.analytics = load_all(self.jinja_environment)
