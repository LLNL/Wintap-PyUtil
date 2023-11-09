import logging
import os
import shutil
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional
from duckdb import CatalogException

import git
import yaml
from duckdb import CatalogException
from jinja2 import Environment
from mitreattack.stix20 import MitreAttackData

from ..database.constants import ANALYTICS_RESULTS_TABLE
from ..database.wintap_duckdb import WintapDuckDB
from .constants import (
    ANALYTICS_DIR,
    ATTACK_STIX_REPO_URL,
    CAR_REPO_URL,
    COVERAGE,
    ID,
    LATEST_ENTERPRISE_DEFINITION,
)
from .query_analytic import MITRE_CAR_TYPE, MitreAttackCoverage, QueryAnalytic

MITRE_CAR_PATH = "mitre_car"


def convert_analytic_to_sql_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "sql")


def convert_analytic_to_yaml_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "yaml")


def convert_id_to_filename(raw_id: str, filetype: str) -> str:
    return f'{raw_id.replace("_", "-").upper()}.{filetype}'


## Analytics Helpers


def load_single(analytic_id: str) -> Optional[QueryAnalytic]:
    metadata = load_car_analtyic_metadata()
    return format_car_analytic(analytic_id, metadata)


def load_all(env: Environment) -> Dict[str, QueryAnalytic]:
    analytics: Dict[str, QueryAnalytic] = {}
    metadata = load_car_analtyic_metadata()
    for template in env.list_templates():
        if template.endswith(".sql"):
            analytic_id = template.removesuffix(".sql")
            if analytic_id in metadata:
                analytics[analytic_id] = format_car_analytic(analytic_id, metadata)
    return analytics


def load_car_analtyic_metadata() -> Dict[str, Dict[str, Any]]:
    # list to hold analytic data
    analytics = {}
    # create temporary dir
    tmp_dir = tempfile.mkdtemp()
    # clone car data into the temporary dir
    git.Repo.clone_from(CAR_REPO_URL, tmp_dir, branch="master", depth=1)
    # load yaml files into list of dictionaries
    for f in os.scandir(f"{tmp_dir}/{ANALYTICS_DIR}"):
        if f.is_file() and f.name.endswith("yaml"):
            with open(f.path, "r") as single:
                try:
                    raw_yaml = yaml.safe_load(single)
                    analytics[raw_yaml[ID]] = raw_yaml
                except yaml.YAMLError as err:
                    logging.error("error loading car analytic file: %s", f.path)
    # remove temporary dir
    shutil.rmtree(tmp_dir)
    return analytics


def format_car_analytic(analytic_id: str, metadata: Dict[str, Any]) -> QueryAnalytic:
    # format coverage as expected
    coverage = []
    for entry in metadata.get(analytic_id, {}).get(COVERAGE, []):
        coverage.append(MitreAttackCoverage(**entry))
    return QueryAnalytic(
        analytic_id=analytic_id,
        analytic_template=f"{analytic_id}.sql",
        query_type=MITRE_CAR_TYPE,
        metadata=metadata.get(analytic_id, {}),
        coverage=coverage,
    )


def run_against_day(
    daypk: int, env: Environment, db: WintapDuckDB, analytics: List[QueryAnalytic]
) -> None:
    """Runs a single or all CAR analytics against data for a single daypk."""
    for analytic in analytics:
        query_str = env.get_template(analytic.analytic_template).render(
            {"search_day_pk": daypk}
        )
        try:
            db.query(
                f"INSERT INTO {ANALYTICS_RESULTS_TABLE} SELECT pid_hash, '{analytic.analytic_id}', first_seen, 'pid_hash' FROM ( {query_str} )"
            )
        except CatalogException as err:
            # Don't include the stacktrace to keep the output succinct.
            logging.error(f"{analytic.analytic_id}: {e.args}", stack_info=False)
    return


## MITRE ATT&CK utils
def load_attack_metadata() -> MitreAttackData:
    # create temporary dir
    tmp_dir = tempfile.mkdtemp()
    # clone attack data into the temporary dir
    git.Repo.clone_from(ATTACK_STIX_REPO_URL, tmp_dir, branch="master", depth=1)
    path = f"{tmp_dir}/{LATEST_ENTERPRISE_DEFINITION}"
    # load matrix stiix data
    data = MitreAttackData(path)
    # remove temporary dir
    shutil.rmtree(tmp_dir)
    return data
