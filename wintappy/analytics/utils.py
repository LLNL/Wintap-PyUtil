import logging
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional

import git
import yaml

from .mitre_car.constants import ANALYTICS_DIR, COVERAGE, ID, REPO_URL
from .query_analytic import MITRE_CAR_TYPE, MITREAttackCoverage, QueryAnalytic

MITRE_CAR_PATH = "mitre_car"


def convert_analytic_to_sql_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "sql")


def convert_analytic_to_yaml_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "yaml")


def convert_id_to_filename(raw_id: str, filetype: str) -> str:
    return f'{raw_id.replace("_", "-").upper()}.{filetype}'


def get_car_query(raw_id: str) -> str:
    cwd = os.path.dirname(__file__)
    sql_file = os.path.join(
        cwd, MITRE_CAR_PATH, convert_analytic_to_sql_filename(raw_id)
    )
    data = ""
    with open(sql_file, "r") as file:
        data = file.read()
    return data


def get_car_analytics() -> Dict[str, QueryAnalytic]:
    analytics: Dict[str, QueryAnalytic] = {}
    metadata = load_car_analtyic_metadata()
    cwd = os.path.dirname(__file__)
    file_path = os.path.join(cwd, MITRE_CAR_PATH)
    for f in os.scandir(file_path):
        if f.is_file() and f.name.endswith("sql"):
            analytic_id = f.name.removesuffix(".sql")
            query_str = ""
            with open(f.path, "r") as file:
                query_str = file.read()
            analytic = format_car_analytic(analytic_id, metadata, query_str)
            if analytic:
                analytics[analytic_id] = analytic
    return analytics


def load_car_analtyic_metadata() -> Dict[str, Dict[str, Any]]:
    # list to hold analytic data
    analytics = {}
    # create temporary dir
    tmp_dir = tempfile.mkdtemp()
    # clone car data into the temporary dir
    git.Repo.clone_from(REPO_URL, tmp_dir, branch="master", depth=1)
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


def format_car_analytic(
    analytic_id: str, metadata: Dict[str, Any], query_str: str
) -> Optional[QueryAnalytic]:
    # format coverage as expected
    coverage = []
    for entry in metadata.get(analytic_id, {}).get(COVERAGE, []):
        coverage.append(MITREAttackCoverage(**entry))
    # only format if we have the metadata for it
    if analytic_id not in metadata:
        logging.warning("skipping analytic (%s): missing metadata", analytic_id)
        return None
    return QueryAnalytic(
        analytic_id=analytic_id,
        query_string=query_str,
        query_type=MITRE_CAR_TYPE,
        coverage=coverage,
    )
