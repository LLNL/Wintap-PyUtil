import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import Any, Dict, List, Optional

import fsspec
import tqdm
import yaml
from duckdb import CatalogException
from jinja2 import Environment
from mitreattack.stix20 import MitreAttackData

from ..database.constants import ANALYTICS_RESULTS_TABLE
from ..database.wintap_duckdb import WintapDuckDB
from .constants import (
    ANALYTICS_DIR,
    ATTACK_STIX_REPO_NAME,
    ATTACK_STIX_REPO_OWNER,
    CAR_REPO_NAME,
    CAR_REPO_OWNER,
    COVERAGE,
    ENTERPRISE_DIRECTORY,
    ID,
    LATEST_ENTERPRISE_DEFINITION,
)
from .query_analytic import MITRE_CAR_TYPE, MitreAttackCoverage, QueryAnalytic

MITRE_CAR_PATH = "mitre_car"
# Maximum fsspec.get threads
MAX_WORKERS = 32
# Maximum number of retries for failed fsspec.get
MAX_RETRIES = 3


def convert_analytic_to_sql_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "sql")


def convert_analytic_to_yaml_filename(raw_id: str) -> str:
    return convert_id_to_filename(raw_id, "yaml")


def convert_id_to_filename(raw_id: str, filetype: str) -> str:
    return f'{raw_id.replace("_", "-").upper()}.{filetype}'


## Analytics Helpers


def download_one_file(fs: Any, target: str, filename: str) -> None:
    """
    Get a single file from fsspec filesystem
    Args:
        fs (Any): filesystem instance
        filename (str): filename to download
        tmp_dir (str): target directory to copy to
    """
    fs.get(filename, f"{target}{os.sep}{filename}")


def get_files(
    fs: Any, target: str, filenames: List[str], retry_attempt: int = 0
) -> None:
    """
    Get files from fsspec filesystem into the provided target path.
    Multi-threaded, TQDM progress output.
    """

    # The fs client and target is shared between threads
    func = partial(download_one_file, fs, target)

    # List for storing possible failed gets
    failed_downloads = []

    with tqdm.tqdm(desc="Getting files from filesystem", total=len(filenames)) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Using a dict for preserving the file for each future, to store it as a failure if we need that
            futures = {
                executor.submit(func, filename): filename for filename in filenames
            }
            for future in as_completed(futures):
                if future.exception():
                    failed_downloads.append(futures[future])
                    logging.error(future.exception())
                pbar.update(1)
    if len(failed_downloads) > 0:
        if retry_attempt < MAX_RETRIES:
            logging.warning(
                f"  {len(failed_downloads)} downloads have failed. Retrying."
            )
            get_files(fs, target, failed_downloads, retry_attempt + 1)
        else:
            logging.warning(
                f"  {len(failed_downloads)} files have failed. Writing to CSV."
            )


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
    tmp_dir = f"{tempfile.mkdtemp()}{os.sep}{ANALYTICS_DIR}"
    # clone car data into the temporary dir
    fs = fsspec.filesystem("github", org=CAR_REPO_OWNER, repo=CAR_REPO_NAME)
    get_files(fs, tmp_dir, fs.ls(ANALYTICS_DIR))
    # load yaml files into list of dictionaries
    for f in os.scandir(tmp_dir):
        if f.is_file() and f.name.endswith("yaml"):
            with open(f.path, "r", encoding="utf-8") as single:
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
            logging.error(f"{analytic.analytic_id}: {err.args}", stack_info=False)
    return


## MITRE ATT&CK utils
def load_attack_metadata() -> MitreAttackData:
    # create temporary dir
    tmp_dir = f"{tempfile.mkdtemp()}"
    # clone attack data into the temporary dir
    fs = fsspec.filesystem(
        "github", org=ATTACK_STIX_REPO_OWNER, repo=ATTACK_STIX_REPO_NAME
    )
    get_files(fs, tmp_dir, fs.ls(ENTERPRISE_DIRECTORY))
    # load matrix stiix data
    data = MitreAttackData(
        f"{tmp_dir}{os.sep}{ENTERPRISE_DIRECTORY}{os.sep}{LATEST_ENTERPRISE_DEFINITION}"
    )
    # remove temporary dir
    shutil.rmtree(tmp_dir)
    return data
