import argparse
import logging
import sys
from datetime import datetime

from jinja2 import Environment, PackageLoader

from wintappy.analytics.utils import load_all, run_against_day
from wintappy.database.constants import ANALYTICS_RESULTS_TABLE
from wintappy.database.wintap_duckdb import WintapDuckDB, WintapDuckDBOptions
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.rawtorolling import daterange


def process_range(
    current_dataset: str, start_date: datetime, end_date: datetime
) -> None:
    con = ru.init_db(current_dataset)
    ## basic setup for what we will use to run analytics
    options = WintapDuckDBOptions(con, current_dataset, load_analytics=False)
    wintap_duckdb = WintapDuckDB(options)
    env = Environment(
        loader=PackageLoader("wintappy", package_path="./analytics/mitre_car/")
    )
    analytics = load_all(env)
    # run analytics against input range
    for single_date in daterange(start_date, end_date):
        daypk = int(single_date.strftime("%Y%m%d"))
        logging.debug(f"running with daypk: {daypk}")
        # run analytics against this day
        run_against_day(daypk, env, wintap_duckdb, list(analytics.values()))
        # write results out to the fs for this day
        wintap_duckdb.write_table(
            ANALYTICS_RESULTS_TABLE, daypk, location=current_dataset
        )
        # clear out results table that we just wrote out to the fs
        wintap_duckdb.clear_table(ANALYTICS_RESULTS_TABLE)
    return


def main():
    parser = argparse.ArgumentParser(
        prog="run_enrichment.py",
        description="Run enrichements against wintap data, write out results partitioned by day",
    )
    parser.add_argument(
        "-d", "--dataset", help="Path to the dataset dir to process", required=True
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)", required=True)
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)", required=True)
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        help="Logging Level: INFO, WARN, ERROR, DEBUG",
    )
    args = parser.parse_args()

    try:
        logging.getLogger().setLevel(args.log_level)
        logging.getLogger().handlers[0].setFormatter(
            logging.Formatter("%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
        )
    except ValueError:
        logging.error(f"Invalid log level: {args.log_level}")
        sys.exit(1)

    cur_dataset = args.dataset

    start_date = datetime.strptime(args.start, "%Y%m%d")
    end_date = datetime.strptime(args.end, "%Y%m%d")

    process_range(cur_dataset, start_date, end_date)


if __name__ == "__main__":
    main()
