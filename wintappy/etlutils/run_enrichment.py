import argparse
import dataclasses
import json
import logging
import sys
from datetime import datetime

import fsspec

from wintappy.analytics.utils import (
    get_formatted_groups,
    load_attack_metadata,
    run_against_day,
)
from wintappy.config import get_config, print_config
from wintappy.database.constants import (
    CAR_ANALYTIC_COVERAGE,
    CAR_ANALYTICS_RESULTS_TABLE,
    CAR_ANALYTICS_TABLE,
    GROUPS_TABLE,
    MITRE_DIR,
    TACTICS_TABLE,
    TECHNIQUES_TABLE,
)
from wintappy.etlutils.transformer_manager import TransformerManager
from wintappy.etlutils.utils import configure_basic_logging, daterange, get_date_range


def add_enrichment_tables(
    manager: TransformerManager, enrichment_location: str
) -> None:
    # setup metadata tables
    mitre_attack_data = load_attack_metadata()
    metadata_tables = {
        CAR_ANALYTICS_TABLE: list(
            map(
                lambda x: x.table_item(),
                manager.analytics.values(),
            )
        ),
        CAR_ANALYTIC_COVERAGE: [
            item
            for obj in manager.analytics.values()
            for item in obj.coverage_table_items()
        ],
        TECHNIQUES_TABLE: list(
            map(
                lambda x: json.loads(x.serialize()),
                mitre_attack_data.get_techniques(remove_revoked_deprecated=True),
            )
        ),
        TACTICS_TABLE: list(
            map(
                lambda x: json.loads(x.serialize()),
                mitre_attack_data.get_tactics(remove_revoked_deprecated=True),
            )
        ),
        GROUPS_TABLE: get_formatted_groups(mitre_attack_data=mitre_attack_data),
    }
    # Create a memory filesystem and write the dictionary data to it
    for table_name, table_data in metadata_tables.items():
        table_name_internal = f"{table_name}_internal"
        with fsspec.filesystem("memory").open(
            f"{table_name_internal}.json", "w"
        ) as file:
            file.write(json.dumps(table_data))
        # Register the memory filesystem and create the table
        manager.wintap_duckdb.register_filesystem(fsspec.filesystem("memory"))
        if table_name in [TECHNIQUES_TABLE, TACTICS_TABLE]:
            manager.wintap_duckdb.query(
                f"CREATE TABLE IF NOT EXISTS {table_name_internal} AS SELECT * FROM read_json_auto('memory://{table_name_internal}.json')"
            )
            # Insert the data into the table
            manager.wintap_duckdb.query(
                f"INSERT INTO {table_name_internal} SELECT * FROM read_json_auto('memory://{table_name_internal}.json')"
            )
            # Now we need to unnest the data
            manager.wintap_duckdb.query(
                f"CREATE OR REPLACE VIEW {table_name} as select unnest(external_references).external_id as external_id, * from {table_name_internal}"
            )
        else:
            manager.wintap_duckdb.query(f"DROP TABLE IF EXISTS {table_name}")
            manager.wintap_duckdb.query(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_json_auto('memory://{table_name_internal}.json')"
            )
        # finally, write out the tables
        manager.wintap_duckdb.write_table(
            table_name, location=f"{enrichment_location}/{MITRE_DIR}"
        )
    return


def process_range(
    manager: TransformerManager, start_date: datetime, end_date: datetime
) -> None:
    # run analytics against input range
    analytics_list = list(manager.analytics.values())
    for single_date in daterange(start_date, end_date):
        daypk = int(single_date.strftime("%Y%m%d"))
        logging.debug(f"running with daypk: {daypk}")
        # run analytics against this day
        run_against_day(
            daypk, manager.jinja_environment, manager.wintap_duckdb, analytics_list
        )
        # write results out to the fs for this day
        manager.wintap_duckdb.write_table(
            CAR_ANALYTICS_RESULTS_TABLE, daypk, location=manager.dataset_path
        )
        # clear out results table that we just wrote out to the fs
        manager.wintap_duckdb.clear_table(CAR_ANALYTICS_RESULTS_TABLE)
    return


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="run_enrichment.py",
        description="Run enrichements against wintap data, write out results partitioned by day",
    )
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")
    parser.add_argument(
        "-E",
        "--populate-enrichment-tables",
        help="Add enrichment tables to the specified path",
        default="",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        help="Logging Level: INFO, WARN, ERROR, DEBUG",
    )
    options, _ = parser.parse_known_args(argv)

    # setup config based on env variables and config file
    args = get_config(options.config)
    # update config with CLI args
    args.update({k: v for k, v in vars(options).items() if v is not None})
    try:
        logging.getLogger().setLevel(args.LOG_LEVEL)
    except ValueError:
        logging.error(f"Invalid log level: {args.LOG_LEVEL}")
        sys.exit(1)
    print_config(args)

    manager = TransformerManager(current_dataset=args.DATASET)

    start_date, end_date = get_date_range(args.START, args.END)

    process_range(manager, start_date, end_date)

    if args.POPULATE_ENRICHMENT_TABLES:
        add_enrichment_tables(manager, args.POPULATE_ENRICHMENT_TABLES)


if __name__ == "__main__":
    main(argv=None)
