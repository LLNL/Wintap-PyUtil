import argparse
import dataclasses
import json
import logging
import sys
from datetime import datetime

import fsspec

from wintappy.analytics.constants import TACTIC_STIX_TYPE, TECHNIQUE_STIX_TYPE
from wintappy.analytics.utils import load_attack_metadata, run_against_day
from wintappy.config import get_configs
from wintappy.database.constants import (
    CAR_ANALYTIC_COVERAGE,
    CAR_ANALYTICS_RESULTS_TABLE,
    CAR_ANALYTICS_TABLE,
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
            manager.jinja_environment,
            manager.wintap_duckdb,
            analytics_list,
            daypk=daypk,
        )
    return


def process_table(manager: TransformerManager) -> None:
    # run analytics against single table
    analytics_list = list(manager.analytics.values())
    logging.debug("running without daypk")
    # run analytics against this day
    run_against_day(manager.jinja_environment, manager.wintap_duckdb, analytics_list)
    return


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="run_enrichment.py",
        description="Run enrichements against wintap data, write out results partitioned by day",
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")
    parser.add_argument(
        "-E",
        "--populate-enrichment-tables",
        help="Add enrichment tables to the specified path",
        default="",
    )
    args = get_configs(parser, argv)

    manager = TransformerManager(current_dataset=args.DATASET, agg_level=args.AGGLEVEL)

    start_date, end_date = get_date_range(args.START, args.END, agg_level=args.AGGLEVEL)
    if start_date and end_date:
        process_range(manager, start_date, end_date)
    else:
        process_table(manager)

    if args.POPULATE_ENRICHMENT_TABLES:
        add_enrichment_tables(manager, args.POPULATE_ENRICHMENT_TABLES)


if __name__ == "__main__":
    main(argv=None)
