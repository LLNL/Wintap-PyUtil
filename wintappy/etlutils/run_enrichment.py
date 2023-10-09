import argparse
import json
import logging
import sys
from datetime import datetime
import fsspec

from wintappy.analytics.constants import (
    TECHNIQUE_STIX_TYPE,
    TACTIC_STIX_TYPE
)
from wintappy.analytics.utils import run_against_day, load_attack_metadata
from wintappy.database.constants import (
    ANALYTICS_RESULTS_TABLE,
    ANALYTICS_TABLE,
    TACTICS_TABLE,
    TECHNIQUES_TABLE
)
from wintappy.etlutils.transformer_manager import TransformerManager
from wintappy.etlutils.utils import daterange

def add_enrichment_tables(manager: TransformerManager) -> None:
    # first, load analytics metadata
    for analytic_id in manager.analytics:
        for entry in manager.analytics[analytic_id].coverage:
            for t in entry.tactics:
                manager.wintap_duckdb.insert_analytics_table(
                    analytic_id=analytic_id,
                    technique_id=entry.technique,
                    technique_stix_type=TECHNIQUE_STIX_TYPE,
                    tactic_id=t,
                    tactic_stix_type=TACTIC_STIX_TYPE,
                )
    # write out the tables
    manager.wintap_duckdb.write_table(
        ANALYTICS_TABLE, location=manager.dataset_path
    )
    # clear out results table that we just wrote out to the fs
    manager.wintap_duckdb.clear_table(ANALYTICS_TABLE)

    # Next, load tactic and technique metadata
    mitre_attack_data = load_attack_metadata()
    metadata_tables = {
        TECHNIQUES_TABLE: list(map(lambda x: json.loads(x.serialize()), mitre_attack_data.get_techniques(remove_revoked_deprecated=True))),
        TACTICS_TABLE: list(map(lambda x: json.loads(x.serialize()), mitre_attack_data.get_tactics(remove_revoked_deprecated=True)))
    }
    # Create a memory filesystem and write the dictionary data to it
    for table_name, table_data in metadata_tables.items():
        table_name_internal = f"{table_name}_internal"
        with fsspec.filesystem('memory').open(f'{table_name_internal}.json', 'w') as file:
            file.write(json.dumps(table_data))
        # Register the memory filesystem and create the table
        manager.wintap_duckdb.register_filesystem(fsspec.filesystem('memory'))
        manager.wintap_duckdb.query(f"CREATE TABLE IF NOT EXISTS {table_name_internal} AS SELECT * FROM read_json_auto('memory://{table_name_internal}.json')")
        # Insert the data into the table
        manager.wintap_duckdb.query(f"INSERT INTO {table_name_internal} SELECT * FROM read_json_auto('memory://{table_name_internal}.json')")
        # Now we need to unnest the data
        manager.wintap_duckdb.query(f"CREATE OR REPLACE VIEW {table_name} as select unnest(external_references).external_id as external_id, * from {table_name_internal}")
        # finally, write out the tables
        manager.wintap_duckdb.write_table(
            table_name, location=manager.dataset_path
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
        run_against_day(daypk, manager.jinja_environment, manager.wintap_duckdb, analytics_list)
        # write results out to the fs for this day
        manager.wintap_duckdb.write_table(
            ANALYTICS_RESULTS_TABLE, daypk, location=manager.dataset_path
        )
        # clear out results table that we just wrote out to the fs
        manager.wintap_duckdb.clear_table(ANALYTICS_RESULTS_TABLE)
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
    parser.add_argument("-E", "--populate-enrichment-tables", help="End date (YYYYMMDD)", default=False, required=False)
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

    manager = TransformerManager(current_dataset=cur_dataset)

    start_date = datetime.strptime(args.start, "%Y%m%d")
    end_date = datetime.strptime(args.end, "%Y%m%d")

    process_range(manager, start_date, end_date)

    if args.populate_enrichment_tables:
        add_enrichment_tables(manager)


if __name__ == "__main__":
    main()
