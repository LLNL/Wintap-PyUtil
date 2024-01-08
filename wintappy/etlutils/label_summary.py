import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import get_configs, print_config
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def create_networkx_view(con, dataset):
    # Set max JSON size to 64MB
    sql = f"create or replace view labels_networkx as select * from read_json_auto('{dataset}/labels/networkx/*.json', filename=true, maximum_object_size=67108864)"
    con.execute(sql)


def create_process_view(con, dataset, agglevel="rolling"):
    # TODO: include uses startswith, change to allow discrete or wildcards.
    globs = ru.get_glob_paths_for_dataset(dataset, subdir=agglevel, include="process")
    ru.create_raw_views(con, globs)


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="label_summary.py",
        description="Convert networkx JSON to table summarized by PID_HASH",
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")

    args = get_configs(parser, argv)
    print_config(args)

    con = ru.init_db()
    #    globs.update(ru.get_glob_paths_for_dataset(cur_dataset, subdir="rolling", include="sigma_labels",lookups=f'{cur_dataset}/../lookups'))
    #    globs.update(ru.get_glob_paths_for_dataset(cur_dataset, subdir="rolling", include="mitre_labels"))

    # Create a views for the NETWORKX and PROCESS table. Evolve this into creating views for "dependent" table types.
    create_networkx_view(con, args.DATASET)
    create_process_view(con, args.DATASET, args.AGGLEVEL)
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["label_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
    logging.debug(con.execute("show tables").fetchall())
    ru.write_parquet(
        con,
        args.DATASET,
        ru.get_db_objects(con, exclude=["raw_", "tmp"]),
        agg_level=f"{args.AGGLEVEL}",
    )


if __name__ == "__main__":
    main(argv=None)
