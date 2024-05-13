import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.datautils import summary_util as su
from wintappy.etlutils.utils import configure_basic_logging

save_db_objects = []


def label_summary(con, dataset):
    # Create a views for Labels.
    if su.create_networkx_view(con, dataset):
        save_db_objects.extend(
            [
                "labels_graph_net_conn",
                "labels_graph_nodes",
                "labels_graph_process_summary",
                "labels_networkx",
            ]
        )
        logging.debug("Found labels")
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["label_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
        # save_db_objects.append("process_label_summary")


def lolbas_summary(con, dataset):
    # Create a views for LOLBAS.
    if su.create_lolbas_view(con, dataset):
        save_db_objects.append("lolbas")
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["lolbas_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
        # save_db_objects.append("process_lolbas_summary")


def sigma_summary(con, args):
    # Create a views for SIGMA.
    if su.create_sigma_labels_view(con, args.DATASET, args.AGGLEVEL):
        save_db_objects.append("sigma_labels")
    su.create_lookups(con, args.DATASET, "sigma")
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["sigma_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
        # save_db_objects.extend(["sigma_labels_summary", "process_summary_sigma"])


def mitre_summary(con, args):
    # Create a views for the MITRE.
    if su.create_mitre_labels_view(con, args.DATASET, args.AGGLEVEL):
        save_db_objects.append("mitre_labels")
    su.create_lookups(con, args.DATASET, "mitre")
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["mitre_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
        # save_db_objects.extend(["process_mitre_summary"])


def uber_summary(con):
    # Create the final UBER Summary that joins all of the PROCESS summaries together
    ru.run_sql_no_args(
        con, resource_files("wintappy.datautils").joinpath("uber_summary.sql")
    )
    save_db_objects.extend(["process_uber_summary"])


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="uber_summary.py",
        description="Convert networkx JSON to table summarized by PID_HASH",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_start(required=False)
    env_config.add_end(required=False)
    env_config.add_dataset_path(required=True)
    env_config.add_aggregation_level(required=False)
    args = env_config.get_options(argv)

    con = ru.init_db()
    # Create a view for the PROCESS table.
    logging.info(f"Creating PROCESS view: {args.DATASET} -> {args.AGGLEVEL}")
    su.create_process_view(con, args.DATASET, args.AGGLEVEL)

    logging.info(f"Creating Label Summary view")
    label_summary(con, args.DATASET)
    logging.info(f"Creating LOLBAS Summary view")
    lolbas_summary(con, args.DATASET)
    logging.info(f"Creating MITRE Summary view")
    mitre_summary(con, args)
    logging.info(f"Creating SIGMA Summary view")
    sigma_summary(con, args)

    logging.info(f"Creating UBER Summary!!!")
    uber_summary(con)

    logging.debug(con.execute("show tables").fetchall())
    logging.debug(f"Objects to save: {save_db_objects}")
    ru.write_parquet(
        con,
        args.DATASET,
        save_db_objects,
        agg_level=f"{args.AGGLEVEL}",
    )

    logging.info("Complete")


if __name__ == "__main__":
    main(argv=None)
