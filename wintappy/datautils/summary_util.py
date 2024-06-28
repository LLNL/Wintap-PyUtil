import logging
from importlib.resources import files as resource_files

from wintappy.datautils import rawutil as ru


def create_lolbas_view(con, dataset):
    # Map in the LOLBAS data
    sql = f"""
        CREATE OR REPLACE view lolbas
        AS
        SELECT * FROM read_csv_auto('{dataset}/../lookups/benignware/lolbas.csv',header=true,normalize_names=1)
    """
    con.execute(sql)
    return True


def create_mitre_labels_view(con, dataset, agglevel="rolling") -> bool:
    created_empty = False
    globs = ru.get_glob_paths_for_dataset(
        dataset, subdir=agglevel, include="mitre_labels"
    )
    if "mitre_labels" in globs.keys():
        logging.info("Found MITRE_LABELS")
        ru.create_raw_views(con, globs)
    else:
        # Create an empty view definition. This allows subsequent queries to run.
        # Use "false" to return no rows, but still gets the schema definition.
        logging.info("TODO: Creating empty MITRE_LABELS")
        #        sql = f"create view sigma_labels as select * from '{dataset}/samples/sigma_labels.parquet' where false "
        #        con.execute(sql)
        created_empty = True
    return created_empty


def create_networkx_view(con, dataset):
    # Set max JSON size to 64MB
    sql = f"create or replace view labels_networkx as select * from read_json_auto('{dataset}/labels/networkx/*.json', filename=true, maximum_object_size=67108864)"
    con.execute(sql)
    return True


def create_process_view(con, dataset, agglevel="rolling"):
    # TODO: include uses startswith, change to allow discrete or wildcards.
    globs = ru.get_glob_paths_for_dataset(dataset, subdir=agglevel, include="process")
    ru.create_raw_views(con, globs)


def create_sigma_labels_view(con, dataset, agglevel="rolling") -> bool:
    created_empty = False
    globs = ru.get_glob_paths_for_dataset(
        dataset, subdir=agglevel, include="sigma_labels"
    )
    if "sigma_labels" in globs.keys():
        logging.info("Found SIGMA_LABELS")
        ru.create_raw_views(con, globs)
    else:
        # Create an empty view definition. This allows subsequent queries to run.
        # Use "false" to return no rows, but still gets the schema definition.
        logging.info("Creating empty SIGMA_LABEL")
        sql = f"create view sigma_labels as select * from '{dataset}/samples/sigma_labels.parquet' where false "
        con.execute(sql)
        created_empty = True
    return created_empty


def create_lookups(con, dataset, include=None):
    globs = ru.get_glob_paths_for_dataset(
        # TODO ../lookups is currently required to get the SIGMA/MITRE Labels working correctly.
        # TODO Consider simplifying get_glob_paths_for_dataset() to look in specific directory, no special lookups code.
        dataset,
        subdir=f"../lookups/{include}",
    )
    ru.create_raw_views(con, globs)
