import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from glob import glob
from importlib.resources import files as resource_files

import duckdb
import pyarrow.parquet as pq
from duckdb import CatalogException
from pyarrow.lib import ArrowInvalid


def init_db(dataset=None, agg_level="rolling", database=":memory:", lookups=""):
    """
    Initialize an in memory db instance and configure with our custom sql.
    """
    con = duckdb.connect(database=database)
    # TODO fix reference to SQL scripts
    run_sql_no_args(con, resource_files("wintappy.datautils").joinpath("initdb.sql"))
    if not dataset == None:
        globs = get_glob_paths_for_dataset(dataset, agg_level, lookups=lookups)
        create_views(con, globs)
    return con


def get_glob_paths_for_dataset(dataset, subdir="raw_sensor", include=None, lookups=""):
    """
    Build fully-qualifed glob paths for the dataset path. Return a map keyed by top level (event) dir.
    Expected structure is one of:

    Multiple files with/without Hive structure:
    {dataset}/{eventType}/[{attr=value}/..]/{filename}.parquet

    Single file at the top level:
    {dataset}/{eventType}.parquet
    """
    dataset_path = os.path.join(dataset, subdir)
    event_types = [
        os.path.join(dataset_path, fn)
        for fn in os.listdir(dataset_path)
        if include == None or fn.startswith(include)
    ]
    # optionally add lookup directory
    if lookups is not None and lookups != "":
        for path, _, files in os.walk(lookups):
            for name in files:
                if name.endswith(".parquet"):
                    event_types.append(os.path.join(path, name))
    globs = defaultdict(set)
    for cur_event in event_types:
        event_type = cur_event.split(os.sep)[-1]
        if os.path.isdir(cur_event):
            for dirpath, dirnames, filenames in os.walk(cur_event):
                if not dirnames:
                    if dirpath == cur_event:
                        # No dir globs needed
                        globs[event_type].add(f"{cur_event}{os.sep}*.parquet")
                    else:
                        # Remove the pre-fix, including event_type. Convert that to a glob.
                        subdir = dirpath.replace(cur_event, "")
                        glob = os.sep.join(["*"] * subdir.count(os.path.sep))
                        globs[event_type].add(
                            os.path.join(cur_event, glob, "*.parquet")
                        )
                        logging.debug(
                            f"{event_type} {subdir} {glob} has 0 subdirectories and {len(filenames)} files"
                        )
        else:
            # Treat as a simple, single file.
            if event_type.lower().endswith("parquet"):
                event = re.split(r"\.", event_type)[0]
                logging.info(f"{datetime.now()} Found {event} file: {event_type}")
                globs[event].add(cur_event)
    return validate_globs(globs)


def validate_globs(raw_data):
    """
    raw_data is a map of event_type -> list of globs.
    Confirm that:
    * There is only 1 glob per event_type. A poorly formed dataset dir structure could result in multiples.
    * That there are actually files in the path. It's not uncommon for collection of specific feature type to by inconsistent over time.
    * For each file, confirm that it has data. Wintap does produce empty parquet files and that confuses duckdb and some other tools.
        * If an empty file is found, delete it
        * When complete, if all files were empty, then remove the event_type/glob entry.
    """
    globs = {}
    for event_type, pathspec_set in raw_data.items():
        # Normally, we'll only have one pathspec.
        if len(pathspec_set) > 1:
            raise Exception(f"Too many leaf dirs!: {pathspec_set}")
        else:
            pathspec = next(iter(pathspec_set))
            num_files = len(glob(pathspec))
            if num_files == 0:
                logging.info(f"Not found: {pathspec}  Skipping")
            else:
                logging.info(f"Found {num_files} parquet files in {pathspec}")
                #                for file in glob(pathspec):
                #                    table=pq.read_table(file)
                #                    if table.num_rows==0:
                #                        logging.info(f'{file} is empty, deleting.')
                #                        os.remove(file)
                #                        num_files-=1
                if num_files == 0:
                    logging.info(f"Skipping empty glob: {pathspec}")
                else:
                    globs[event_type] = pathspec
    return globs


def get_globs_for(dataset, daypk):
    """
    This function is intended to reduce the raw_sensor input to a single day of activity for processing.
    First, get the full glob paths for raw_sensor.  Then, splice in the dayPK specification.
    Finally, confirm that specific path exists and has files.
    """
    globs_all = get_glob_paths_for_dataset(dataset)
    globs = {}
    for event_type, pathspec in globs_all.items():
        # Add in the daypk filter
        pathspec = pathspec.replace(
            f"{os.sep}*{os.sep}", f"{os.sep}dayPK={daypk}{os.sep}", 1
        )
        num_files = len(glob(pathspec))
        if num_files == 0:
            logging.info(f"Not found: {pathspec}  Skipping")
        else:
            logging.info(f"Found {num_files} parquet files in {pathspec}")
            # Check for empty files. These confuse duckdb and lead to schema errors.
            for file in glob(pathspec):
                try:
                    table = pq.read_table(file)
                    if table.num_rows == 0:
                        logging.info(f"{file} is empty, deleting.")
                        os.remove(file)
                except ArrowInvalid:
                    # Move invalid files out of the way
                    # Move to dataset/invalid/path
                    logging.error(f"Invalid parquet: {file}")
                    os.rename(file, f"{file}.invalid")
                except OSError as e:
                    logging.error(f"OSError on {file}", e)
                    os.rename(file, f"{file}.oserror_invalid")
            # Sometimes, all the files have been removed, skip the pathspec in those cases
            if len(glob(pathspec)) > 0:
                globs[event_type] = pathspec
            else:
                logging.info(f"Skipping empty path: {pathspec}")
    return globs


def loadSqlStatements(file):
    """
    Read sql script into map keyed by table name.
    """
    file = open(file, "r")
    lines = file.readlines()

    statements = {}
    count = 0
    # Strips the newline character
    curKey = ""
    curStatement = ""
    for count, line in enumerate(lines):
        if line.lower().startswith("create "):
            # For tables and views, use the object name
            curKey = line.strip().split()[-1]
            curStatement = line
        elif line.lower().startswith("update "):
            # Add line number to be sure its unique as there can be multiple UPDATEs per table
            curKey = f"{line.strip()}-{count}"
            curStatement = line
        else:
            if line.strip() == ";":
                # We done. Save the statement. Don't save the semi-colon.
                statements[curKey] = curStatement
                curStatement = "SKIP"
            else:
                if curStatement != "SKIP":
                    curStatement += line
    return statements


def generate_view_sql(event_map, start=None, end=None):
    """
    Create SQL for each of the event_types in the map.
    """
    # View Template
    stmts = []
    for event_type, pathspec in event_map.items():
        if "raw_" in event_type and '/raw_sensor/' in pathspec:
            # Raw files *may* have differing schemas, so enable union'ing of all schemas.
            # FIX in Wintap(?): Found that exact dups are in the raw tables, so remove them here using the GROUP BY ALL.
            # Only implement duplicate fix on 'raw_sensor' path. RAW tables in 'rolling' are already fixed.
            view_sql = f"""
            create or replace view {event_type} as
            select *, count(*) num_dups from parquet_scan('{pathspec}',hive_partitioning=1,union_by_name=true) group by all
            """
        else:
            view_sql = f"""
            create or replace view {event_type} as
            select * from parquet_scan('{pathspec}',hive_partitioning=1)
            """
        if start != None and end != None:
            stmt += f"where dayPK between {start} and {end}"
        if start != None and end == None:
            stmt += f"where dayPK = {start}"
        stmts.append(view_sql)
        logging.debug(f"View for {event_type} using {pathspec}")
        logging.debug(view_sql)
    return stmts


def create_views(con, event_map):
    stmts = generate_view_sql(event_map)
    for sql in stmts:
        cursor = con.cursor()
        try:
            cursor.execute(sql)
        except duckdb.duckdb.IOException as e:
            logging.error(f"SQL Failed: {sql}", e)
            logging.error("If the error is too many files open, try this on OSX:")
            logging.error("ulimit -Sn 524288; ulimit -Hn 10485760")
            raise e
        finally:
            cursor.close()


def create_raw_views(con, raw_data, start=None, end=None):
    """
    Create views in the db for each of the event_types.
    """
    create_views(con, raw_data)

    # For now, processing REQUIREs that RAW_PROCESS_STOP exist even if its empty. Create an empty table if needed.
    create_empty_process_stop(con)


def create_empty_process_stop(con):
    """
    The current processing expects there to always be a RAW_PROCESS_STOP table for the final step for PROCESS.
    This function is used to create an empty table with the right structure for cases where there were no PROCESS_STOP events reported.
    """
    db_objects = con.execute(
        "select table_name, table_type from information_schema.tables where table_schema='main' and lower(table_name)='raw_process_stop'"
    ).fetchall()
    if len(db_objects) == 0:
        logging.info("Creating empty RAW_PROCESS_STOP")
        cursor = con.cursor()
        cursor.execute(
            """
        CREATE TABLE raw_process_stop (
            PidHash VARCHAR,
            ParentPidHash VARCHAR,
            CPUCycleCount BIGINT,
            CPUUtilization INTEGER,
            CommitCharge BIGINT,
            CommitPeak BIGINT,
            ReadOperationCount BIGINT,
            WriteOperationCount BIGINT,
            ReadTransferKiloBytes BIGINT,
            WriteTransferKiloBytes BIGINT,
            HardFaultCount INTEGER,
            TokenElevationType INTEGER,
            ExitCode BIGINT,
            MessageType VARCHAR,
            Hostname VARCHAR,
            ActivityType VARCHAR,
            EventTime BIGINT,
            ReceiveTime BIGINT,
            PID INTEGER,
            IncrType VARCHAR,
            EventCount INTEGER,
            FirstSeenMs BIGINT,
            LastSeenMs BIGINT
        )
        """
        )
        cursor.close()


def run_sql_no_args(con, sqlfile):
    """
    Execute all SQL statements in the file without binding any parameters.
    Format should be SQL statements (DDL) delimited with semi-colons. Comments are allowed SQL
    style:
       -- for single line
       /*
       Multi-line
       */
    """
    etl_sql = loadSqlStatements(sqlfile)
    for key in etl_sql:
        logging.info(f"Processing: {key}")
        try:
            con.execute(etl_sql[key])
        except CatalogException as e:
            logging.info(f"No raw data for {key}")
        except Exception as e:
            logging.info(f"  Failed: {e}")
            logging.info(type(e))


def get_db_objects(con, exclude=None):
    """
    Get all tables/views defined in the db.
    exclude should be a list of strings. If the strings appear in the object names, they'll be dropped from the result.
    """
    db_objects = con.execute(
        "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
    ).fetchall()
    if exclude != None:
        # Find matches NOT including any of the words
        tables = [t for t, x in db_objects if not any(e in t for e in exclude)]
        logging.debug(f"Not Matches: {tables}")
    else:
        tables = [t for t, x in db_objects]
    return tables


def write_parquet(con, datasetpath, db_objects, daypk=None):
    """
    Write tables/views from duckdb instance to parquet.
    If daypk is provided, write to corresponding path in rolling.
    Otherwise, write to stdview.
    """
    for object_name in db_objects:
        logging.info(f"Writing {object_name}")
        try:
            if daypk == None:
                pathspec = f"{datasetpath}{os.sep}stdview"
                filename = f"{object_name}.parquet"
            else:
                pathspec = f"{datasetpath}{os.sep}rolling{os.sep}{object_name}{os.sep}dayPK={daypk}"
                filename = f"{object_name}-{daypk}.parquet"
            if not os.path.exists(pathspec):
                os.makedirs(pathspec)
                logging.debug(f"created folder: {pathspec} ")
            else:
                logging.debug(f"folder already exists: {pathspec}")
            # TODO Add test for file existence
            sql = f"COPY {object_name} TO '{pathspec}{os.sep}{filename}' (FORMAT 'parquet')"
            con.execute(sql)
        except duckdb.IOException as e:
            logging.exception(f"Failed to write: {object_name}")
