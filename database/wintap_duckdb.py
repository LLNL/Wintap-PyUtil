import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import duckdb
from duckdb import DuckDBPyConnection

from .wintap_database import WintapDatabase

@dataclass
class WintapDuckDBOptions:
    connection: DuckDBPyConnection
    dataset_path: str
    exclude_tables: Optional[list] = None

class WintapDuckDB(WintapDatabase):
    _connection: DuckDBPyConnection
    _dataset_path: str

    def __init__(self, options: WintapDuckDBOptions):
        ## TODO: in the future, we could move the db connection setup here too
        self._connection = options.connection
        self._dataset_path = options.dataset_path
        self._tables = self._get_tables(options.exclude_tables)

    def _get_tables(self, exclude=Optional[list]) -> list:
        """
        Get all tables/views defined in the db.
        exclude should be a list of strings. If the strings appear in the object names, they'll be dropped from the result.
        """
        db_objects = self._connection.execute(
            "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
        ).fetchall()
        if exclude!=None:
            # Find matches NOT including any of the words
            tables=[t for t,x in db_objects if not any(e in t for e in exclude )]
            logging.debug(f'Not Matches: {tables}')
        else:
            tables=[t for t,x in db_objects]
        self._tables = tables  

    def query(self, query_string: str) -> list:
        """
        Given a string representing a DuckDB query, execute it
        against the configured db connection
        """
        return self._connection.execute(query_string).fetchall()

    def write(self, partition_key=None) -> None:
        """
        Write tables/views from duckdb instance to parquet.
        If partition_key is provided, write to corresponding path in rolling.
        Otherwise, write to stdview.
        """
        for object_name in self._tables:
            logging.info(f"Writing {object_name}")
            try:
                if partition_key == None:
                    pathspec = f"{self._dataset_path}/stdview"
                    filename = f"{object_name}.parquet"
                else:
                    pathspec = f"{self._dataset_path}/rolling/{object_name}/dayPK={partition_key}"
                    filename = f"{object_name}-{partition_key}.parquet"
                if not os.path.exists(pathspec):
                    os.makedirs(pathspec)
                    logging.debug("folder '{}' created ".format(pathspec))
                else:
                    logging.debug("folder {} already exists".format(pathspec))
                # TODO Add test for file existence
                sql = f"COPY {object_name} TO '{pathspec}/{filename}' (FORMAT 'parquet')"
                self._connection.execute(sql)
            except duckdb.IOException as e:
                logging.exception(f"Failed to write: {object_name}")
        return
