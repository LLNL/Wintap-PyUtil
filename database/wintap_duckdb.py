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


class WintapDuckDB(WintapDatabase):
    _connection: DuckDBPyConnection
    _dataset_path: str

    def __init__(self, options: WintapDuckDBOptions):
        ## TODO: in the future, we could move the db connection setup here too
        self._connection = options.connection
        self._dataset_path = options.dataset_path

    def get_tables(self) -> list:
        """
        Get all tables/views defined in the db.
        exclude should be a list of strings. If the strings appear in the object names, they'll be dropped from the result.
        """
        db_objects = self._connection.execute(
            "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
        ).fetchall()

        return [t for t, _ in db_objects]

    def query(self, query_string: str) -> list:
        """
        Given a string representing a DuckDB query, execute it
        against the configured db connection
        """
        return self._connection.execute(query_string).fetchall()

    def write_table(
        self,
        table: str,
        partition_key: Optional[str] = None,
    ) -> None:
        """
        Write tables/views from duckdb instance to parquet.
        If partition_key is provided, write to corresponding path in rolling.
        Otherwise, write to stdview.
        """
        logging.info(f"Writing {table}")
        try:
            if partition_key == None:
                pathspec = f"{self._dataset_path}/stdview"
                filename = f"{table}.parquet"
            else:
                pathspec = f"{self._dataset_path}/rolling/{table}/dayPK={partition_key}"
                filename = f"{table}-{partition_key}.parquet"
            if not os.path.exists(pathspec):
                os.makedirs(pathspec)
                logging.debug("folder '{}' created ".format(pathspec))
            else:
                logging.debug("folder {} already exists".format(pathspec))
            # TODO Add test for file existence
            sql = f"COPY {table} TO '{pathspec}/{filename}' (FORMAT 'parquet')"
            self._connection.execute(sql)
        except duckdb.IOException as e:
            logging.exception(f"Failed to write: {table}")
        return
