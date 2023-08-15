import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import duckdb
from duckdb import DuckDBPyConnection
from jinja2 import Environment, FileSystemLoader
from pandas import DataFrame

from .constants import (
    ANALYTICS_RESULTS_TABLE,
    CREATE_ANALYTICS_TEMPLATE,
    INSERT_ANALYTICS_TEMPLATE,
    PID_HASH,
    TEMPLATE_DIR,
)


@dataclass
class WintapDuckDBOptions:
    connection: DuckDBPyConnection
    dataset_path: str


class WintapDuckDB:
    _connection: DuckDBPyConnection
    _dataset_path: str

    def __init__(self, options: WintapDuckDBOptions):
        ## TODO: in the future, we could move the db connection setup here too
        self._connection = options.connection
        self._dataset_path = options.dataset_path
        cwd = os.path.dirname(__file__)
        self._jinja_environment = Environment(
            loader=FileSystemLoader(os.path.join(cwd, TEMPLATE_DIR))
        )
        self._setup_tables()

    def _setup_tables(self) -> None:
        """Create extra tables that store analytics results"""
        self.query(
            self._jinja_environment.get_template(CREATE_ANALYTICS_TEMPLATE).render()
        )

    def get_tables(self) -> list:
        """
        Get all tables/views defined in the db.
        exclude should be a list of strings. If the strings appear in the object names, they'll be dropped from the result.
        """
        db_objects = self._connection.execute(
            "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
        ).fetchall()

        return [t for t, _ in db_objects]

    def query(self, query_string: str) -> DataFrame:
        """
        Given a string representing a DuckDB query, execute it
        against the configured db connection
        """
        return self._connection.execute(query_string).df()

    def write_table(
        self,
        table: str,
        partition_key: Optional[int] = None,
        location: Optional[str] = None,
    ) -> None:
        """
        Write tables/views from duckdb instance to parquet.
        If partition_key is provided, write to corresponding path in rolling.
        Otherwise, write to stdview.
        """
        logging.info(f"Writing {table}")
        path = self._dataset_path
        if location:
            logging.debug(
                f"Writing to path {location} rather than the dataset path {path}"
            )
            path = location
        try:
            if partition_key == None:
                pathspec = f"{path}/stdview"
                filename = f"{table}.parquet"
            else:
                pathspec = f"{path}/rolling/{table}/dayPK={partition_key}"
                filename = f"{table}-{partition_key}.parquet"
            if not os.path.exists(pathspec):
                os.makedirs(pathspec)
                logging.debug("folder '{}' created ".format(pathspec))
            else:
                logging.debug("folder {} already exists".format(pathspec))
            # TODO Add test for file existence
            sql = f"COPY {table} TO '{pathspec}/{filename}' (FORMAT 'parquet')"
            logging.debug(f"generated copy sql: {sql}")
            self._connection.execute(sql)
        except duckdb.IOException as e:
            logging.exception(f"Failed to write: {table}")
        return

    def insert_analytics_table(
        self,
        analytic_id: str,
        entity_id: str,
        entity_type: str = PID_HASH,
        event_time: datetime = datetime.now(),
    ) -> None:
        sql = self._jinja_environment.get_template(INSERT_ANALYTICS_TEMPLATE).render(
            # for now, we will simply support pid_hash as entity ids
            entity=entity_id,
            analytic_id=analytic_id,
            time=int(event_time.strftime("%s")),
            # for now, we will simply support pid_hash as entity types
            entity_type=entity_type,
        )
        logging.debug(f"generated insert analtyic: {sql}")
        self._connection.execute(sql)

    def clear_table(self, table: str) -> None:
        """clear contents of a table in the connection. Mainly used after writing out table to file."""
        logging.info(f"Clearing {table}")
        try:
            sql = f"DELETE FROM {table}"
            logging.debug(f"generated delete sql: {sql}")
            self._connection.execute(sql)
        except duckdb.IOException as e:
            logging.exception(f"Failed to clear: {table}")
        return
