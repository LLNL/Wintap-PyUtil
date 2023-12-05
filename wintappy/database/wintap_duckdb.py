import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import duckdb
from duckdb import DuckDBPyConnection
from jinja2 import Environment, FileSystemLoader
from pandas import DataFrame

from .constants import (
    ANALYTICS_RESULTS_TABLE,
    ANALYTICS_TABLE,
    CREATE_ANALYTICS_RESULTS_TEMPLATE,
    CREATE_ANALYTICS_TEMPLATE,
    INSERT_ANALYTICS_RESULTS_TEMPLATE,
    INSERT_ANALYTICS_TEMPLATE,
    PID_HASH,
    TEMPLATE_DIR,
)


@dataclass
class WintapDuckDBOptions:
    connection: DuckDBPyConnection
    dataset_path: str
    load_analytics: bool = True


class WintapDuckDB:
    _connection: DuckDBPyConnection
    _dataset_path: str
    _load_analytics: bool

    def __init__(self, options: WintapDuckDBOptions):
        ## TODO: in the future, we could move the db connection setup here too
        self._connection = options.connection
        self._dataset_path = options.dataset_path
        self._load_analytics = options.load_analytics
        cwd = os.path.dirname(__file__)
        self._jinja_environment = Environment(
            loader=FileSystemLoader(os.path.join(cwd, TEMPLATE_DIR))
        )
        self._setup_tables()

    def _setup_tables(self) -> None:
        """Create extra tables that store analytics results"""
        if self._is_table_or_view(ANALYTICS_RESULTS_TABLE):
            if self._load_analytics:
                return
        # Because we are generating analytics, we should drop any existing views
        # of our data, else we will run into errors
        self.query(f"DROP VIEW IF EXISTS {ANALYTICS_RESULTS_TABLE}")
        self.query(
            self._jinja_environment.get_template(
                CREATE_ANALYTICS_RESULTS_TEMPLATE
            ).render()
        )
        # shim in for sigma 
        self.query(f"DROP VIEW IF EXISTS sigma_labels")
        self.query(
            self._jinja_environment.get_template(
                "create_sigma_results.sql"
            ).render()
        )
        # Create table for analytics metadata
        self.query(f"DROP VIEW IF EXISTS {ANALYTICS_TABLE}")
        self.query(
            self._jinja_environment.get_template(CREATE_ANALYTICS_TEMPLATE).render()
        )

    def _is_table_or_view(self, table_name: str):
        try:
            self.query(f"describe {table_name}")
            logging.debug(f"table or view ({table_name}) already exists")
        except duckdb.CatalogException as err:
            logging.debug(f"table or view ({table_name}) does not exist")
            return False
        return True

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

    def register_filesystem(self, fs: str) -> None:
        return self._connection.register_filesystem(filesystem=fs)

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

        logging.debug(f"Writing {table}")
        path = self._dataset_path
        if location:
            logging.debug(
                f"Writing to path {location} rather than the dataset path {path}"
            )
            path = location
        try:
            if partition_key == None:
                pathspec = f"{path}"
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

    def insert_analytics_results_table(
        self,
        analytic_id: str,
        entity_id: str,
        entity_type: str = PID_HASH,
        event_time: datetime = datetime.now(),
    ) -> None:
        sql = self._jinja_environment.get_template(
            INSERT_ANALYTICS_RESULTS_TEMPLATE
        ).render(
            # for now, we will simply support pid_hash as entity ids
            entity=entity_id,
            analytic_id=analytic_id,
            time=int(event_time.strftime("%s")),
            # for now, we will simply support pid_hash as entity types
            entity_type=entity_type,
        )
        logging.debug(f"generated insert analtyic: {sql}")
        self._connection.execute(sql)

    def insert_analytics_table(
        self,
        analytic_id: str,
        description: str,
        technique_id: str,
        technique_stix_type: str,
        tactic_id: str,
        tactic_stix_type: str,
    ) -> None:
        sql = self._jinja_environment.get_template(INSERT_ANALYTICS_TEMPLATE).render(
            analytic_id=analytic_id,
            description=description,
            technique_id=technique_id,
            technique_stix_type=technique_stix_type,
            tactic_id=tactic_id,
            tactic_stix_type=tactic_stix_type,
        )
        logging.debug(f"generated insert analtyic: {sql}")
        self._connection.execute(sql)

    def clear_table(self, table: str) -> None:
        """clear contents of a table in the connection. Mainly used after writing out table to file."""
        logging.debug(f"Clearing {table}")
        try:
            sql = f"DELETE FROM {table}"
            logging.debug(f"generated delete sql: {sql}")
            self._connection.execute(sql)
        except duckdb.IOException as e:
            logging.exception(f"Failed to clear: {table}")
        return
