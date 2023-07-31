from datetime import datetime, timezone
from unittest import mock

from wintappy.database.wintap_duckdb import WintapDuckDB, WintapDuckDBOptions


class TestWinTapDuckDB:
    dataset_path = "test"

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_get_tables(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        wintap_db.get_tables()
        connection.execute.assert_called_with(
            "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
        )

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_query(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        query = "select 1"
        wintap_db.query(query)
        connection.execute.assert_called_with(query)

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_write(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        table_name, day_pk = "my-test-table", "202306"
        wintap_db.write_table(table_name, day_pk)
        expected_pathspec = "test/rolling/my-test-table/dayPK=202306"
        expected_filename = "my-test-table-202306.parquet"
        sql = f"COPY {table_name} TO '{expected_pathspec}/{expected_filename}' (FORMAT 'parquet')"
        connection.execute.assert_called_with(sql)

    @mock.patch("datetime.datetime")
    @mock.patch("duckdb.DuckDBPyConnection")
    def test_insert_analytics(
        self, connection: mock.MagicMock, mock_datetime: mock.MagicMock
    ) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        mock_time = "1689378948"
        mock_datetime.strftime.return_value = mock_time
        mock_analytic_id = "my-cool-analytic"
        mock_entity_type = "not_the-pid-hash"
        expected_sql = f"INSERT INTO analytics_results(\n    entity,\n    analytic_id,\n    time,\n    entity_type\n)\nVALUES (\n    '{mock_entity_type}',\n    '{mock_analytic_id}',\n    to_timestamp({int(mock_time)}),\n    'pid_hash'\n)"
        wintap_db.insert_analytics_table(
            mock_analytic_id,
            mock_entity_type,
            event_time=mock_datetime,
        )
        connection.execute.assert_called_with(expected_sql)
