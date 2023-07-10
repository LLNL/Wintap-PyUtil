from unittest import mock

from ..wintap_duckdb import WintapDuckDB, WintapDuckDBOptions


class TestWinTapDuckDB:
    dataset_path = "test"

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_get_tables(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        wintap_db.get_tables()
        connection.execute.assert_called_once_with(
            "select table_name, table_type from information_schema.tables where table_schema='main' order by all"
        )

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_query(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        query = "select 1"
        wintap_db.query(query)
        connection.execute.assert_called_once_with(query)

    @mock.patch("duckdb.DuckDBPyConnection")
    def test_write(self, connection: mock.MagicMock) -> None:
        wintap_db = WintapDuckDB(WintapDuckDBOptions(connection, self.dataset_path))
        table_name, day_pk = "my-test-table", "202306"
        wintap_db.write_table(table_name, day_pk)
        expected_pathspec = "test/rolling/my-test-table/dayPK=202306"
        expected_filename = "my-test-table-202306.parquet"
        sql = f"COPY {table_name} TO '{expected_pathspec}/{expected_filename}' (FORMAT 'parquet')"
        connection.execute.assert_called_once_with(sql)
