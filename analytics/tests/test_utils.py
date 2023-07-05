from ..utils import convert_analytic_to_filename, get_car_query


class TestSqlUtils:
    def test_convert_analytic_to_filename(self):
        analytic_name = "CAR-1234-56-79"
        assert "car_1234_56_79.sql" == convert_analytic_to_filename(analytic_name)
        analytic_name = "CaR-12345"
        assert "car_12345.sql" == convert_analytic_to_filename(analytic_name)
        analytic_name = "car_12345"
        assert "car_12345.sql" == convert_analytic_to_filename(analytic_name)

    def test_load_sql_query(self):
        expected = "-- Processes Spawning cmd.exe\nSELECT pid_hash\nFROM process\nWHERE process_name = 'cmd.exe'\n"
        assert expected == get_car_query("car-2013-02-003")
