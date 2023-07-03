from ..utils.sql_utils import convert_analytic_to_filename

class TestSqlUtils:

    def test_convert_analytic_to_filename(self):
        analytic_name = "CAR-1234-56-79"
        assert "car_1234_56_79" == convert_analytic_to_filename(analytic_name)
        analytic_name = "CaR-12345"
        assert "car_12345" == convert_analytic_to_filename(analytic_name)
        analytic_name = "car_12345"
        assert "car_12345" == convert_analytic_to_filename(analytic_name)

