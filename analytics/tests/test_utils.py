import os
from typing import Any, Dict
from unittest import mock

from ..query_analytic import MITRE_CAR_TYPE, MITREAttackCoverage, QueryAnalytic
from ..utils import (
    convert_id_to_filename,
    format_car_analytic,
    get_car_analytics,
    get_car_query,
)


class TestSqlUtils:
    ## reduce duplication of some test data
    test_id: str = "my-test-id"
    test_query_str: str = "-- my super awesome analytic\nSELECT (1)\n"
    test_metadata: Dict[str, Any] = {
        "my-test-id": {
            "id": "my-test-id",
            "coverage": [
                {
                    "technique": "technique1",
                    "tactics": ["my-ta-1", "my-ta-2"],
                    "coverage": "Moderate",
                }
            ],
        }
    }

    def test_convert_analytic_to_filename(self) -> None:
        analytic_name = "CAR_1234_56_79"
        assert "CAR-1234-56-79.sql" == convert_id_to_filename(analytic_name, "sql")
        analytic_name = "CaR_12345"
        assert "CAR-12345.sql" == convert_id_to_filename(analytic_name, "sql")
        analytic_name = "car_12345"
        assert "CAR-12345.sql" == convert_id_to_filename(analytic_name, "sql")

    @mock.patch("os.path.join")
    def test_load_sql_query(self, mock_join: mock.MagicMock) -> None:
        cwd = os.path.dirname(__file__)
        mock_join.return_value = f"{cwd}/fixtures/{self.test_id}.sql"
        assert self.test_query_str == get_car_query(self.test_id)

    @mock.patch("os.path.join")
    @mock.patch("analytics.utils.load_car_analtyic_metadata")
    def test_get_car_analytics(self, mock_load: mock.MagicMock, mock_join: mock.MagicMock) -> None:
        mock_load.return_value = self.test_metadata
        cwd = os.path.dirname(__file__)
        mock_join.return_value = f"{cwd}/fixtures"
        output = get_car_analytics()
        expected_output = {
            self.test_id: QueryAnalytic(
                analytic_id=self.test_id,
                query_string=self.test_query_str,
                query_type=MITRE_CAR_TYPE,
                coverage=[
                    MITREAttackCoverage(
                        coverage="Moderate",
                        tactics=["my-ta-1", "my-ta-2"],
                        technique="technique1",
                        subtechniques=None,
                    )
                ],
            )
        }
        assert expected_output == output

    def test_format_car_analytic_normal(self) -> None:
        expected_output = QueryAnalytic(
            analytic_id=self.test_id,
            query_string=self.test_query_str,
            query_type=MITRE_CAR_TYPE,
            coverage=[
                MITREAttackCoverage(
                    coverage="Moderate",
                    tactics=["my-ta-1", "my-ta-2"],
                    technique="technique1",
                    subtechniques=None,
                )
            ],
        )
        assert expected_output == format_car_analytic(
            self.test_id, self.test_metadata, self.test_query_str
        )

    def test_format_car_analytic_no_coverage(self) -> None:
        my_metadata: Dict[str, Any] = {"my-test-id": {"id": "my-test-id"}}
        expected_output = QueryAnalytic(
            analytic_id=self.test_id,
            query_string=self.test_query_str,
            query_type=MITRE_CAR_TYPE,
            coverage=[],
        )
        assert expected_output == format_car_analytic(
            self.test_id, my_metadata, self.test_query_str
        )

    def test_format_car_analytic_no_metadata(self) -> None:
        assert None == format_car_analytic(self.test_id, {}, self.test_query_str)
