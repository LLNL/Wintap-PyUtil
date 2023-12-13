import os
from typing import Any, Dict
from unittest import mock

from wintappy.analytics.query_analytic import (
    MITRE_CAR_TYPE,
    MitreAttackCoverage,
    CARAnalytic,
)
from wintappy.analytics.utils import (
    convert_id_to_filename,
    format_car_analytic,
    load_all,
    load_single,
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

    def setup_class(self):
        self.test_query_analytic = CARAnalytic(
            analytic_id=self.test_id,
            analytic_template=f"{self.test_id}.sql",
            query_type=MITRE_CAR_TYPE,
            coverage=[
                MitreAttackCoverage(
                    coverage="Moderate",
                    tactics=["my-ta-1", "my-ta-2"],
                    technique="technique1",
                    subtechniques=None,
                )
            ],
            metadata=self.test_metadata[self.test_id],
        )

    def test_convert_analytic_to_filename(self) -> None:
        analytic_name = "CAR_1234_56_79"
        assert "CAR-1234-56-79.sql" == convert_id_to_filename(analytic_name, "sql")
        analytic_name = "CaR_12345"
        assert "CAR-12345.sql" == convert_id_to_filename(analytic_name, "sql")
        analytic_name = "car_12345"
        assert "CAR-12345.sql" == convert_id_to_filename(analytic_name, "sql")

    def test_load_sql_query(self) -> None:
        expected = CARAnalytic(
            analytic_id="my-test-id",
            analytic_template="my-test-id.sql",
            query_type=MITRE_CAR_TYPE,
            metadata={},
            coverage=[],
        )
        assert expected == load_single(self.test_id)

    @mock.patch("jinja2.Environment")
    @mock.patch("wintappy.analytics.utils.load_car_analtyic_metadata")
    def test_get_car_analytics(
        self, mock_load: mock.MagicMock, mock_env: mock.MagicMock
    ) -> None:
        mock_load.return_value = self.test_metadata
        mock_env.list_templates.return_value = [f"{self.test_id}.sql"]
        output = load_all(mock_env)
        expected_output = {self.test_id: self.test_query_analytic}
        assert expected_output == output

    def test_format_car_analytic_normal(self) -> None:
        expected_output = self.test_query_analytic
        assert expected_output == format_car_analytic(self.test_id, self.test_metadata)

    def test_format_car_analytic_no_coverage(self) -> None:
        my_metadata: Dict[str, Any] = {"my-test-id": {"id": "my-test-id"}}
        expected_output = CARAnalytic(
            analytic_id="my-test-id",
            analytic_template="my-test-id.sql",
            query_type="MITRE_CAR",
            metadata={"id": "my-test-id"},
            coverage=[],
        )
        assert expected_output == format_car_analytic(self.test_id, my_metadata)
