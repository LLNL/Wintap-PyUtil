from typing import Any, Dict
from unittest import mock

from wintappy.analytics.query_analytic import (
    CARAnalytic,
    SigmaAnalytic,
    MITRE_CAR_TYPE,
    SIMGA_TYPE,
)


class TestCarAnalytic:

    def test_table_item(self) -> None:
        analytic = CARAnalytic(
            analytic_id='test-id',
            analytic_template='',
            coverage=[],
            metadata={
                "implementations": {"some":"really","intense": ["definitions of the detection"]},
                "unit_tests": ["one", "two","it should not matter"],
                "id": "CAR_ID_ONE_ONE_ONE",
                "description": "hello, world"
            }
        )
        expected_output = {
            "id": "CAR_ID_ONE_ONE_ONE",
            "description": "hello, world"
        }
        assert analytic.table_item() == expected_output
        assert analytic.query_type == MITRE_CAR_TYPE

    def test_table_item_missing_pop_entry(self) -> None:
        analytic = CARAnalytic(
            analytic_id='test-id',
            analytic_template='',
            coverage=[],
            metadata={
                "my_other_field": "this is missing the implementations field",
                "unit_tests": ["one", "two","it should not matter"],
                "id": "CAR_ID_ONE_ONE_ONE",
                "description": "hello, world"
            }
        )
        expected_output = {
            "my_other_field": "this is missing the implementations field",
            "id": "CAR_ID_ONE_ONE_ONE",
            "description": "hello, world"
        }
        assert analytic.table_item() == expected_output

class TestSigmaAnalytic:

    def test_table_item(self) -> None:
        analytic = SigmaAnalytic(
            analytic_id='test-id',
            analytic_template='',
            coverage=[],
            metadata={
                "this":"should fail when there is an implementation for SigmaAnalyticTableItem"
            }
        )
        assert analytic.table_item() == {}
        assert analytic.query_type == SIMGA_TYPE
