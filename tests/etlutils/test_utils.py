from typing import Any, Dict
from unittest import mock
from datetime import datetime as dt

from wintappy.etlutils.utils import (
    get_date_range,
    daterange
)


class TestUtils:

    @mock.patch('wintappy.etlutils.utils.datetime')
    def test_get_date_range_default(self, mock_datetime: mock.MagicMock) -> None:
        mock_datetime.utcnow.return_value = dt(2021, 12, 5, 11, 24)
        mock_datetime.return_value = dt(2021, 12, 5)
        start, end = get_date_range("", "")
        assert start == dt(2021, 12, 4, 0, 0)
        assert end == dt(2021, 12, 5, 11, 24)
        start, end = get_date_range(None, None)
        assert start == dt(2021, 12, 4, 0, 0)
        assert end == dt(2021, 12, 5, 11, 24)

    def test_get_date_range_specified(self) -> None:
        start, end = get_date_range("20231101", "20231108")
        assert start == dt(2023, 11, 1, 0, 0)
        assert end == dt(2023, 11, 8, 0, 0)

    def test_get_date_range_specified_datefmt(self) -> None:
        start, end = get_date_range("20231101 05 05", "20231108 10 56", "%Y%m%d %H %M")
        assert start == dt(2023, 11, 1, 5, 5)
        assert end == dt(2023, 11, 8, 10, 56)
