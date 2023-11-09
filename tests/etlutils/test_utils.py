from typing import Any, Dict
from unittest import mock
from datetime import datetime as dt

from wintappy.etlutils.utils import (
    get_date_range,
    daterange,
    latest_processed_datetime,
    pk_sort,
)


class TestUtils:

    @mock.patch("os.listdir")
    @mock.patch('wintappy.etlutils.utils.datetime')
    def test_get_date_range_default(self, mock_datetime: mock.MagicMock, mock_listdir: mock.MagicMock) -> None:
        mock_datetime.utcnow.return_value = dt(2021, 12, 5, 11, 24)
        mock_datetime.return_value = dt(2021, 12, 5)
        mock_listdir.return_value = []
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

    def test_pk_sort(self) -> None:
        assert pk_sort("dayPK=20230814") == "20230814"
        assert pk_sort("hourPK=05") == "05"

    def test_pk_sort_random_data(self) -> None:
        assert pk_sort("I-do-not-have-a-equals") == "I-do-not-have-a-equals"
        assert pk_sort("heyo") == "heyo"

    @mock.patch("os.listdir")
    @mock.patch('wintappy.etlutils.utils.datetime')
    def test_latest_processed_datetime_no_data(self, mock_datetime: mock.MagicMock, mock_listdir: mock.MagicMock) -> None:
        mock_datetime.utcnow.return_value = dt(2021, 12, 5, 11, 24)
        mock_datetime.return_value = dt(2021, 12, 5)
        mock_listdir.return_value = []
        assert latest_processed_datetime("blah") == dt(2021, 12, 4, 0, 0)

    @mock.patch("os.listdir")
    def test_latest_processed_datetime_data(self, mock_listdir: mock.MagicMock) -> None:
        mock_listdir.side_effect = [["dayPK=20231101", "dayPK=20231105"], ["hourPK=06", "hourPK=12"]]
        assert latest_processed_datetime("blah") == dt(2023, 11, 5, 12, 0)
        mock_listdir.side_effect = [["dayPK=20231101", "dayPK=20231105"], []]
        assert latest_processed_datetime("blah") == dt(2023, 11, 5, 0, 0)

    @mock.patch("os.listdir")
    @mock.patch('wintappy.etlutils.utils.datetime')
    def test_latest_processed_datetime_random_data_files(self, mock_datetime: mock.MagicMock, mock_listdir: mock.MagicMock) -> None:
        mock_datetime.utcnow.return_value = dt(2021, 12, 5, 11, 24)
        mock_datetime.return_value = dt(2021, 12, 5)
        mock_listdir.side_effect = [["wat-is-this-file", "another-random-file"], ["heyo", "goodbye"]]
        assert latest_processed_datetime("blah") == dt(2021, 12, 4, 0, 0)
        mock_listdir.side_effect = [["wat-is-this-file", "another-random-file"], []]
        assert latest_processed_datetime("blah") == dt(2021, 12, 4, 0, 0)
