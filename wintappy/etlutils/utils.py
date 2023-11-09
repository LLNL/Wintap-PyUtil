import heapq
import logging
import os
from datetime import datetime, timedelta

DEFAULT_DATE_RANGE_PATH = f"raw_sensor{os.sep}raw_process{os.sep}"


def configure_basic_logging() -> None:
    logging.basicConfig()
    logger = logging.getLogger()
    logger.handlers[0].setFormatter(
        logging.Formatter("%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
    )


def daterange(start_date: datetime, end_date: datetime):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_date_range(
    start_date: str,
    end_date: str,
    date_format: str = "%Y%m%d",
    data_set_path: str = os.getcwd(),
) -> (datetime, datetime):
    start = None
    end = None
    if end_date:
        end = datetime.strptime(end_date, date_format)
    if start_date:
        start = datetime.strptime(start_date, date_format)
    if start and end:
        return start, end
    start = latest_processed_datetime(data_set_path)
    end = datetime.utcnow()
    return start, end


def latest_processed_datetime(data_set_path) -> datetime:
    path = f"{data_set_path}{os.sep}{DEFAULT_DATE_RANGE_PATH}"
    try:
        daypks = os.listdir(path)
    except FileNotFoundError:
        # directory does not exist
        logging.info(f"Directory ({path}) does not exist. Will use default times.")
        daypks = []
    # remove "bad" directories
    daypks = [d for d in daypks if "=" in d]
    # if there is no data, return a default of a day ago
    if len(daypks) == 0:
        end = datetime.utcnow()
        return datetime(end.year, end.month, end.day) - timedelta(days=1)
    _, day = heapq.nlargest(1, daypks, key=pk_sort)[0].split("=")
    # default hour
    hour = "00"
    hourpks = os.listdir(f"{path}{os.sep}dayPK={day}")
    # remove "bad" directories
    hourpks = [h for h in hourpks if "=" in h]
    if len(hourpks) > 0:
        _, hour = heapq.nlargest(1, hourpks, key=pk_sort)[0].split("=")
    return datetime.strptime(f"{day}{hour}", "%Y%m%d%H")


def pk_sort(pk):
    if "=" in pk:
        _, value = pk.split("=")
        return value
    return pk
