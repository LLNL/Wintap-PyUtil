from datetime import datetime, timedelta
import logging

def daterange(start_date: datetime, end_date: datetime):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_date_range(start_date:str, end_date:str, date_format: str = "%Y%m%d") -> (datetime, datetime):
    end = datetime.utcnow()
    start = datetime(end.year, end.month, end.day) - timedelta(days=1)
    if end_date:
        end = datetime.strptime(end_date, date_format)
    if start_date:
        start = datetime.strptime(start_date, date_format)
    logging.debug(f"Using date range: {start} -> {end}")
    return start, end


def configure_basic_logging() -> None:
    logging.basicConfig()
    logger = logging.getLogger()
    logger.handlers[0].setFormatter(
        logging.Formatter("%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
    )
