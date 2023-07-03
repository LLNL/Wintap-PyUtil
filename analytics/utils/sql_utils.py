from typing import Optional


def run_query(analytic_id: Optional[str]):
    pass


def store_results(analytic_id: Optional[str]):
    pass


def print_results(analytic_id: Optional[str]):
    pass


def convert_analytic_to_filename(raw_id: str) -> str:
    return raw_id.replace("-", "_").lower()
