import os

from importlib_resources import files


def convert_analytic_to_filename(raw_id: str) -> str:
    return f'{raw_id.replace("-", "_").lower()}.sql'


def get_car_query(raw_id: str) -> str:
    cwd = os.path.dirname(__file__)
    sql_file = os.path.join(cwd, "mitre_car", convert_analytic_to_filename(raw_id))
    data = ""
    with open(sql_file, "r") as file:
        data = file.read()
    return data
