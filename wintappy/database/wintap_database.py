from abc import ABC, abstractmethod
from typing import Any, Optional

from pandas import DataFrame


class WintapDatabase(ABC):
    @abstractmethod
    def query(self, query_string: str) -> DataFrame:
        pass

    @abstractmethod
    def write_table(self, table: str, partition_key: Optional[str]) -> None:
        pass
