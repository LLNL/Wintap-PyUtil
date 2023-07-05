from abc import ABC, abstractmethod
from typing import Any, Optional


class WintapDatabase(ABC):
    @abstractmethod
    def query(self, query_string: str) -> Any:
        pass

    @abstractmethod
    def write(self, partition_key: Optional[str]) -> None:
        pass
