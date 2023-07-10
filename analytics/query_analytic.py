from dataclasses import dataclass
from typing import List, Optional

from pandas import DataFrame

MITRE_CAR_TYPE = "MITRE_CAR"


@dataclass
class MITREAttackCoverage:
    coverage: str
    tactics: List[str]
    technique: str
    subtechniques: Optional[List[str]] = None


@dataclass
class QueryAnalytic:
    analytic_id: str
    query_string: str
    query_type: str
    coverage: List[MITREAttackCoverage]
    """_results: Optional[DataFrame] = None

    @property
    def results(self) -> Optional[DataFrame]:
        return self._results

    @results.setter
    def results(self, results: DataFrame) -> None:
        self._results = results
        return 8"""
