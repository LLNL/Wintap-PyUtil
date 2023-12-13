from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from pandas import DataFrame

MITRE_CAR_TYPE = "MITRE_CAR"


@dataclass
class MitreAttackCoverage:
    coverage: str
    tactics: List[str]
    technique: str
    subtechniques: Optional[List[str]] = None


@dataclass
class QueryAnalytic(ABC):
    analytic_id: str
    analytic_template: str
    query_type: str
    metadata: Dict[str, Any]

    def get_tactics(self) -> List[str]:
        """Helper function to quickly return all tactics in coverage"""
        tactics = []
        for c in self.coverage:
            tactics.extend(c.tactics)
        return tactics

    def get_techniques(self) -> List[str]:
        """Helper function to quickly return all techniques in coverage"""
        techniques = []
        for c in self.coverage:
            techniques.append(c.technique)
        return techniques

    @abstractmethod
    def table_item(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def coverage_table_items(self) -> List[Dict[str, Any]]:
        pass


@dataclass
class CARAnalytic(QueryAnalytic):
    coverage: List[MitreAttackCoverage]

    def table_item(self) -> Dict[str, Any]:
        data = self.metadata
        data.pop("implementations")
        data.pop("unit_tests")
        return data

    def coverage_table_items(self) -> List[Dict[str, Any]]:
        data = []
        for entry in self.coverage:
            single_entry = asdict(entry)
            single_entry["id"] = self.analytic_id
            single_entry["uid"] = f"{self.analytic_id}-{entry.technique}"
            single_entry["subtechniques"] = (
                list(map(lambda x: x.split(".")[-1], entry.subtechniques))
                if entry.subtechniques
                else []
            )
            data.append(single_entry)
        return data


@dataclass
class SigmaAnalytic(QueryAnalytic):
    pass
