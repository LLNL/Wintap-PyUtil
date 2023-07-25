from dataclasses import dataclass
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
class QueryAnalytic:
    analytic_id: str
    analytic_template: str
    query_type: str
    metadata: Dict[str, Any]
    coverage: List[MitreAttackCoverage]

    def get_tactics(self) -> List[str]:
        '''Helper function to quickly return all tactics in coverage'''
        tactics = []
        for c in self.coverage:
            tactics.extend(c.tactics)
        return tactics

    def get_techniques(self) -> List[str]:
        '''Helper function to quickly return all techniques in coverage'''
        techniques = []
        for c in self.coverage:
            techniques.append(c.technique)
        return techniques
