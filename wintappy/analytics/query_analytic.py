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
