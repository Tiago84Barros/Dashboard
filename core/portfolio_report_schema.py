
from typing import TypedDict, List, Dict, Any

class PortfolioReport(TypedDict):
    analysis_mode: str
    analytical_basis: str
    executive_summary: str
    portfolio_identity: str
    key_strengths: List[str]
    key_weaknesses: List[str]
    macro_reading: str
    hidden_risks: List[str]
    asset_roles: List[Dict[str, Any]]
    misalignments: List[str]
    action_plan: List[str]
    final_insight: str
