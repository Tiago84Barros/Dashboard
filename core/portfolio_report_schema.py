from typing import Any, Dict, List, TypedDict


class AssetRoleItem(TypedDict, total=False):
    ticker: str
    role: str
    rationale: str


class PortfolioReport(TypedDict, total=False):
    analysis_mode: str
    analytical_basis: str
    executive_summary: str
    portfolio_identity: str
    key_strengths: List[str]
    key_weaknesses: List[str]
    macro_reading: str
    hidden_risks: List[str]
    asset_roles: List[AssetRoleItem]
    misalignments: List[str]
    action_plan: List[str]
    final_insight: str
