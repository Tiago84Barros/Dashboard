from typing import List, TypedDict


class AssetRoleItem(TypedDict, total=False):
    ticker: str
    role: str
    rationale: str


class AllocationSuggestionItem(TypedDict, total=False):
    ticker: str
    suggested_range: str
    rationale: str


class PortfolioReport(TypedDict, total=False):
    analysis_mode: str
    analytical_basis: str
    executive_summary: str
    portfolio_identity: str
    current_market_context: str
    macro_reading: str
    international_risk_links: List[str]
    macro_scenario_dependencies: List[str]
    portfolio_vulnerabilities_under_current_regime: List[str]
    what_the_portfolio_is_implicitly_betting_on: List[str]
    portfolio_concentration_analysis: str
    allocation_adjustment_rationale: str
    key_strengths: List[str]
    key_weaknesses: List[str]
    hidden_risks: List[str]
    asset_roles: List[AssetRoleItem]
    suggested_allocations: List[AllocationSuggestionItem]
    misalignments: List[str]
    action_plan: List[str]
    final_insight: str
