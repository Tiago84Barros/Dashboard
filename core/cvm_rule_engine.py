"""
core/cvm_rule_engine.py

Motor inicial de regras contextuais para o pipeline CVM V2.
Etapa 1: priorização, validade temporal, escopo e tratamento conservador
para conflitos simples.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

SCOPE_RANK = {
    "global": 1,
    "sector": 2,
    "company": 3,
}

STATEMENT_ALIASES = {
    "dre": {"dre", "demonstracao do resultado", "demonstração do resultado", "resultado"},
    "bpa": {"bpa", "ativo"},
    "bpp": {"bpp", "passivo", "patrimonio liquido", "patrimônio líquido"},
    "dfc": {"dfc", "fluxo de caixa", "caixa"},
    "dmpl": {"dmpl", "mutacoes do patrimonio liquido", "mutações do patrimônio líquido"},
    "dva": {"dva", "valor adicionado"},
}


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == ""


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).strip().lower()


def _parse_date(value: Any) -> Optional[date]:
    if _is_blank(value):
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def _matches_dimension(rule_value: Any, row_value: Any, aliases: Optional[dict[str, set[str]]] = None) -> bool:
    rule_norm = normalize_text(rule_value)
    row_norm = normalize_text(row_value)

    if not rule_norm or rule_norm in {"all", "both"}:
        return True
    if not row_norm:
        return False
    if rule_norm == row_norm:
        return True
    if rule_norm in row_norm or row_norm in rule_norm:
        return True

    if aliases:
        for canonical, variants in aliases.items():
            all_terms = {normalize_text(canonical), *(normalize_text(v) for v in variants)}
            if rule_norm in all_terms and any(term and term in row_norm for term in all_terms):
                return True

    return False


def prepare_rules(mappings: pd.DataFrame) -> pd.DataFrame:
    if mappings is None or mappings.empty:
        return pd.DataFrame()

    rules = mappings.copy()

    for col in [
        "cd_conta", "ds_conta_pattern", "canonical_key", "sinal", "prioridade",
        "priority", "confidence_score", "rule_scope", "source_doc", "statement_type",
        "parent_cd_conta", "level_min", "level_max", "sector", "company_cvm",
        "valid_from", "valid_to", "notes",
    ]:
        if col not in rules.columns:
            rules[col] = None

    rules["cd_conta"] = rules["cd_conta"].astype(str).str.strip().replace({"nan": ""})
    rules["canonical_key"] = rules["canonical_key"].astype(str).str.strip().replace({"nan": ""})
    rules["rule_scope"] = rules["rule_scope"].apply(lambda v: normalize_text(v) or None)
    rules["source_doc"] = rules["source_doc"].apply(lambda v: normalize_text(v) or None)
    rules["statement_type"] = rules["statement_type"].apply(lambda v: normalize_text(v) or None)
    rules["parent_cd_conta"] = rules["parent_cd_conta"].astype(str).str.strip().replace({"nan": ""})
    rules["sector"] = rules["sector"].apply(lambda v: normalize_text(v) or None)

    rules["priority_effective"] = pd.to_numeric(
        rules["priority"].where(rules["priority"].notna(), rules["prioridade"]),
        errors="coerce",
    ).fillna(0).astype(int)

    rules["confidence_effective"] = pd.to_numeric(rules["confidence_score"], errors="coerce").fillna(1.0)
    rules["sinal_effective"] = pd.to_numeric(rules["sinal"], errors="coerce").fillna(1.0)

    def _derive_scope(row: pd.Series) -> str:
        scope = row.get("rule_scope")
        if scope in SCOPE_RANK:
            return scope
        if not _is_blank(row.get("company_cvm")):
            return "company"
        if not _is_blank(row.get("sector")):
            return "sector"
        return "global"

    rules["rule_scope_effective"] = rules.apply(_derive_scope, axis=1)
    rules["scope_rank"] = rules["rule_scope_effective"].map(SCOPE_RANK).fillna(0).astype(int)

    rules["valid_from_effective"] = rules["valid_from"].apply(_parse_date)
    rules["valid_to_effective"] = rules["valid_to"].apply(_parse_date)
    rules["level_min_effective"] = pd.to_numeric(rules["level_min"], errors="coerce")
    rules["level_max_effective"] = pd.to_numeric(rules["level_max"], errors="coerce")

    compiled_patterns: List[Optional[re.Pattern[str]]] = []
    for pat in rules["ds_conta_pattern"].tolist():
        if _is_blank(pat):
            compiled_patterns.append(None)
            continue
        try:
            compiled_patterns.append(re.compile(str(pat), re.IGNORECASE))
        except re.error:
            compiled_patterns.append(None)
    rules["compiled_pattern"] = compiled_patterns

    rules = rules[rules["canonical_key"].ne("")].copy()
    rules = rules.sort_values(
        by=["scope_rank", "priority_effective", "confidence_effective"],
        ascending=[False, False, False],
        kind="stable",
    ).reset_index(drop=True)
    return rules


def build_rule_indexes(rules: pd.DataFrame) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    if rules is None or rules.empty:
        return {}, {}, []

    records = rules.to_dict(orient="records")
    exact_groups: Dict[str, List[Dict[str, Any]]] = {}
    regex_rules: List[Dict[str, Any]] = []

    for rule in records:
        cd_conta = str(rule.get("cd_conta") or "").strip()
        if cd_conta:
            exact_groups.setdefault(cd_conta, []).append(rule)
        if rule.get("compiled_pattern") is not None:
            regex_rules.append(rule)

    def _is_fast_rule(rule: Dict[str, Any]) -> bool:
        return (
            rule.get("rule_scope_effective") == "global"
            and rule.get("compiled_pattern") is None
            and _is_blank(rule.get("source_doc"))
            and _is_blank(rule.get("statement_type"))
            and _is_blank(rule.get("company_cvm"))
            and _is_blank(rule.get("sector"))
            and _is_blank(rule.get("parent_cd_conta"))
            and pd.isna(rule.get("level_min_effective"))
            and pd.isna(rule.get("level_max_effective"))
            and rule.get("valid_from_effective") is None
            and rule.get("valid_to_effective") is None
        )

    fast_exact: Dict[str, Dict[str, Any]] = {}
    exact_candidates: Dict[str, List[Dict[str, Any]]] = {}

    for cd_conta, group in exact_groups.items():
        if len(group) == 1 and _is_fast_rule(group[0]):
            fast_exact[cd_conta] = group[0]
        else:
            exact_candidates[cd_conta] = group

    return fast_exact, exact_candidates, regex_rules


def _rule_matches_context(row: Dict[str, Any], rule: Dict[str, Any], require_pattern: bool = False) -> bool:
    row_source_doc = normalize_text(row.get("source_doc"))
    row_statement_type = normalize_text(row.get("tipo_demo"))
    row_ds_conta = str(row.get("ds_conta") or "")
    row_company_cvm = row.get("cd_cvm")
    row_parent_cd_conta = str(row.get("conta_pai") or "").strip()
    row_level = pd.to_numeric(pd.Series([row.get("nivel_conta")]), errors="coerce").iloc[0]
    row_dt = _parse_date(row.get("dt_refer"))
    row_sector = normalize_text(row.get("sector"))

    source_doc = normalize_text(rule.get("source_doc"))
    if not _matches_dimension(source_doc, row_source_doc):
        return False

    statement_type = normalize_text(rule.get("statement_type"))
    if not _matches_dimension(statement_type, row_statement_type, aliases=STATEMENT_ALIASES):
        return False

    company_cvm = rule.get("company_cvm")
    if not _is_blank(company_cvm) and str(company_cvm).strip() != str(row_company_cvm).strip():
        return False

    sector = normalize_text(rule.get("sector"))
    if sector and sector != row_sector:
        return False

    parent_cd_conta = str(rule.get("parent_cd_conta") or "").strip()
    if parent_cd_conta and parent_cd_conta != row_parent_cd_conta:
        return False

    level_min = rule.get("level_min_effective")
    if not pd.isna(level_min):
        if pd.isna(row_level) or float(row_level) < float(level_min):
            return False

    level_max = rule.get("level_max_effective")
    if not pd.isna(level_max):
        if pd.isna(row_level) or float(row_level) > float(level_max):
            return False

    valid_from = rule.get("valid_from_effective")
    if valid_from is not None and (row_dt is None or row_dt < valid_from):
        return False

    valid_to = rule.get("valid_to_effective")
    if valid_to is not None and (row_dt is None or row_dt > valid_to):
        return False

    compiled = rule.get("compiled_pattern")
    if require_pattern and compiled is None:
        return False
    if compiled is not None and not compiled.search(row_ds_conta):
        return False

    return True


def _rule_specificity(rule: Dict[str, Any]) -> int:
    score = 0
    if not _is_blank(rule.get("company_cvm")):
        score += 4
    if not _is_blank(rule.get("sector")):
        score += 3
    if not _is_blank(rule.get("source_doc")):
        score += 2
    if not _is_blank(rule.get("statement_type")):
        score += 2
    if not _is_blank(rule.get("parent_cd_conta")):
        score += 1
    if rule.get("compiled_pattern") is not None:
        score += 1
    if rule.get("valid_from_effective") is not None or rule.get("valid_to_effective") is not None:
        score += 1
    if not pd.isna(rule.get("level_min_effective")) or not pd.isna(rule.get("level_max_effective")):
        score += 1
    return score


def select_best_rule(row: Dict[str, Any], candidates: Iterable[Dict[str, Any]], require_pattern: bool = False) -> Tuple[Optional[Dict[str, Any]], bool]:
    matched: List[Tuple[Tuple[int, int, float], Dict[str, Any]]] = []
    for rule in candidates:
        if not _rule_matches_context(row, rule, require_pattern=require_pattern):
            continue
        ranking = (
            int(rule.get("scope_rank") or 0),
            _rule_specificity(rule),
            int(rule.get("priority_effective") or 0),
            float(rule.get("confidence_effective") or 0.0),
        )
        matched.append((ranking, rule))

    if not matched:
        return None, False

    matched.sort(key=lambda item: item[0], reverse=True)
    best_rank, best_rule = matched[0]

    if len(matched) > 1:
        second_rank, second_rule = matched[1]
        same_rank = second_rank == best_rank
        conflicting_target = (
            second_rule.get("canonical_key") != best_rule.get("canonical_key")
            or float(second_rule.get("sinal_effective") or 1.0) != float(best_rule.get("sinal_effective") or 1.0)
        )
        if same_rank and conflicting_target:
            return None, True

    return best_rule, False
