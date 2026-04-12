# =========================
# PATCH6 REPORT — FINAL CONSOLIDADO (ETAPA 12)
# =========================

import streamlit as st
import math
import html

# ==========================================
# 🔹 UTILITÁRIOS
# ==========================================

def _esc(x):
    return html.escape(str(x)) if x is not None else ""

def _is_missing(v):
    try:
        return v is None or str(v).lower() in ["nan", "none", "-", ""]
    except:
        return True

def _safe_float(v):
    try:
        return float(v)
    except:
        return None

# ==========================================
# 🔹 MACRO SNAPSHOT
# ==========================================

def _macro_snapshot(m):
    return {
        "selic": _safe_float(m.get("selic")),
        "dolar": _safe_float(m.get("dolar")),
        "ipca": _safe_float(m.get("ipca")),
        "pib": _safe_float(m.get("pib")),
    }

# ==========================================
# 🔹 MACRO SCORE
# ==========================================

def _macro_score(company, macro):

    snap = _macro_snapshot(macro)
    score = 0
    drivers = []

    if snap["selic"]:
        if snap["selic"] > 12:
            score -= 2
            drivers.append("Selic elevada pressiona custo de capital")

    if snap["dolar"]:
        if company["ticker"].startswith(("PETR", "VALE")):
            score += 2
            drivers.append("Dólar favorece receita externa")

    if snap["pib"]:
        if snap["pib"] < 1:
            score -= 1
            drivers.append("PIB fraco limita crescimento")

    return score, drivers[:2]

# ==========================================
# 🔹 DECISÃO AJUSTADA
# ==========================================

def _final_action(base, macro_score):
    if base == "manter" and macro_score >= 2:
        return "aumentar"
    if base == "manter" and macro_score <= -2:
        return "revisar"
    return base

# ==========================================
# 🔹 RENDER MACRO TOPO
# ==========================================

def _render_macro(m):

    snap = _macro_snapshot(m)

    st.markdown("## 📊 Cenário Macro Atual")

    cols = st.columns(4)

    labels = ["Selic", "Dólar", "IPCA", "PIB"]
    keys = ["selic", "dolar", "ipca", "pib"]

    for col, k, l in zip(cols, keys, labels):
        if snap[k] is None:
            continue
        col.metric(l, snap[k])

# ==========================================
# 🔹 DECISÃO DO CICLO
# ==========================================

def _render_decisao(companies, macro):

    grupos = {"aumentar": [], "manter": [], "revisar": [], "reduzir": []}

    for c in companies:
        base = c["acao"]
        mscore, _ = _macro_score(c, macro)
        final = _final_action(base, mscore)
        grupos[final].append(c["ticker"])

    st.markdown("## 🧭 Decisão do Ciclo")

    cols = st.columns(3)

    cols[0].metric("Aumentar", ", ".join(grupos["aumentar"]))
    cols[1].metric("Manter/Revisar", ", ".join(grupos["manter"] + grupos["revisar"]))
    cols[2].metric("Reduzir", ", ".join(grupos["reduzir"]))

# ==========================================
# 🔹 RANKING RISCO
# ==========================================

def _render_risco(companies, macro):

    ranking = []

    for c in companies:
        base = c["risco"]
        mscore, drivers = _macro_score(c, macro)
        adj = base + (mscore * 5)
        ranking.append((c, adj, drivers))

    ranking.sort(key=lambda x: x[1], reverse=True)

    st.markdown("## ⚠️ Ranking de Risco")

    for c, score, drivers in ranking[:5]:
        st.markdown(f"""
        **{c['ticker']}**
        - Risco: {score}
        - Macro: {drivers[0] if drivers else "Neutro"}
        """)

# ==========================================
# 🔹 HISTÓRICO
# ==========================================

def _render_hist(c):

    delivered = len(c.get("delivered", []))
    risks = len(c.get("risks", []))

    if delivered > risks:
        label = "Histórico favorável"
    elif risks > delivered:
        label = "Histórico pressionado"
    else:
        label = "Histórico misto"

    st.markdown(f"🧠 **{label}**")

# ==========================================
# 🔹 EMPRESA
# ==========================================

def _render_empresa(c, macro):

    st.markdown(f"### {c['ticker']}")

    mscore, drivers = _macro_score(c, macro)

    st.markdown(f"""
    **Tese:** {c['tese']}

    **Força:** {c['forca']}

    **Risco:** {c['risco_txt']}

    **Macro:** {drivers[0] if drivers else "Neutro"}
    """)

    _render_hist(c)

# ==========================================
# 🔹 MAIN
# ==========================================

def render_patch6_report(data):

    macro = data["macro"]
    companies = data["companies"]

    _render_macro(macro)
    _render_decisao(companies, macro)
    _render_risco(companies, macro)

    st.markdown("## 🏢 Empresas")

    for c in companies:
        _render_empresa(c, macro)
