from __future__ import annotations

import streamlit as st

from core.patch6_report_v2 import render_patch6_report_v2


def _first_non_empty(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, (list, tuple, set, dict)) and len(v) == 0:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _normalize_tickers(value) -> list[str]:
    """
    Normaliza tickers vindos de diferentes formatos possíveis no estado da sessão.
    """
    if value is None:
        return []

    if isinstance(value, dict):
        # tenta extrair chaves ou valores úteis
        possible = []
        for k, v in value.items():
            if isinstance(k, str) and k.strip():
                possible.append(k.strip().upper())
            elif isinstance(v, str) and v.strip():
                possible.append(v.strip().upper())
        return sorted(list(dict.fromkeys(possible)))

    if isinstance(value, (list, tuple, set)):
        normalized = []
        for item in value:
            if isinstance(item, str) and item.strip():
                normalized.append(item.strip().upper())
            elif isinstance(item, dict):
                ticker = (
                    item.get("ticker")
                    or item.get("Ticker")
                    or item.get("symbol")
                    or item.get("Symbol")
                )
                if isinstance(ticker, str) and ticker.strip():
                    normalized.append(ticker.strip().upper())
        return sorted(list(dict.fromkeys(normalized)))

    if isinstance(value, str):
        return [value.strip().upper()] if value.strip() else []

    return []


def _resolve_tickers_from_session() -> list[str]:
    """
    Busca tickers dinamicamente em múltiplas chaves possíveis do app.
    Não assume estrutura fixa.
    """
    candidates = [
        st.session_state.get("tickers_portfolio"),
        st.session_state.get("portfolio_tickers"),
        st.session_state.get("tickers"),
        st.session_state.get("portfolio"),
        st.session_state.get("portfolio_data"),
        st.session_state.get("selected_tickers"),
        st.session_state.get("carteira_tickers"),
        st.session_state.get("ativos_portfolio"),
        st.session_state.get("ativos_carteira"),
    ]

    for candidate in candidates:
        tickers = _normalize_tickers(candidate)
        if tickers:
            return tickers

    return []


def _resolve_period_ref() -> str:
    """
    Busca o período de forma dinâmica.
    """
    value = _first_non_empty(
        st.session_state.get("period_ref_patch6"),
        st.session_state.get("period_ref"),
        st.session_state.get("janela_patch6"),
        st.session_state.get("analysis_window"),
        st.session_state.get("periodo_analise"),
        "36M",
    )
    return str(value).strip()


def _resolve_analysis_mode() -> str:
    """
    Busca o modo de análise de forma dinâmica.
    """
    value = _first_non_empty(
        st.session_state.get("analysis_mode"),
        st.session_state.get("modo_analise"),
        "rigid",
    )
    value = str(value).strip().lower()
    return value if value in {"rigid", "flex"} else "rigid"


def _resolve_llm_factory():
    """
    Busca o llm_factory se ele já estiver disponível no estado da sessão.
    """
    return _first_non_empty(
        st.session_state.get("llm_factory"),
        st.session_state.get("patch6_llm_factory"),
    )


def render() -> None:
    st.title("📊 Análise de Portfólio V2")
    st.caption("Leitura dinâmica orientada à decisão, adaptada ao estado atual do sistema.")

    tickers = _resolve_tickers_from_session()
    period_ref = _resolve_period_ref()
    analysis_mode = _resolve_analysis_mode()
    llm_factory = _resolve_llm_factory()

    if not tickers:
        st.warning(
            "Nenhum ticker ativo foi encontrado no estado atual da aplicação. "
            "A V2 depende da carteira realmente carregada no sistema."
        )
        st.info(
            "Confirme se a carteira foi criada ou selecionada antes de abrir esta página, "
            "e se os tickers estão sendo salvos no session_state pelo fluxo principal do app."
        )
        return

    st.markdown(
        f"""
        **Carteira detectada:** {len(tickers)} ativo(s)  
        **Período:** {period_ref}  
        **Modo de análise:** {"Rígido" if analysis_mode == "rigid" else "Flexível"}
        """
    )

    render_patch6_report_v2(
        tickers=tickers,
        period_ref=period_ref,
        llm_factory=llm_factory,
        show_company_details=True,
        analysis_mode=analysis_mode,
    )
