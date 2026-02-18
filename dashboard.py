# dashboard.py — robusto (não quebra se um módulo não existir)
from __future__ import annotations

import importlib
from typing import Callable, Dict, List, Optional, Tuple

import streamlit as st


def _import_first(paths: List[str]) -> Tuple[Optional[object], Optional[str], Optional[Exception]]:
    """Tenta importar o primeiro módulo disponível em `paths`."""
    last_err: Optional[Exception] = None
    for p in paths:
        try:
            mod = importlib.import_module(p)
            return mod, p, None
        except Exception as e:
            last_err = e
            continue
    return None, None, last_err


def _get_renderer(mod: object) -> Optional[Callable[[], None]]:
    """Padrão do projeto: render(). Fallback: main()."""
    fn = getattr(mod, "render", None)
    if callable(fn):
        return fn
    fn = getattr(mod, "main", None)
    if callable(fn):
        return fn
    return None


def _build_pages() -> Dict[str, Dict[str, object]]:
    """
    Define páginas com candidatos. Só entra no menu se importar + tiver render/main.
    Ajuste/adicione candidatos conforme o seu repo.
    """
    candidates: Dict[str, List[str]] = {
        "Básica": [
            "page.basica",
            "page.basic",
            "page.basico",
            "page.empresas_view",  # se sua “básica” for esse módulo
        ],
        "Avançada": [
            "page.advanced",
            "advanced",
        ],
        "Criação de Portfólio": [
            "page.criacao_portfolio",
            "criacao_portfolio",
        ],
        "Análises de Portfólio": [
            "page.analises_portfolio",   # novo nome
            "page.patch6_teste",         # compatibilidade
            "patch6_teste",
        ],
        "Configurações": [
            "page.configuracoes",
            "configuracoes",
            "page.configuracao",
        ],
    }

    pages_ok: Dict[str, Dict[str, object]] = {}
    for label, paths in candidates.items():
        mod, used, err = _import_first(paths)
        if mod is None:
            continue
        renderer = _get_renderer(mod)
        if renderer is None:
            continue
        pages_ok[label] = {"module": mod, "path": used, "render": renderer}

    return pages_ok


def main() -> None:
    st.set_page_config(page_title="Dashboard Financeiro", layout="wide")

    pages = _build_pages()

    st.sidebar.title("Análises")

    if not pages:
        st.error("Nenhuma página pôde ser carregada. Verifique nomes dos módulos em /page e __init__.py.")
        st.stop()

    labels = list(pages.keys())
    escolha = st.sidebar.radio("Escolha a seção:", labels)

    mod_path = pages[escolha]["path"]
    render_fn = pages[escolha]["render"]

    try:
        render_fn()
    except Exception as e:
        st.error("Falha ao carregar a página selecionada.")
        st.caption(f"Módulo: {mod_path}")
        st.exception(e)


if __name__ == "__main__":
    main()
