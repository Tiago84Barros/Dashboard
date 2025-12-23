from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import streamlit as st

try:
    from core.cvm_sync import apply_update, get_sync_status
except Exception:
    apply_update = None
    get_sync_status = None


def _fmt_dt(x: Optional[str]) -> str:
    if not x:
        return "—"
    try:
        d = dt.datetime.fromisoformat(str(x).replace("Z", "+00:00"))
        return d.astimezone().strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(x)


def _status_fallback() -> Dict[str, Any]:
    return {
        "last_year": None,
        "last_run_at": None,
        "remote_latest_year": None,
        "has_updates": None,
        "notes": "get_sync_status() não disponível ou falhou.",
    }


def render() -> None:
    st.markdown(
        """
        <style>
          .cfg-title {font-size: 2rem; font-weight: 800; margin-bottom: .25rem;}
          .cfg-sub {color: #6b7280; margin-bottom: 1.25rem;}
          .card {border: 1px solid rgba(0,0,0,.08); border-radius: 16px; padding: 16px; background: rgba(255,255,255,.75);}
          .pill {display:inline-block; padding: 4px 10px; border-radius: 999px; font-size:.85rem; border:1px solid rgba(0,0,0,.10);}
          .pill-ok {background: rgba(16,185,129,.12);}
          .pill-warn {background: rgba(245,158,11,.12);}
          .pill-bad {background: rgba(239,68,68,.12);}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='cfg-title'>⚙️ Configurações</div>", unsafe_allow_html=True)
    st.markdown("<div class='cfg-sub'>Atualize a base no Supabase e acompanhe o status em tempo real.</div>", unsafe_allow_html=True)

    status: Dict[str, Any] = _status_fallback()
    if callable(get_sync_status):
        try:
            status = get_sync_status() or status
        except Exception as e:
            status = {**status, "notes": f"Falha em get_sync_status(): {e}"}

    last_year = status.get("last_year")
    last_run_at = status.get("last_run_at")
    remote_latest_year = status.get("remote_latest_year")
    has_updates = status.get("has_updates")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Último ano no banco", value=str(last_year) if last_year else "—")
    with c2:
        st.metric("Última atualização", value=_fmt_dt(last_run_at))
    with c3:
        st.metric("Último ano disponível (CVM)", value=str(remote_latest_year) if remote_latest_year else "—")
    with c4:
        if has_updates is True:
            st.markdown("<span class='pill pill-warn'>Há atualizações pendentes</span>", unsafe_allow_html=True)
        elif has_updates is False:
            st.markdown("<span class='pill pill-ok'>Base está atualizada</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='pill pill-bad'>Status indisponível</span>", unsafe_allow_html=True)

    st.markdown("")
    left, right = st.columns([1, 1])

    with left:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Atualização do banco (Supabase)")

        if apply_update is None:
            st.error("apply_update() não foi encontrado. Verifique `core/cvm_sync.py` e os imports.")
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.write("Clique para sincronizar a base (CVM → Supabase).")

            run = st.button("🔄 Atualizar agora", use_container_width=True)
            progress = st.progress(0, text="Aguardando início…")
            log_box = st.empty()

            if run:
                logs = []

                def progress_cb(pct: float, msg: str = "") -> None:
                    pct = max(0.0, min(100.0, float(pct)))
                    progress.progress(int(pct), text=msg or f"Progresso: {pct:.0f}%")
                    if msg:
                        logs.append(msg)
                        log_box.markdown("\n".join([f"- {x}" for x in logs[-12:]]))

                try:
                    progress_cb(5, "Iniciando rotina…")

                    # Preferencial: apply_update suporta callback
                    try:
                        apply_update(progress_cb=progress_cb)
                    except TypeError:
                        # Fallback sem callback
                        progress_cb(20, "Executando atualização (sem progresso detalhado)…")
                        apply_update()
                        progress_cb(90, "Finalizando…")

                    progress_cb(100, "Concluído.")
                    st.success("Atualização concluída com sucesso.")
                    st.rerun()

                except Exception as e:
                    progress.progress(0, text="Falha na atualização.")
                    st.error(f"Erro na atualização: {e}")

            st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.subheader("Resumo e diagnóstico")

        st.markdown(
            f"""
            - **Último ano no banco:** {last_year if last_year else "—"}
            - **Última atualização:** {_fmt_dt(last_run_at)}
            - **Último ano disponível (CVM):** {remote_latest_year if remote_latest_year else "—"}
            - **Atualizações pendentes:** {("Sim" if has_updates else "Não") if has_updates in [True, False] else "Indisponível"}
            """.strip()
        )

        notes = status.get("notes")
        if notes:
            st.caption(notes)

        st.markdown("</div>", unsafe_allow_html=True)
