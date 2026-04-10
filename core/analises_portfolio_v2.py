from __future__ import annotations


def render() -> None:
    """Página V2 que reaproveita toda a lógica da página atual,
    mas substitui apenas o renderer do relatório consolidado pelo V2 real.
    """
    import core.patch6_report as legacy_report
    from core.patch6_report_v2_real import render_patch6_report_v2_real
    from page import analises_portfolio as legacy_page

    legacy_report.render_patch6_report = render_patch6_report_v2_real
    legacy_page.render()
