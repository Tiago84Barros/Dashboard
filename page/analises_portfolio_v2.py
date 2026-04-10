from __future__ import annotations


def render() -> None:
    """Página V2 que reaproveita toda a lógica da página atual,
    mas substitui apenas o renderer do relatório consolidado pelo V2 real.
    """
    import core.patch6_report as legacy_report
    from core.patch6_report_v2 import render_patch6_report_v2
    from page import analises_portfolio as legacy_page

    legacy_report.render_patch6_report = render_patch6_report_v2
    legacy_page.render()
