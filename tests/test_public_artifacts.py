import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DASHBOARD = ROOT / "outputs" / "dashboard" / "grid-electrification-command-center.html"
PUBLIC_REPORT = ROOT / "outputs" / "reports" / "informe_analitico_red_electrificacion.pdf"
PUBLIC_GRAPHS = ROOT / "outputs" / "graphs"
PUBLIC_FONTS = ROOT / "assets" / "fonts"

PUBLIC_TEXT_GLOBS = [
    "README.md",
    "docs/*.md",
    "outputs/**/*.md",
    "outputs/**/*.json",
    "notebooks/*.ipynb",
]


def test_public_text_artifacts_do_not_expose_local_paths():
    forbidden = [
        "/Users/",
        str(ROOT),
    ]

    checked = []
    for pattern in PUBLIC_TEXT_GLOBS:
        for path in ROOT.glob(pattern):
            if path.is_file():
                text = path.read_text(encoding="utf-8")
                checked.append(path)
                assert not any(token in text for token in forbidden), path

    assert checked


def test_publication_artifacts_are_complete_and_standalone():
    graphs = sorted(PUBLIC_GRAPHS.glob("*.png"))
    reports = sorted((ROOT / "outputs" / "reports").glob("*.pdf"))

    assert PUBLIC_DASHBOARD.exists()
    assert PUBLIC_REPORT.exists()
    assert reports == [PUBLIC_REPORT]
    report_bytes = PUBLIC_REPORT.read_bytes()
    assert report_bytes.startswith(b"%PDF")
    assert len(report_bytes) > 1_000_000
    assert b"Inter-SemiBold" in report_bytes
    assert b"SourceSerif4" in report_bytes
    assert max(map(int, re.findall(rb"/Count\s+(\d+)", report_bytes))) >= 30
    assert len(graphs) == 19
    assert all(path.stat().st_size > 10_000 for path in graphs)

    dashboard = PUBLIC_DASHBOARD.read_text(encoding="utf-8")
    assert "../../src/" not in dashboard
    assert "fonts.googleapis.com" not in dashboard
    assert "cdn.jsdelivr.net/npm/chart.js" not in dashboard


def test_report_font_assets_are_self_contained_and_licensed():
    required_fonts = {
        "Inter-Regular.ttf",
        "Inter-SemiBold.ttf",
        "SourceSerif4-Regular.ttf",
        "SourceSerif4-Bold.ttf",
        "SourceSerif4-Italic.ttf",
    }

    assert {path.name for path in PUBLIC_FONTS.glob("*.ttf")} == required_fonts
    assert all((PUBLIC_FONTS / name).stat().st_size > 100_000 for name in required_fonts)
    assert "SIL OPEN FONT LICENSE" in (PUBLIC_FONTS / "Inter-OFL.txt").read_text(encoding="utf-8")
    assert "SIL OPEN FONT LICENSE" in (PUBLIC_FONTS / "SourceSerif4-OFL.txt").read_text(encoding="utf-8")
