from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DASHBOARD = ROOT / "outputs" / "dashboard" / "grid-electrification-command-center.html"
PUBLIC_REPORT = ROOT / "outputs" / "reports" / "informe_analitico_red_electrificacion.pdf"
PUBLIC_GRAPHS = ROOT / "outputs" / "graphs"

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

    assert PUBLIC_DASHBOARD.exists()
    assert PUBLIC_REPORT.exists()
    assert PUBLIC_REPORT.read_bytes().startswith(b"%PDF")
    assert len(graphs) == 19
    assert all(path.stat().st_size > 10_000 for path in graphs)

    dashboard = PUBLIC_DASHBOARD.read_text(encoding="utf-8")
    assert "../../src/assets" not in dashboard
    assert "fonts.googleapis.com" not in dashboard
    assert "cdn.jsdelivr.net/npm/chart.js" not in dashboard
