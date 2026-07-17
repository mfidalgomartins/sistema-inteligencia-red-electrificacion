from PIL import Image as PILImage
from reportlab.platypus import PageBreak, Paragraph, Table

from scripts import build_pdf_report as report
from scripts import build_publication_outputs as publication


def test_pdf_design_system_matches_consulting_reference_hierarchy():
    report._register_fonts()
    styles = report._styles()

    assert report.PAPER == "#FFFFFF"
    assert report.NAVY == "#0B2C5F"
    assert report.BLUE == "#1E5AE8"
    assert report.CYAN_SOFT == "#E7F6FA"
    assert styles["CoverTitle"].fontName == "SourceSerif-Bold"
    assert styles["H1"].fontName == "SourceSerif-Bold"
    assert styles["H2"].fontName == "SourceSerif-Bold"
    assert styles["Body"].fontName == "Inter"
    assert styles["Lead"].fontName == "Inter"


def test_pdf_design_system_removes_legacy_green_and_cream_palette():
    legacy_colors = {"#1F3B2D", "#2D5A42", "#FAFAF7", "#EEF0EC"}
    active_colors = {
        report.INK,
        report.NAVY,
        report.BLUE,
        report.CYAN,
        report.CYAN_SOFT,
        report.SLATE,
        report.MUTED,
        report.PAPER,
        report.LINE,
        report.SOFT,
    }

    assert legacy_colors.isdisjoint(active_colors)


def test_chart_palette_is_translated_to_blue_without_touching_warning_red():
    source = PILImage.new("RGB", (2, 1))
    source.putdata([(31, 59, 45), (156, 43, 27)])

    recolored = report._apply_chart_palette(source)
    primary = recolored.getpixel((0, 0))
    warning = recolored.getpixel((1, 0))

    assert primary != (31, 59, 45)
    assert primary[2] > primary[1] > primary[0]
    assert warning == (156, 43, 27)


def test_figure_subsection_uses_two_column_evidence_layout():
    report._register_fonts()
    styles = report._styles()
    story = []
    chart = report.GRAPHS / "01_tendencia_demanda_carga_neta.png"

    report._subsection(
        story,
        styles,
        "Título ejecutivo",
        ["Evidencia izquierda.", "Evidencia derecha.", "Lectura posterior."],
        figure=(chart, 1, "Título de figura", "Nota de figura."),
    )

    evidence_tables = [flowable for flowable in story if isinstance(flowable, Table)]
    assert len(evidence_tables) == 1
    assert evidence_tables[0]._ncols == 2


def test_numbered_section_starts_on_a_fresh_page_without_duplicate_breaks():
    report._register_fonts()
    styles = report._styles()
    story = [Paragraph("Conteúdo anterior", styles["Body"])]

    report._section(story, styles, "1 · Secção", "Título", "Introdução")

    assert isinstance(story[1], PageBreak)

    story = [PageBreak()]
    report._section(story, styles, "Resumo executivo", "Título", "Introdução")

    assert sum(isinstance(flowable, PageBreak) for flowable in story) == 1


def test_multi_column_text_distributes_content_in_reading_order():
    report._register_fonts()
    styles = report._styles()

    table = report._multi_column_text(
        ["Primeiro.", "Segundo.", "Terceiro.", "Quarto."],
        styles,
        columns=2,
    )

    assert table._ncols == 2
    assert [paragraph.getPlainText() for paragraph in table._cellvalues[0][0]] == ["Primeiro.", "Segundo."]
    assert [paragraph.getPlainText() for paragraph in table._cellvalues[0][1]] == ["Terceiro.", "Quarto."]


def test_recommendation_columns_keep_each_heading_with_its_body():
    report._register_fonts()
    styles = report._styles()
    recommendations = [(f"P{i}", f"Decisão {i}") for i in range(4)]

    table = report._recommendation_columns(recommendations, styles, columns=2)

    first_column = [paragraph.getPlainText() for paragraph in table._cellvalues[0][0]]
    second_column = [paragraph.getPlainText() for paragraph in table._cellvalues[0][1]]
    assert first_column == ["P0", "Decisão 0", "P1", "Decisão 1"]
    assert second_column == ["P2", "Decisão 2", "P3", "Decisão 3"]


def test_paginate_rows_repeats_header_and_preserves_all_data_rows():
    rows = [["HEADER"], ["1"], ["2"], ["3"], ["4"], ["5"]]

    pages = report._paginate_rows(rows, rows_per_page=2)

    assert pages == [
        [["HEADER"], ["1"], ["2"]],
        [["HEADER"], ["3"], ["4"]],
        [["HEADER"], ["5"]],
    ]


def test_prepare_outputs_preserves_existing_publication(monkeypatch, tmp_path):
    outputs = tmp_path / "outputs"
    graphs = outputs / "graphs"
    dashboard = outputs / "dashboard"
    reports = outputs / "reports"
    graphs.mkdir(parents=True)
    existing = graphs / "existing.png"
    existing.write_bytes(b"published")

    monkeypatch.setattr(publication, "OUTPUTS", outputs)
    monkeypatch.setattr(publication, "GRAPHS", graphs)
    monkeypatch.setattr(publication, "DASHBOARD", dashboard)
    monkeypatch.setattr(publication, "REPORTS", reports)

    publication.prepare_outputs()

    assert existing.read_bytes() == b"published"
    assert dashboard.is_dir()
    assert reports.is_dir()


def test_prune_stale_graphs_runs_only_against_generated_set(monkeypatch, tmp_path):
    graphs = tmp_path / "graphs"
    graphs.mkdir()
    current = graphs / "current.png"
    stale = graphs / "stale.png"
    current.write_bytes(b"current")
    stale.write_bytes(b"stale")
    monkeypatch.setattr(publication, "GRAPHS", graphs)

    publication.prune_stale_graphs([current])

    assert current.exists()
    assert not stale.exists()
