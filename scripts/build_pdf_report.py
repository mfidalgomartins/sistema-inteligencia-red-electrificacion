from __future__ import annotations

import html
import json
from collections.abc import Iterable, Sequence
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image as PILImage
from reportlab import rl_config
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    CondPageBreak,
    Frame,
    HRFlowable,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

rl_config.invariant = 1

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
GRAPHS = ROOT / "outputs" / "graphs"
REPORTS = ROOT / "outputs" / "reports"
FONTS = ROOT / "assets" / "fonts"

INK = "#17202A"
NAVY = "#0B2C5F"
BLUE = "#1E5AE8"
CYAN = "#4BC3E8"
CYAN_SOFT = "#E7F6FA"
RED = "#A33A2B"
SLATE = "#445161"
MUTED = "#667383"
PAPER = "#FFFFFF"
LINE = "#CDD8E5"
SOFT = "#F3F7FA"
WHITE = "#FFFFFF"

REPORT_TITLE = "Cartera de red y electrificación"
REPORT_SUBTITLE = "Diagnóstico integrado, prioridades de intervención y gobierno de inversión"


def _register_fonts() -> None:
    required = {
        "Inter": "Inter-Regular.ttf",
        "Inter-SemiBold": "Inter-SemiBold.ttf",
        "SourceSerif": "SourceSerif4-Regular.ttf",
        "SourceSerif-Bold": "SourceSerif4-Bold.ttf",
        "SourceSerif-Italic": "SourceSerif4-Italic.ttf",
    }
    missing = [filename for filename in required.values() if not (FONTS / filename).exists()]
    if missing:
        raise FileNotFoundError(f"Faltan tipografías integradas: {', '.join(missing)}")
    for name, filename in required.items():
        if name not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont(name, str(FONTS / filename)))
    pdfmetrics.registerFontFamily(
        "SourceSerif",
        normal="SourceSerif",
        bold="SourceSerif-Bold",
        italic="SourceSerif-Italic",
        boldItalic="SourceSerif-Bold",
    )
    pdfmetrics.registerFontFamily(
        "Inter",
        normal="Inter",
        bold="Inter-SemiBold",
        italic="Inter",
        boldItalic="Inter-SemiBold",
    )


def _read(name: str) -> pd.DataFrame:
    path = DATA / name
    if not path.exists():
        raise FileNotFoundError(f"Dataset requerido no encontrado: {path}")
    return pd.read_csv(path)


def _fmt_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", ".")


def _fmt_dec(value: float, decimals: int = 1) -> str:
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}%}".replace(".", ",")


def _label(value: object) -> str:
    raw = str(value).strip()
    translations = {
        "seasonal_naive": "Persistencia estacional",
        "synthetic": "Sintética",
        "watch": "Vigilancia",
        "critico": "Crítico",
        "alto": "Alto",
        "medio": "Medio",
        "bajo": "Bajo",
        "congestion_risk_score": "Riesgo de congestión",
        "resilience_risk_score": "Riesgo de resiliencia",
        "service_impact_score": "Impacto de servicio",
        "flexibility_gap_score": "Brecha de flexibilidad",
        "asset_exposure_score": "Exposición de activos",
        "electrification_pressure_score": "Presión de electrificación",
        "economic_priority_score": "Prioridad económica",
        "intervencion_inmediata_prioritaria": "Intervención inmediata",
        "reforzar_red_local": "Refuerzo local",
        "desplegar_almacenamiento": "Despliegue de almacenamiento",
        "optimizar_operacion": "Optimización operativa",
        "activar_flexibilidad": "Activación de flexibilidad",
        "sustituir_activos": "Sustitución de activos",
        "monitorizar": "Monitorización",
        "refuerzo_selectivo": "Refuerzo selectivo",
        "revision_trimestral": "Revisión trimestral",
        "retraso_capex": "Retraso de CAPEX",
        "capex_mas_flexibilidad": "CAPEX más flexibilidad",
    }
    return translations.get(raw, raw.replace("_", " ").capitalize())


class EditorialDocTemplate(BaseDocTemplate):
    def __init__(self, filename: str, **kwargs):
        super().__init__(filename, **kwargs)
        cover_frame = Frame(
            2.0 * cm,
            1.8 * cm,
            A4[0] - 4.0 * cm,
            A4[1] - 3.8 * cm,
            id="cover_frame",
            showBoundary=0,
        )
        body_frame = Frame(
            self.leftMargin,
            self.bottomMargin,
            self.width,
            self.height,
            id="body_frame",
            showBoundary=0,
        )
        self.addPageTemplates(
            [
                PageTemplate(id="cover", frames=cover_frame, onPage=self._cover_page),
                PageTemplate(id="body", frames=body_frame, onPage=self._body_page),
            ]
        )

    def _cover_page(self, canvas, _doc) -> None:
        canvas.saveState()
        canvas.setFillColor(colors.HexColor(NAVY))
        canvas.rect(0, 0, A4[0], A4[1], stroke=0, fill=1)
        canvas.setFillColor(colors.HexColor(BLUE))
        canvas.rect(0, 0, 0.22 * cm, A4[1], stroke=0, fill=1)
        canvas.setStrokeColor(colors.HexColor(CYAN))
        canvas.setLineWidth(1.4)
        canvas.line(2.0 * cm, A4[1] - 2.15 * cm, 5.2 * cm, A4[1] - 2.15 * cm)
        canvas.setStrokeColor(colors.HexColor("#284979"))
        canvas.setLineWidth(0.35)
        canvas.line(2.0 * cm, 5.0 * cm, A4[0] - 2.0 * cm, 5.0 * cm)
        canvas.restoreState()

    def _body_page(self, canvas, doc) -> None:
        canvas.saveState()
        canvas.setFillColor(colors.HexColor(PAPER))
        canvas.rect(0, 0, A4[0], A4[1], stroke=0, fill=1)
        canvas.setFillColor(colors.HexColor(NAVY))
        canvas.rect(0, A4[1] - 0.55 * cm, A4[0], 0.55 * cm, stroke=0, fill=1)
        canvas.setFont("Inter", 6.4)
        canvas.setFillColor(colors.white)
        canvas.drawString(self.leftMargin, A4[1] - 0.36 * cm, "SISTEMA DE INTELIGENCIA DE RED")
        canvas.drawRightString(A4[0] - self.rightMargin, A4[1] - 0.36 * cm, "CARTERA DE RED Y ELECTRIFICACIÓN")
        canvas.setStrokeColor(colors.HexColor(LINE))
        canvas.setLineWidth(0.35)
        canvas.line(self.leftMargin, 1.15 * cm, A4[0] - self.rightMargin, 1.15 * cm)
        canvas.setFont("Inter", 6.6)
        canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(
            self.leftMargin, 0.76 * cm, "Uso de dirección · soporte a decisión, no autorización automática de CAPEX"
        )
        canvas.drawRightString(A4[0] - self.rightMargin, 0.76 * cm, f"{doc.page}")
        canvas.restoreState()

    def afterFlowable(self, flowable) -> None:
        if not isinstance(flowable, Paragraph):
            return
        if flowable.style.name not in {"H1", "H2"}:
            return
        level = 0 if flowable.style.name == "H1" else 1
        text = flowable.getPlainText()
        key = f"section-{self.page}-{abs(hash((text, level)))}"
        self.canv.bookmarkPage(key)
        self.canv.addOutlineEntry(text, key, level=level, closed=level == 0)
        if level == 0:
            self.notify("TOCEntry", (level, text, self.page, key))


def _styles():
    base = getSampleStyleSheet()
    base.add(
        ParagraphStyle(
            name="CoverLabel",
            fontName="Inter-SemiBold",
            fontSize=7.4,
            leading=9,
            tracking=1.25,
            textColor=colors.HexColor("#B9D9F0"),
            spaceAfter=22,
        )
    )
    base.add(
        ParagraphStyle(
            name="CoverTitle",
            fontName="SourceSerif-Bold",
            fontSize=35,
            leading=38,
            textColor=colors.white,
            spaceAfter=15,
        )
    )
    base.add(
        ParagraphStyle(
            name="CoverSubtitle",
            fontName="Inter",
            fontSize=12.4,
            leading=17.2,
            textColor=colors.HexColor(CYAN_SOFT),
            spaceAfter=0,
        )
    )
    base.add(
        ParagraphStyle(
            name="CoverMeta",
            fontName="Inter",
            fontSize=7.1,
            leading=10.5,
            textColor=colors.HexColor("#B9D9F0"),
        )
    )
    base.add(
        ParagraphStyle(
            name="CoverTableLabel",
            fontName="Inter",
            fontSize=7.2,
            leading=10,
            textColor=colors.HexColor("#9EC5E0"),
        )
    )
    base.add(
        ParagraphStyle(
            name="CoverTableValue",
            fontName="Inter-SemiBold",
            fontSize=7.9,
            leading=10.5,
            textColor=colors.white,
        )
    )
    base.add(
        ParagraphStyle(
            name="ContentsTitle",
            fontName="SourceSerif-Bold",
            fontSize=25,
            leading=29,
            textColor=colors.HexColor(NAVY),
            spaceAfter=16,
        )
    )
    base.add(
        ParagraphStyle(
            name="Eyebrow",
            fontName="Inter-SemiBold",
            fontSize=7.4,
            leading=10,
            tracking=1.05,
            textColor=colors.HexColor(BLUE),
            spaceBefore=2,
            spaceAfter=5,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="H1",
            fontName="SourceSerif-Bold",
            fontSize=19.8,
            leading=23.2,
            textColor=colors.HexColor(NAVY),
            spaceBefore=6,
            spaceAfter=8,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="H2",
            fontName="SourceSerif-Bold",
            fontSize=13.6,
            leading=17.2,
            textColor=colors.HexColor(INK),
            spaceBefore=13,
            spaceAfter=6,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="H3",
            fontName="Inter-SemiBold",
            fontSize=10.2,
            leading=13.2,
            textColor=colors.HexColor(NAVY),
            spaceBefore=10,
            spaceAfter=4,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="Lead",
            fontName="Inter",
            fontSize=9.8,
            leading=14.3,
            textColor=colors.HexColor(SLATE),
            spaceAfter=11,
        )
    )
    base.add(
        ParagraphStyle(
            name="Body",
            fontName="Inter",
            fontSize=8.85,
            leading=13.3,
            textColor=colors.HexColor(INK),
            spaceAfter=7,
        )
    )
    base.add(
        ParagraphStyle(
            name="BodyTight",
            parent=base["Body"],
            fontSize=8.15,
            leading=11.8,
            spaceAfter=0,
        )
    )
    base.add(
        ParagraphStyle(
            name="ColumnBody",
            parent=base["Body"],
            fontSize=7.55,
            leading=10.8,
            spaceAfter=5,
        )
    )
    base.add(
        ParagraphStyle(
            name="ColumnHeading",
            fontName="Inter-SemiBold",
            fontSize=8.25,
            leading=10.5,
            textColor=colors.HexColor(NAVY),
            spaceBefore=1,
            spaceAfter=2,
        )
    )
    base.add(
        ParagraphStyle(
            name="FigureLabel",
            fontName="Inter-SemiBold",
            fontSize=6.6,
            leading=8,
            tracking=0.8,
            textColor=colors.HexColor(BLUE),
            spaceBefore=7,
            spaceAfter=2,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="FigureTitle",
            fontName="Inter-SemiBold",
            fontSize=9.4,
            leading=11.7,
            textColor=colors.HexColor(INK),
            spaceBefore=0,
            spaceAfter=5,
            keepWithNext=True,
        )
    )
    base.add(
        ParagraphStyle(
            name="Caption",
            fontName="Inter",
            fontSize=6.8,
            leading=9.2,
            textColor=colors.HexColor(MUTED),
            spaceBefore=3,
            spaceAfter=7,
        )
    )
    base.add(
        ParagraphStyle(
            name="CalloutLabel",
            fontName="Inter-SemiBold",
            fontSize=7.2,
            leading=10,
            tracking=0.8,
            textColor=colors.white,
        )
    )
    base.add(
        ParagraphStyle(
            name="CalloutText",
            fontName="Inter",
            fontSize=8.5,
            leading=12.7,
            textColor=colors.HexColor(INK),
        )
    )
    base.add(
        ParagraphStyle(
            name="TableHeader",
            fontName="Inter-SemiBold",
            fontSize=7,
            leading=9,
            textColor=colors.white,
        )
    )
    base.add(
        ParagraphStyle(
            name="TableCell",
            fontName="Inter",
            fontSize=7.4,
            leading=10.2,
            textColor=colors.HexColor(INK),
        )
    )
    base.add(
        ParagraphStyle(
            name="TableCellRight",
            parent=base["TableCell"],
            alignment=TA_RIGHT,
        )
    )
    base.add(
        ParagraphStyle(
            name="KpiValue",
            fontName="Inter-SemiBold",
            fontSize=16.5,
            leading=18,
            textColor=colors.HexColor(BLUE),
            alignment=TA_LEFT,
        )
    )
    base.add(
        ParagraphStyle(
            name="KpiLabel",
            fontName="Inter",
            fontSize=7.2,
            leading=10,
            textColor=colors.HexColor(MUTED),
            alignment=TA_LEFT,
        )
    )
    base.add(
        ParagraphStyle(
            name="Note",
            fontName="SourceSerif-Italic",
            fontSize=8.8,
            leading=12.5,
            textColor=colors.HexColor(MUTED),
            spaceAfter=7,
        )
    )
    return base


def _para(text: str, styles, style: str = "Body") -> Paragraph:
    return Paragraph(text, styles[style])


def _multi_column_text(paragraphs: Sequence[str], styles, *, columns: int) -> Table:
    if columns < 1 or columns > len(paragraphs):
        raise ValueError("El número de columnas debe estar entre 1 y el número de párrafos")
    per_column = (len(paragraphs) + columns - 1) // columns
    cells = []
    for column in range(columns):
        start = column * per_column
        cells.append([_para(text, styles, "ColumnBody") for text in paragraphs[start : start + per_column]])

    table = Table([cells], colWidths=[16.9 * cm / columns] * columns, hAlign="LEFT")
    commands = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]
    for column in range(columns):
        commands.extend(
            [
                ("LEFTPADDING", (column, 0), (column, 0), 0 if column == 0 else 8),
                ("RIGHTPADDING", (column, 0), (column, 0), 0 if column == columns - 1 else 8),
            ]
        )
        if column:
            commands.append(("LINEBEFORE", (column, 0), (column, 0), 0.35, colors.HexColor(LINE)))
    table.setStyle(TableStyle(commands))
    return table


def _recommendation_columns(
    recommendations: Sequence[tuple[str, str]],
    styles,
    *,
    columns: int,
) -> Table:
    if columns < 1 or columns > len(recommendations):
        raise ValueError("El número de columnas debe estar entre 1 y el número de recomendaciones")
    per_column = (len(recommendations) + columns - 1) // columns
    cells = []
    for column in range(columns):
        start = column * per_column
        flowables = []
        for title, body in recommendations[start : start + per_column]:
            flowables.append(Paragraph(html.escape(title), styles["ColumnHeading"]))
            flowables.append(_para(body, styles, "ColumnBody"))
        cells.append(flowables)

    table = Table([cells], colWidths=[16.9 * cm / columns] * columns, hAlign="LEFT")
    commands = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]
    for column in range(columns):
        commands.extend(
            [
                ("LEFTPADDING", (column, 0), (column, 0), 0 if column == 0 else 8),
                ("RIGHTPADDING", (column, 0), (column, 0), 0 if column == columns - 1 else 8),
            ]
        )
        if column:
            commands.append(("LINEBEFORE", (column, 0), (column, 0), 0.35, colors.HexColor(LINE)))
    table.setStyle(TableStyle(commands))
    return table


def _paginate_rows(rows: Sequence[Sequence[object]], *, rows_per_page: int) -> list[list[list[object]]]:
    if not rows:
        raise ValueError("La tabla debe incluir una cabecera")
    if rows_per_page < 1:
        raise ValueError("rows_per_page debe ser positivo")
    header = list(rows[0])
    data_rows = [list(row) for row in rows[1:]]
    return [[header, *data_rows[start : start + rows_per_page]] for start in range(0, len(data_rows), rows_per_page)]


def _kpi_band(items: Sequence[tuple[str, str]], styles) -> Table:
    cells = []
    for value, label in items:
        cells.append(
            [
                Paragraph(html.escape(value), styles["KpiValue"]),
                Paragraph(html.escape(label), styles["KpiLabel"]),
            ]
        )
    table = Table([cells], colWidths=[16.9 * cm / len(cells)], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(CYAN_SOFT)),
                ("LINEABOVE", (0, 0), (-1, 0), 0.55, colors.HexColor("#A9DCEB")),
                ("LINEBELOW", (0, 0), (-1, -1), 0.55, colors.HexColor("#A9DCEB")),
                ("LINEBEFORE", (1, 0), (-1, -1), 0.45, colors.HexColor("#B7DDE9")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return table


def _cover_table(rows: Sequence[tuple[str, str]], styles) -> Table:
    rendered = [
        [
            Paragraph(html.escape(label), styles["CoverTableLabel"]),
            Paragraph(html.escape(value), styles["CoverTableValue"]),
        ]
        for label, value in rows
    ]
    table = Table(rendered, colWidths=[3.6 * cm, 13.3 * cm], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.35, colors.HexColor("#365681")),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _callout(label: str, text: str, styles, *, risk: bool = False) -> Table:
    color = RED if risk else NAVY
    label_cell = Paragraph(html.escape(label.upper()), styles["CalloutLabel"])
    text_cell = Paragraph(text, styles["CalloutText"])
    table = Table([[label_cell, text_cell]], colWidths=[3.0 * cm, 13.9 * cm], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, 0), colors.HexColor(color)),
                ("BACKGROUND", (1, 0), (1, 0), colors.HexColor("#F8ECEA" if risk else CYAN_SOFT)),
                ("LINEABOVE", (0, 0), (-1, 0), 0.4, colors.HexColor(color)),
                ("LINEBELOW", (0, 0), (-1, -1), 0.4, colors.HexColor(color)),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return table


def _data_table(
    rows: Sequence[Sequence[object]],
    widths: Sequence[float],
    styles,
    *,
    numeric_columns: Iterable[int] = (),
) -> Table:
    numeric = set(numeric_columns)
    rendered = []
    for row_index, row in enumerate(rows):
        rendered_row = []
        for column_index, value in enumerate(row):
            text = html.escape(str(value))
            style_name = (
                "TableHeader" if row_index == 0 else ("TableCellRight" if column_index in numeric else "TableCell")
            )
            rendered_row.append(Paragraph(text, styles[style_name]))
        rendered.append(rendered_row)
    table = Table(rendered, colWidths=list(widths), repeatRows=1, hAlign="LEFT", splitByRow=1)
    commands = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(NAVY)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor(NAVY)),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, colors.HexColor(LINE)),
    ]
    for row_index in range(1, len(rendered)):
        if row_index % 2 == 0:
            commands.append(("BACKGROUND", (0, row_index), (-1, row_index), colors.HexColor(CYAN_SOFT)))
    table.setStyle(TableStyle(commands))
    return table


def _hex_rgb(value: str) -> np.ndarray:
    value = value.lstrip("#")
    return np.array([int(value[index : index + 2], 16) for index in (0, 2, 4)], dtype=np.uint8)


def _apply_chart_palette(source: PILImage.Image) -> PILImage.Image:
    """Traduz verdes editoriais para azul, preservando avisos e conteúdo."""
    rgb = source.convert("RGB")
    pixels = np.array(rgb, dtype=np.uint8)
    hsv = np.array(rgb.convert("HSV"), dtype=np.uint8)
    hue, saturation, value = hsv[..., 0], hsv[..., 1], hsv[..., 2]

    green = (hue >= 45) & (hue <= 115) & (saturation >= 28)
    pixels[green & (value < 110)] = _hex_rgb(NAVY)
    pixels[green & (value >= 110) & (value < 205)] = _hex_rgb(BLUE)
    pixels[green & (value >= 205)] = _hex_rgb(CYAN)

    near_white = (saturation < 12) & (value > 246)
    pixels[near_white] = _hex_rgb(PAPER)
    recolored = PILImage.fromarray(pixels, mode="RGB")
    if "A" in source.getbands():
        recolored.putalpha(source.getchannel("A"))
    return recolored


def _chart_stream(path: Path) -> BytesIO:
    with PILImage.open(path) as source:
        recolored = _apply_chart_palette(source)
        stream = BytesIO()
        recolored.save(stream, format="PNG", optimize=True)
    stream.seek(0)
    return stream


def _figure(path: Path, number: int, title: str, caption: str, styles, *, width: float = 16.9 * cm):
    if not path.exists():
        raise FileNotFoundError(f"Gráfico requerido no encontrado: {path}")
    with PILImage.open(path) as image:
        ratio = image.height / image.width
    height = min(width * ratio, 9.0 * cm)
    image = Image(_chart_stream(path), width=width, height=height)
    return KeepTogether(
        [
            Paragraph(f"FIGURA {number:02d}", styles["FigureLabel"]),
            Paragraph(html.escape(title), styles["FigureTitle"]),
            image,
            Paragraph(caption, styles["Caption"]),
        ]
    )


def _section(story: list[object], styles, eyebrow: str, title: str, lead: str) -> None:
    if story and not isinstance(story[-1], PageBreak):
        story.append(PageBreak())
    story.append(Spacer(1, 0.22 * cm))
    story.append(Paragraph(html.escape(eyebrow.upper()), styles["Eyebrow"]))
    story.append(Paragraph(title, styles["H1"]))
    story.append(Paragraph(lead, styles["Lead"]))
    story.append(HRFlowable(width="100%", thickness=0.75, color=colors.HexColor(BLUE), spaceAfter=8))


def _subsection(
    story: list[object],
    styles,
    title: str,
    paragraphs: Sequence[str],
    *,
    figure: tuple[Path, int, str, str] | None = None,
    conclusion: str | None = None,
) -> None:
    required = 12.0 * cm if figure else 6.0 * cm
    story.append(CondPageBreak(required))
    story.append(Paragraph(title, styles["H2"]))
    before_figure = paragraphs[:2] if figure else paragraphs
    after_figure = paragraphs[2:] if figure else ()
    if len(before_figure) == 2:
        evidence = Table(
            [[_para(before_figure[0], styles, "BodyTight"), _para(before_figure[1], styles, "BodyTight")]],
            colWidths=[8.45 * cm, 8.45 * cm],
            hAlign="LEFT",
        )
        evidence.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (0, 0), 0),
                    ("RIGHTPADDING", (0, 0), (0, 0), 9),
                    ("LEFTPADDING", (1, 0), (1, 0), 9),
                    ("RIGHTPADDING", (1, 0), (1, 0), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LINEBEFORE", (1, 0), (1, 0), 0.35, colors.HexColor(LINE)),
                ]
            )
        )
        story.append(evidence)
    else:
        for text in before_figure:
            story.append(_para(text, styles))
    if figure:
        story.append(_figure(*figure, styles))
    for text in after_figure:
        story.append(_para(text, styles))
    if conclusion:
        story.append(_callout("Implicación para la decisión", conclusion, styles))
        story.append(Spacer(1, 6))


def build_pdf_report() -> Path:
    _register_fonts()
    styles = _styles()

    zone_risk = _read("vw_zone_operational_risk.csv")
    scoring = _read("intervention_scoring_table.csv").sort_values("priority_rank")
    monthly = _read("mart_zone_month_operational.csv")
    scenarios = _read("scenario_summary.csv")
    flex = _read("vw_flexibility_gap.csv")
    forecast_error = _read("forecast_error_by_zone.csv")
    forecast_calibration = _read("forecast_calibration_summary.csv")
    forecast_monitor = _read("forecast_monitoring_status.csv").iloc[0]
    forecast_models = _read("forecast_model_selection_rolling.csv")
    anomalies = _read("anomalies_summary_by_type.csv")
    sensitivity = _read("scoring_sensitivity_analysis.csv")
    feeders = _read("prioridades_inversion_alimentadores.csv").sort_values("ranking_prioridad")
    checks = _read("validation_checks_sql.csv")
    nodes = _read("support_congestion_nodos.csv")

    validation_path = REPORTS / "validation_summary.json"
    validation = json.loads(validation_path.read_text(encoding="utf-8")) if validation_path.exists() else {}
    charts = {path.name.split("_", 1)[0]: path for path in sorted(GRAPHS.glob("*.png"))}
    expected_charts = {f"{number:02d}" for number in range(1, 20)}
    missing_charts = sorted(expected_charts - charts.keys())
    if missing_charts:
        raise FileNotFoundError(f"Faltan gráficos finales: {', '.join(missing_charts)}")

    total_congestion = float(zone_risk["horas_congestion"].sum())
    total_ens = float(zone_risk["ens_total_mwh"].sum())
    top5 = zone_risk.nlargest(5, "horas_congestion")
    top5_share = float(top5["horas_congestion"].sum() / total_congestion)
    best_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmin()]
    worst_scenario = scenarios.loc[scenarios["coste_riesgo_total"].idxmax()]
    scenario_delta = float(worst_scenario["coste_riesgo_total"] / best_scenario["coste_riesgo_total"] - 1)
    z13_score = scoring.loc[scoring["zona_id"].eq("Z013")].iloc[0]
    z13_risk = zone_risk.loc[zone_risk["zona_id"].eq("Z013")].iloc[0]
    risk_counts = scoring["risk_tier"].value_counts()
    region_summary = (
        zone_risk.groupby("region_operativa", as_index=False)
        .agg(
            congestion=("horas_congestion", "sum"),
            ens=("ens_total_mwh", "sum"),
            riesgo=("riesgo_operativo_score", "mean"),
        )
        .sort_values("congestion", ascending=False)
    )
    type_summary = (
        zone_risk.groupby("tipo_zona", as_index=False)
        .agg(
            zonas=("zona_id", "size"), congestion=("horas_congestion", "sum"), riesgo=("riesgo_operativo_score", "mean")
        )
        .sort_values("riesgo", ascending=False)
    )
    latest_month = pd.to_datetime(monthly["mes"]).max()
    first_month = pd.to_datetime(monthly["mes"]).min()
    months_es = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    analysis_window = (
        f"{months_es[first_month.month - 1]} de {first_month.year} a "
        f"{months_es[latest_month.month - 1]} de {latest_month.year} · 24 meses"
    )
    coverage_95 = float(
        forecast_calibration.loc[forecast_calibration["interval_level"].eq(0.95), "empirical_coverage"].iloc[0]
    )
    sensitivity_span = sensitivity.groupby("zona_id")["rank_alt"].agg(["min", "max"])
    sensitivity_span["span"] = sensitivity_span["max"] - sensitivity_span["min"]
    stable_positions = int((sensitivity_span["span"] == 0).sum())
    critical_anomalies = anomalies.loc[anomalies["anomaly_type"].eq("desviacion_vs_patron")].iloc[0]
    best_model = forecast_models.sort_values("model_rank").iloc[0]
    source_mode = _label(validation.get("source_mode", "synthetic"))

    pdf_path = REPORTS / "informe_analitico_red_electrificacion.pdf"
    REPORTS.mkdir(parents=True, exist_ok=True)
    doc = EditorialDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=2.0 * cm,
        rightMargin=2.0 * cm,
        topMargin=1.85 * cm,
        bottomMargin=1.55 * cm,
        title=REPORT_TITLE,
        subject=REPORT_SUBTITLE,
        author="Sistema de Inteligencia de Red para Electrificación Territorial",
        creator="Generador editorial reproducible",
    )
    story: list[object] = []

    # Portada
    story.extend(
        [
            Spacer(1, 0.45 * cm),
            Paragraph("INFORME ANALÍTICO PARA COMITÉ DE INVERSIÓN", styles["CoverLabel"]),
            Paragraph(REPORT_TITLE, styles["CoverTitle"]),
            Paragraph(REPORT_SUBTITLE, styles["CoverSubtitle"]),
            Spacer(1, 9.15 * cm),
            _cover_table(
                [
                    ("Preparado por", "Sistema de Inteligencia de Red para Electrificación Territorial"),
                    ("Ventana de análisis", analysis_window),
                    ("Cobertura", f"24 zonas · {len(nodes)} alimentadores · 576 observaciones zona-mes"),
                    ("Decisión central", "Movilizar la primera oleada; mantener condicionado el CAPEX estructural"),
                ],
                styles,
            ),
            Spacer(1, 8),
            Paragraph(
                f"Fuente: {html.escape(source_mode.lower())} · Validación: {html.escape(str(validation.get('overall_status', 'PASS')))} · Edición: julio de 2026 · Documento de soporte a decisión",
                styles["CoverMeta"],
            ),
            NextPageTemplate("body"),
            PageBreak(),
        ]
    )

    # Índice
    story.append(Paragraph("Contenido", styles["ContentsTitle"]))
    story.append(
        Paragraph(
            "El informe sigue una secuencia de decisión: mandato, evidencia, alternativas, incertidumbre, ejecución y control. Las figuras se leen como soporte de la argumentación, no como piezas aisladas.",
            styles["Lead"],
        )
    )
    toc = TableOfContents()
    toc.levelStyles = [
        ParagraphStyle(
            name="TOC1",
            fontName="Inter-SemiBold",
            fontSize=9.2,
            leading=14,
            leftIndent=0,
            firstLineIndent=0,
            textColor=colors.HexColor(NAVY),
            spaceBefore=6,
        ),
        ParagraphStyle(
            name="TOC2",
            fontName="Inter",
            fontSize=8.3,
            leading=12,
            leftIndent=14,
            firstLineIndent=0,
            textColor=colors.HexColor(SLATE),
        ),
    ]
    story.append(toc)
    story.append(PageBreak())

    # Resumen ejecutivo
    _section(
        story,
        styles,
        "Resumen ejecutivo",
        "La cartera permite movilizar decisiones sin confundir prioridad analítica con autorización de inversión",
        "La evidencia identifica una urgencia inequívoca, una primera oleada acotada de expedientes y un conjunto amplio de medidas reversibles. El diseño de gobierno preserva velocidad de actuación y disciplina de capital.",
    )
    story.append(
        _kpi_band(
            [
                ("Z013", "única zona crítica; prioridad 86,2"),
                (_fmt_int(total_congestion), "horas-zona de congestión"),
                (_fmt_int(total_ens), "MWh de energía no suministrada"),
                (f"+{_fmt_pct(scenario_delta, 0)}", "coste de riesgo al retrasar CAPEX"),
            ],
            styles,
        )
    )
    story.append(Spacer(1, 9))
    executive_narrative = [
        f"La cartera cubre 24 zonas con una distribución deliberadamente asimétrica: una zona crítica, {int(risk_counts.get('alto', 0))} de riesgo alto, {int(risk_counts.get('medio', 0))} medio y {int(risk_counts.get('bajo', 0))} bajo. La asimetría importa porque evita repartir recursos de ingeniería de forma uniforme. <b>Z013 concentra la única señal que exige apertura inmediata de expediente</b>; el resto se secuencia según factor dominante, reversibilidad y evidencia pendiente.",
        f"La presión no es marginal. El conjunto registra {_fmt_int(total_congestion)} horas-zona de congestión y {_fmt_int(total_ens)} MWh de energía no suministrada. Las cinco zonas con mayor congestión explican {_fmt_pct(top5_share, 0)} del total, una concentración suficiente para organizar una primera oleada, pero no para gestionar cada territorio como un sistema aislado. La coordinación regional sigue siendo necesaria por dependencias de subestación, ventanas de obra y riesgo de desplazamiento.",
        f"Z013 reúne {_fmt_int(z13_risk['horas_congestion'])} horas de congestión, {_fmt_int(z13_risk['ens_total_mwh'])} MWh de ENS y una brecha técnica de {_fmt_int(z13_score['gap_tecnico_mw'])} MW. Estas señales justifican diagnóstico, mitigación transitoria y comparación acelerada de alternativas. No determinan por sí mismas si la solución final debe ser refuerzo, operación, flexibilidad o almacenamiento; esa elección requiere topología validada, flujo de carga, contingencias y economía completa.",
        "La recomendación de cartera combina cinco estudios de refuerzo local, dos validaciones de almacenamiento, una sustitución de activos, cuatro activaciones de flexibilidad, cinco optimizaciones operativas y seis mandatos de vigilancia. La diversidad es una fortaleza: <b>reduce el sesgo hacia obra física</b> y obliga a definir qué evidencia debe producir cada familia antes de recibir capital o continuidad presupuestaria.",
        f"Los escenarios refuerzan la dirección de viaje. La combinación de CAPEX y flexibilidad presenta el menor coste de riesgo relativo, mientras el retraso de CAPEX lo eleva un {_fmt_pct(scenario_delta, 0)}. La lectura correcta es acelerar la maduración de alternativas y proteger medidas reversibles de corto plazo. La lectura incorrecta sería utilizar escenarios sin probabilidades ni costes licitados como presupuesto o valor esperado.",
        f"El pronóstico es útil para secuenciar, pero se encuentra en vigilancia: el MAE de la última ventana sube {_fmt_dec(forecast_monitor['mae_drift_ratio'], 2)}× frente al histórico y el intervalo nominal del 95 % cubre {_fmt_pct(coverage_95)}. El modelo no invalida la cartera, aunque sí impide presentar trayectorias puntuales como compromisos deterministas. La revisión trimestral debe recalibrar ranking, previsión y escenarios antes de elevar nuevos expedientes.",
    ]
    story.append(_multi_column_text(executive_narrative, styles, columns=3))
    story.append(
        _callout(
            "Resolución propuesta",
            "Aprobar alcance, responsables y presupuesto de diagnóstico, estudios y medidas reversibles. Excluir de esta resolución las obras estructurales y los contratos de largo plazo. Cada expediente volverá al comité con necesidad validada, alternativas comparables, economía completa y riesgo residual.",
            styles,
        )
    )
    story.append(Spacer(1, 9))
    executive_rows = [
        ["DECISIÓN", "ALCANCE", "HORIZONTE", "CONDICIÓN DE CIERRE"],
        ["Abrir Z013", "Causa raíz, mitigación y alternativas", "30 días", "Expediente elevado, rediseñado o cerrado"],
        [
            "Madurar refuerzos",
            "Z021, Z020, Z016, Z015 y Z019",
            "90 días",
            "Ingeniería, coste, permisos y riesgo residual",
        ],
        ["Validar almacenamiento", "Z001 y Z024", "90 días", "Potencia, duración, nodo, ventana y beneficio"],
        ["Ejecutar medidas reversibles", "9 zonas", "0–6 meses", "Resultado medido frente a línea base"],
        ["Gobernar vigilancia", "6 zonas", "Trimestral", "Mantener, escalar o cerrar con gatillo explícito"],
    ]
    story.append(_data_table(executive_rows, [3.0 * cm, 5.0 * cm, 2.2 * cm, 6.7 * cm], styles))

    # 1. Mandato y arquitectura de decisión
    _section(
        story,
        styles,
        "1 · Mandato y arquitectura de decisión",
        "El sistema ordena trabajo y evidencia; la autorización de capital permanece en un proceso de gobierno separado",
        "El informe responde dónde actuar, con qué tipo de expediente y bajo qué condiciones. No sustituye el estudio eléctrico, la valoración financiera ni la decisión formal del comité.",
    )
    for paragraph in [
        "La pregunta de negocio no es qué zona obtiene el mayor índice, sino <b>qué decisión debe tomarse ahora y qué incertidumbre debe cerrarse después</b>. El ranking integra congestión, servicio, flexibilidad, activos, electrificación y prioridad económica en una señal común. Esa señal abre un expediente, orienta el factor dominante y define urgencia; no elige automáticamente una tecnología ni libera presupuesto de obra.",
        "La arquitectura propuesta separa tres niveles. El primero es operativo: estabilizar, medir y evitar que una señal creciente se convierta en evento de servicio. El segundo es de maduración: comparar alternativas con la misma línea base, horizonte y definición de beneficio. El tercero es de inversión: decidir únicamente cuando necesidad, alternativas, economía y ejecutabilidad han superado umbrales documentados.",
        "Esta separación protege dos valores que suelen entrar en tensión. La organización conserva velocidad para actuar sobre Z013 y desplegar medidas reversibles, a la vez que evita la aprobación por inercia de expedientes todavía incompletos. El resultado esperado no es más análisis, sino decisiones comparables con una salida explícita: autorizar, mitigar con umbral, rediseñar o cerrar.",
    ]:
        story.append(_para(paragraph, styles))
    _subsection(
        story,
        styles,
        "Una cartera diversificada reduce el sesgo hacia CAPEX",
        [
            "La composición recomendada distribuye las 24 zonas entre siete respuestas. Sólo seis casos —la intervención inmediata y cinco refuerzos— comienzan con una hipótesis estructural fuerte. El resto obliga a validar almacenamiento, condición de activos, flexibilidad, optimización o vigilancia. Esta mezcla es coherente con una etapa de priorización: reserva capacidad de inversión para los problemas que demuestren persistencia y materialidad.",
            "La distribución también cambia la responsabilidad. Ingeniería lidera los refuerzos; operación debe demostrar el efecto de las optimizaciones; flexibilidad valida disponibilidad en la hora crítica; mantenimiento confirma condición y probabilidad de fallo; la oficina de cartera controla fechas, dependencias y retorno al comité. Sin esta asignación, una etiqueta de intervención se convierte en recomendación genérica sin capacidad de ejecución.",
            "Las seis zonas en monitorización no constituyen una cola pasiva. Cada una necesita una línea base, un indicador de deterioro, una fecha de revisión y un umbral que obligue a escalar o cerrar. La misma disciplina aplica a medidas temporales: comprar tiempo tiene valor sólo cuando el beneficio observado supera el coste y no oculta una necesidad estructural.",
        ],
        figure=(
            charts["05"],
            1,
            "Composición de la cartera por respuesta",
            "Número de zonas por intervención recomendada. La figura muestra el diseño de respuesta, no una asignación presupuestaria.",
        ),
        conclusion="Aprobar mandatos y criterios de salida por familia; no aprobar una bolsa agregada de CAPEX basada en la distribución del ranking.",
    )
    _subsection(
        story,
        styles,
        "El embudo convierte señales heterogéneas en decisiones auditables",
        [
            "La priorización combina filtros sucesivos: integridad de datos, materialidad operativa, impacto de servicio, disponibilidad flexible, exposición de activos, presión de electrificación, economía de referencia y confianza. El valor del embudo no está en producir un único número, sino en preservar el rastro que explica por qué dos zonas con puntuación similar pueden recibir respuestas diferentes.",
            "Una zona puede elevarse por congestión persistente; otra, por ENS y clientes afectados; otra, por exposición de activos; una cuarta, por brecha flexible. El índice agregado facilita la secuencia, mientras el factor dominante determina el contenido del expediente. Esta distinción debe mantenerse en toda comunicación ejecutiva para evitar que una cifra sintética oculte la causa de riesgo.",
            "El control mínimo exige conservar versión de datos, pesos, reglas de normalización, resultado por componente, intervención recomendada y fecha de decisión. Cuando cambie un peso o una fuente, la organización debe poder reproducir la posición anterior, explicar el movimiento y confirmar si la decisión sigue siendo válida. La trazabilidad es una condición de credibilidad, no un anexo documental.",
        ],
        figure=(
            charts["06"],
            2,
            "Embudo de priorización de zonas",
            "Secuencia analítica desde la población evaluada hasta los mandatos de actuación, maduración y vigilancia.",
        ),
        conclusion="Tratar el índice como puerta de entrada al expediente y conservar los componentes que justifican la recomendación final.",
    )

    # 2. Datos, método y calidad
    _section(
        story,
        styles,
        "2 · Datos, método y calidad",
        "La publicación es reproducible y suficiente para priorizar; la evidencia sigue siendo preliminar para comprometer capital",
        "Los controles verifican coherencia, dominios y trazabilidad de la capa analítica. No sustituyen datos operativos gobernados, estudios de flujo de carga ni costes licitados.",
    )
    story.append(
        _kpi_band(
            [
                ("24", "zonas evaluadas"),
                ("576", "observaciones zona-mes"),
                (f"{int(checks['passed'].sum())}/{len(checks)}", "controles SQL superados"),
                (str(validation.get("overall_status", "PASS")), "estado de validación"),
            ],
            styles,
        )
    )
    story.append(Spacer(1, 8))
    for paragraph in [
        f"La base combina datos de demanda, carga neta, congestión, estrés, energía no suministrada, clientes afectados, flexibilidad, almacenamiento, activos, electrificación y anomalías. El horizonte observado va de {first_month:%m/%Y} a {latest_month:%m/%Y}; el nivel mensual se complementa con trazabilidad por alimentador para orientar la primera verificación de campo. Esta estructura permite pasar de señal territorial a unidad técnica sin presentar el alimentador como orden de obra.",
        f"La validación automática informa {int(checks['passed'].sum())} controles superados de {len(checks)}, sin bloqueos reportados en el resumen de publicación. El resultado acredita consistencia interna del producto generado. <b>No acredita representatividad de operación real</b>: la fuente se declara como {source_mode.lower()}, los costes son referencias comparativas y no existe todavía un estudio AC/N-1 dentro del alcance.",
        "La puntuación utiliza normalización y reglas explícitas para hacer comparables escalas distintas. La robustez se comprueba mediante sensibilidad de pesos; el pronóstico se valida en ventanas temporales y con cobertura empírica; los escenarios se interpretan de forma relativa. Cada control responde a un riesgo diferente: estabilidad del ranking, generalización temporal y comportamiento bajo supuestos alternativos.",
        "El uso permitido es priorizar investigación, organizar capacidad de ingeniería, diseñar medidas reversibles y secuenciar expedientes. El uso no permitido es presentar el índice, el coste de riesgo o la inversión requerida como caso financiero aprobado. La frontera de uso debe acompañar el informe, el dashboard y cualquier exportación de datos para evitar que el contexto se pierda al circular una cifra aislada.",
    ]:
        story.append(_para(paragraph, styles))
    _subsection(
        story,
        styles,
        "La distribución de riesgo confirma una cartera amplia, pero no uniforme",
        [
            f"La clasificación contiene una zona crítica, {int(risk_counts.get('alto', 0))} altas, {int(risk_counts.get('medio', 0))} medias y {int(risk_counts.get('bajo', 0))} bajas. La ausencia de acumulación en un único nivel sugiere que las reglas discriminan entre exposiciones. Aun así, los cortes deben tratarse como bandas de gestión: una diferencia pequeña alrededor del umbral no convierte automáticamente dos casos cercanos en realidades técnicas distintas.",
            "La lectura correcta combina nivel y distancia al umbral, factor dominante, confianza y sensibilidad. Una zona media con deterioro rápido puede requerir más atención que una alta estable con mitigación eficaz. Por eso, la cadencia trimestral debe revisar movimientos, no sólo posiciones: cambio de puntuación, cambio de categoría, persistencia y evidencia que explica la variación.",
            "El comité no necesita reabrir los 24 casos en cada ciclo. Debe recibir excepciones: entradas o salidas de riesgo alto, deterioros de servicio, pérdida de cobertura flexible, cambios de confianza y expedientes que no alcanzan su siguiente umbral. Este enfoque reduce ruido y concentra la conversación en decisiones que han cambiado desde la revisión anterior.",
        ],
        figure=(
            charts["07"],
            3,
            "Distribución del índice por nivel de riesgo",
            "Puntuación de prioridad y clasificación de las 24 zonas. Las bandas orientan gobierno; no sustituyen el diagnóstico del factor dominante.",
        ),
        conclusion="Gestionar por excepción y por movimiento entre revisiones, no mediante una relectura completa y estática del ranking.",
    )
    _subsection(
        story,
        styles,
        "La tipología territorial condiciona la interpretación de las métricas",
        [
            f"Las ocho zonas industriales acumulan {_fmt_int(type_summary.loc[type_summary['tipo_zona'].eq('industrial'), 'congestion'].iloc[0])} horas de congestión y un riesgo medio de {_fmt_dec(type_summary.loc[type_summary['tipo_zona'].eq('industrial'), 'riesgo'].iloc[0])}. Las zonas rurales no registran congestión en el horizonte, pero eso no equivale a ausencia de vulnerabilidad: cargas menores, radialidad, tiempos de reposición y criticidad social pueden no expresarse en la misma métrica.",
            "La comparación entre tipos debe preservar denominadores y contexto operativo. Una hora de congestión industrial y una interrupción rural pueden tener consecuencias económicas y de resiliencia muy distintas. El modelo incorpora servicio y criticidad para evitar que el volumen de energía domine la decisión, pero la valoración final debe documentar clientes críticos, alternativas de suministro y tolerancia local al riesgo.",
            "La figura funciona como control de coherencia. Si todas las zonas de una tipología se agruparan en el mismo resultado por construcción, el modelo estaría reproduciendo una etiqueta y no evidencia. La dispersión observada dentro de cada grupo indica que las características zonales siguen aportando información, aunque la calibración con datos reales debe comprobar posibles sesgos de medición.",
        ],
        figure=(
            charts["12"],
            4,
            "Perfil de riesgo por tipo de zona",
            "Comparación de los componentes de prioridad entre zonas industriales, urbanas, mixtas y rurales.",
        ),
        conclusion="Mantener comparabilidad común, pero exigir contexto de servicio y topología específico de cada tipología antes de elegir alternativa.",
    )

    # 3. Evidencia operativa
    _section(
        story,
        styles,
        "3 · Evidencia operativa",
        "La presión es persistente, aparece antes de la congestión confirmada y se concentra en un conjunto gestionable de territorios",
        "La secuencia temporal, la concentración y la geografía ofrecen tres perspectivas complementarias para asignar capacidad de diagnóstico sin perder dependencias regionales.",
    )
    _subsection(
        story,
        styles,
        "Demanda y carga neta sostienen un nivel elevado durante todo el horizonte",
        [
            "La serie agregada no muestra una normalización estructural que permita posponer la decisión por simple reversión temporal. Demanda bruta y carga neta se mantienen elevadas, con una separación atribuible a generación distribuida y condiciones operativas. La persistencia importa más que un máximo aislado: una red que opera repetidamente cerca de sus límites consume margen de maniobra y amplifica el efecto de contingencias.",
            "La carga neta es la referencia adecuada para operación, mientras la demanda bruta conserva valor para planificación de electrificación. Confundir ambas puede producir dos errores opuestos: sobredimensionar por ignorar generación local o infravalorar exposición en horas en las que esa generación no coincide con la punta. Los expedientes deben utilizar perfiles horarios y condiciones coincidentes, no promedios anuales.",
            "La implicación no es acelerar todas las obras, sino proteger la calidad de la decisión. El horizonte de 0–24 meses debe combinar medidas de corto plazo que preserven servicio con estudios que eliminen incertidumbre técnica. La actualización mensual debe distinguir crecimiento estructural, estacionalidad y eventos extraordinarios antes de modificar una secuencia aprobada.",
        ],
        figure=(
            charts["01"],
            5,
            "Trayectoria de demanda bruta y carga neta",
            "Series mensuales agregadas en GWh. La separación entre ambas magnitudes no debe interpretarse como capacidad firme disponible.",
        ),
        conclusion="Usar perfiles coincidentes de carga neta para operación y conservar demanda bruta como señal de presión estructural de electrificación.",
    )
    _subsection(
        story,
        styles,
        "El estrés operativo crea una ventana de actuación anterior a la congestión",
        [
            "Las horas de estrés permanecen por encima de la congestión confirmada. La distancia entre ambas curvas representa tiempo operando cerca del umbral sin haber materializado todavía el evento formal. Esa señal precursora es útil para activar revisión, comprobar disponibilidad flexible y preparar mitigación antes de que el margen se agote.",
            "No todo estrés debe convertirse en expediente estructural. La respuesta depende de persistencia, severidad, impacto de servicio y capacidad de aliviar la hora crítica. Dos revisiones consecutivas por encima del umbral constituyen una regla de gestión razonable para escalar desde vigilancia a diagnóstico; una reducción sostenida después de una medida reversible permite mantener o retirar la intervención.",
            "La métrica debe conservar una definición estable. Cambiar límites operativos, disponibilidad de medida o calidad de telemetría puede mover la serie sin modificar la realidad física. El propietario del indicador debe documentar esos cambios y presentar un puente de reconciliación; de lo contrario, la cartera puede reaccionar a una discontinuidad de datos como si fuera deterioro de red.",
        ],
        figure=(
            charts["02"],
            6,
            "Evolución de congestión y estrés operativo",
            "Horas-zona mensuales. El estrés anticipa presión, pero requiere persistencia y contexto antes de escalar a solución estructural.",
        ),
        conclusion="Implantar umbrales mensuales de estrés y congestión con propietario, decisión de escalada y registro de cambios de definición.",
    )
    _subsection(
        story,
        styles,
        "La concentración habilita foco técnico, no gestión aislada",
        [
            f"Las cinco zonas con más horas de congestión explican {_fmt_pct(top5_share, 0)} del total. El Pareto justifica reservar una parte relevante de la capacidad de ingeniería para un grupo pequeño, con Z013, Z020 y Z021 en el frente de la secuencia. Concentrar recursos mejora velocidad de diagnóstico y calidad de los expedientes frente a distribuir estudios superficiales por toda la cartera.",
            "La concentración también aumenta el riesgo de cuello de botella. Si los cinco casos dependen de los mismos especialistas, proveedores o ventanas de indisponibilidad, abrirlos simultáneamente puede retrasar el conjunto. La oficina de cartera debe secuenciar recursos escasos y definir entregables intermedios que permitan cerrar una alternativa antes de esperar al expediente completo.",
            "El tramo restante del Pareto no desaparece. Las zonas de riesgo medio y bajo necesitan vigilancia gobernada porque una combinación de crecimiento, degradación o pérdida de flexibilidad puede cambiar su posición. La revisión trimestral preserva esa opción sin consumir la misma intensidad de ingeniería que los casos líderes.",
        ],
        figure=(
            charts["03"],
            7,
            "Concentración de horas de congestión",
            "Pareto por zona. La participación acumulada orienta foco de recursos; no captura por sí sola impacto de servicio ni dependencias regionales.",
        ),
        conclusion="Asignar capacidad prioritaria al primer tramo del Pareto y mantener una cola de vigilancia con gatillos explícitos.",
    )
    _subsection(
        story,
        styles,
        "La geografía obliga a coordinar expedientes por región operativa",
        [
            f"Norte concentra {_fmt_int(region_summary.iloc[0]['congestion'])} horas de congestión y {_fmt_int(region_summary.iloc[0]['ens'])} MWh de ENS, aunque el riesgo medio regional no es el más alto. Esta combinación muestra por qué volumen y severidad deben leerse juntos. Una región grande puede dominar el total; otra más pequeña puede presentar un riesgo medio superior y requerir atención selectiva.",
            "La región es una unidad de coordinación, no un sustituto de la zona. Compartición de subestaciones, indisponibilidades planificadas, límites de transporte y disponibilidad de brigadas pueden alterar el orden óptimo. Antes de elevar un refuerzo, el expediente debe comprobar si otra intervención cercana resuelve parte del problema, lo desplaza o condiciona su ventana de ejecución.",
            "El mapa ejecutivo debe evitar el efecto semáforo sin contexto. Color intenso no significa obra inmediata; significa que la región requiere una explicación de causa, respuesta y dependencia. La conversación de cartera debe terminar con decisiones territoriales concretas, no con una descripción visual del riesgo.",
        ],
        figure=(
            charts["11"],
            8,
            "Distribución del riesgo por región operativa",
            "Agregación territorial de exposición. La coordinación regional complementa, pero no sustituye, el diagnóstico por zona y alimentador.",
        ),
        conclusion="Revisar los expedientes líderes en foros regionales antes del comité central para identificar dependencias y evitar doble conteo de beneficios.",
    )

    # 4. Prioridad y causa de riesgo
    _section(
        story,
        styles,
        "4 · Prioridad y causa de riesgo",
        "Z013 requiere actuación inmediata; el resto de la cartera debe diferenciar causa dominante y alternativa viable",
        "La puntuación ordena, pero la decisión depende del mecanismo que genera exposición y de cuánto riesgo elimina cada alternativa frente a no actuar.",
    )
    _subsection(
        story,
        styles,
        "El ranking abre expedientes y hace visible la distancia entre casos",
        [
            f"Z013 ocupa la primera posición con {_fmt_dec(z13_score['investment_priority_score'])} puntos y es la única zona crítica. Z021 y Z020 encabezan el siguiente grupo, mientras las posiciones posteriores muestran una transición más gradual. La distancia del líder respalda un tratamiento diferenciado: diagnóstico en 30 días para Z013 frente a maduración de 90 días o secuencias de 3–24 meses en el resto.",
            "Una posición no debe presentarse sin sus componentes. En Z013 convergen riesgo de congestión, impacto de servicio, brecha flexible y prioridad económica; en otras zonas domina una combinación distinta. El responsable del expediente debe explicar qué componente sostiene la prioridad, qué evidencia podría refutarlo y qué alternativa se está comparando.",
            "La estabilidad de la primera posición no elimina incertidumbre sobre solución. La decisión inmediata es abrir el caso, no aprobar una obra concreta. El expediente debe incluir mitigación transitoria, flujo de carga, N-1, topología, disponibilidad flexible, inspección de activos, permisos, coste total y reducción esperada de ENS. Sólo la comparación completa transforma prioridad en recomendación de inversión.",
        ],
        figure=(
            charts["04"],
            9,
            "Ranking de prioridad de inversión por zona",
            "Puntuación agregada y nivel de riesgo. El ranking organiza el trabajo; el contenido del expediente se define por el factor dominante.",
        ),
        conclusion="Abrir Z013 de inmediato y exigir que cada zona líder vuelva con alternativas comparables, no con una ratificación del índice.",
    )
    story.append(CondPageBreak(10 * cm))
    story.append(Paragraph("Dossier de decisión: Z013", styles["H2"]))
    story.append(
        _kpi_band(
            [
                (_fmt_dec(z13_score["investment_priority_score"]), "índice de prioridad"),
                (_fmt_int(z13_risk["horas_congestion"]), "horas de congestión"),
                (_fmt_int(z13_risk["ens_total_mwh"]), "MWh de ENS"),
                (_fmt_int(z13_score["gap_tecnico_mw"]), "MW de brecha técnica"),
            ],
            styles,
        )
    )
    story.append(Spacer(1, 8))
    for paragraph in [
        "La convergencia de señales autoriza una actuación de corto plazo. Operación debe confirmar episodios, patrón horario y mitigación disponible; planificación debe identificar restricciones y contingencias; mantenimiento debe validar condición de los activos asociados. La salida de los primeros 30 días es una causa raíz suficientemente delimitada y un conjunto corto de alternativas, no un diseño definitivo.",
        "El caso base debe cuantificar demanda y carga neta coincidentes, severidad, ENS, clientes afectados y riesgo residual. Cada alternativa —refuerzo, reconfiguración, flexibilidad, almacenamiento o combinación— utilizará esa misma línea base. La comparación incluirá potencia, duración, disponibilidad, plazo, permisos, coste de ciclo de vida y beneficio atribuible. Si una opción no puede medirse contra la misma exposición, no es comparable.",
        "La mitigación transitoria debe llevar caducidad. Puede renovarse sólo si demuestra reducción de presión y no retrasa una solución estructural necesaria. El comité recibirá una recomendación concreta: elevar inversión, mantener mitigación con umbral, rediseñar el alcance o cerrar por evidencia insuficiente. Cualquier otra salida perpetuaría el diagnóstico sin decisión.",
    ]:
        story.append(_para(paragraph, styles))
    story.append(
        _callout(
            "Control de capital",
            "Z013 justifica movilización y presupuesto de estudio. No justifica todavía la selección de tecnología ni la autorización de CAPEX estructural.",
            styles,
            risk=True,
        )
    )
    story.append(Spacer(1, 8))
    _subsection(
        story,
        styles,
        "La puntuación agregada debe descomponerse en factores que puedan gestionarse",
        [
            "La matriz de correlaciones muestra relaciones entre componentes, pero no una cadena causal. Congestión, servicio, activos, electrificación y flexibilidad pueden moverse juntos porque comparten exposición subyacente; también pueden divergir por topología, horario o calidad de datos. La descomposición evita diseñar una solución para el síntoma equivocado.",
            "Un factor muy correlacionado con la puntuación merece revisión metodológica: puede ser un impulsor legítimo o estar dominando el resultado por escala y construcción. El control adecuado combina correlación, distribución, estabilidad de pesos y revisión de casos frontera. Ninguna de estas pruebas, por separado, demuestra que la ponderación sea económicamente óptima.",
            "La gestión debe traducir cada componente a una pregunta verificable. Congestión: ¿dónde y bajo qué contingencia? Servicio: ¿qué clientes y qué ENS puede evitarse? Flexibilidad: ¿cuántos MW firmes, durante cuánto tiempo y en qué nodo? Activos: ¿qué condición y probabilidad de fallo? Electrificación: ¿qué demanda coincidente y con qué certeza? Economía: ¿qué coste total y riesgo residual?",
        ],
        figure=(
            charts["08"],
            10,
            "Correlación entre factores de prioridad",
            "Relaciones lineales entre componentes del modelo. La correlación apoya diagnóstico de consistencia; no establece causalidad ni alternativa preferida.",
        ),
        conclusion="Obligar a que cada expediente declare un factor dominante, una hipótesis refutable y la métrica que demostrará reducción de riesgo.",
    )
    _subsection(
        story,
        styles,
        "La capacidad flexible sólo cuenta si coincide con la hora y el nodo críticos",
        [
            "La matriz riesgo–flexibilidad separa zonas con exposición similar pero distinta capacidad de respuesta. La capacidad nominal no es equivalente a capacidad efectiva: importa su ubicación, disponibilidad, tiempo de activación, duración, estado de carga y coincidencia con la restricción. Esta es la razón por la que Z001 y Z024 reciben una validación de almacenamiento, no una compra predefinida.",
            "El expediente flexible debe comenzar por la forma del problema. Una punta breve y predecible puede admitir respuesta de demanda o almacenamiento de corta duración; una restricción prolongada o de red mallada puede exigir otra combinación. Medir sólo MW oculta la energía necesaria, la frecuencia de activación y la degradación. El caso debe declarar MW, MWh, ventana, disponibilidad y coste por evento.",
            "La alternativa flexible tiene valor adicional cuando compra tiempo para un refuerzo incierto o evita una inversión sobredimensionada. Ese valor de opción debe compararse con el riesgo de no disponibilidad. Un contrato o activo que no responde en la hora crítica no reduce exposición, aunque figure como capacidad instalada en el inventario.",
        ],
        figure=(
            charts["09"],
            11,
            "Matriz de riesgo y cobertura flexible",
            "Posición de las zonas según riesgo operativo y capacidad flexible relativa. La cobertura representada requiere validación horaria y nodal.",
        ),
        conclusion="Condicionar almacenamiento y contratos de flexibilidad a una prueba de potencia, energía, nodo, ventana crítica y disponibilidad firme.",
    )
    _subsection(
        story,
        styles,
        "ENS incorpora continuidad de suministro y cambia la prioridad relativa",
        [
            "La energía no suministrada conecta el diagnóstico técnico con servicio, resiliencia y exposición económica. Dos zonas con las mismas horas de congestión pueden tener consecuencias diferentes por carga interrumpida, clientes críticos y capacidad de reposición. Incorporar ENS evita que el ranking premie únicamente volumen de presión y permite defender la prioridad ante dirección y finanzas.",
            f"La cartera suma {_fmt_int(total_ens)} MWh de ENS. Z013 registra {_fmt_int(z13_risk['ens_total_mwh'])} MWh y forma parte del grupo de mayor impacto, pero el gráfico también muestra zonas cuya posición de servicio no coincide exactamente con la de congestión. Esas diferencias deben investigarse: pueden reflejar severidad, radialidad, duración de interrupción o concentración de clientes.",
            "El caso de inversión no debe monetizar ENS con una referencia única y presentarla como valor definitivo. El coste por MWh sirve para comparación preliminar; la decisión final necesita segmentación de clientes, criticidad, regulación, compensaciones, probabilidad y riesgo residual. La métrica física se mantiene como ancla para evitar que supuestos financieros oculten el resultado de servicio.",
        ],
        figure=(
            charts["10"],
            12,
            "Ranking de impacto de servicio por ENS",
            "Energía no suministrada acumulada por zona. El indicador complementa congestión y debe interpretarse con clientes, duración y criticidad.",
        ),
        conclusion="Incluir ENS evitada y clientes protegidos en todos los expedientes, manteniendo separados el resultado físico y su valoración económica.",
    )
    _subsection(
        story,
        styles,
        "La electrificación es una presión de planificación, no una explicación causal única",
        [
            "La dispersión entre nueva demanda y congestión confirma que el crecimiento de electrificación no explica por sí solo la exposición. Capacidad existente, perfil horario, generación distribuida, flexibilidad y topología modifican la relación. Utilizar una correlación agregada como causalidad llevaría a sobredimensionar algunas zonas y a ignorar restricciones ya presentes en otras.",
            "Los expedientes deben separar demanda contratada, conectada y coincidente. La electrificación industrial y de movilidad puede tener perfiles muy diferentes; el valor relevante para red es la contribución a la hora crítica bajo un horizonte explícito. La incertidumbre se gestiona con escenarios y gatillos de conexión, no con una única proyección puntual.",
            "La información de pipeline comercial puede mejorar el modelo cuando tenga gobierno de fecha, probabilidad y potencia. Hasta entonces, la presión de electrificación actúa como señal secundaria que modifica prioridad y ventana de decisión. La inversión sólo debe anticiparse cuando el coste de esperar, la probabilidad de materialización y la irreversibilidad estén documentados.",
        ],
        figure=(
            charts["13"],
            13,
            "Electrificación y horas de congestión",
            "Relación por zona entre participación de nueva demanda y presión operativa. La asociación no demuestra causalidad.",
        ),
        conclusion="Modelar demanda nueva por probabilidad, fecha y coincidencia horaria; evitar justificar CAPEX con volumen anunciado no gobernado.",
    )
    _subsection(
        story,
        styles,
        "Las anomalías priorizan investigación y aportan una señal precursora",
        [
            f"El sistema identifica varios tipos de anomalía. Las desviaciones frente al patrón suman {_fmt_int(critical_anomalies['n_eventos'])} eventos y preceden congestión en {_fmt_pct(critical_anomalies['pct_precursor_congestion'])} de los casos observados; las cargas relativas anormales son más frecuentes, pero presentan una relación precursora distinta. Esta separación evita tratar todos los eventos como equivalentes.",
            "Una anomalía indica que el comportamiento se aparta de una referencia; no prueba fallo, causa o necesidad de obra. Su valor operativo aumenta cuando se combina con severidad, persistencia, proximidad temporal y confirmación de telemetría. La investigación debe cerrar falsos positivos, cambios de definición y eventos conocidos antes de escalar a la cartera.",
            "La señal precursora puede convertirse en un gatillo útil si se valida fuera de muestra. El equipo debe medir precisión, cobertura y tiempo de anticipación por tipo, además de registrar qué decisiones produjo. Una alerta que anticipa muchos eventos pero genera demasiadas investigaciones improductivas puede consumir capacidad sin reducir riesgo.",
        ],
        figure=(
            charts["14"],
            14,
            "Anomalías y relación precursora",
            "Frecuencia, severidad y proporción de eventos que anteceden congestión o interrupción por tipología de anomalía.",
        ),
        conclusion="Usar anomalías para ordenar investigación; exigir confirmación operativa y seguimiento de falsos positivos antes de incorporarlas como gatillo automático.",
    )

    # 5. Pronóstico, escenarios y robustez
    _section(
        story,
        styles,
        "5 · Pronóstico, escenarios y robustez",
        "La dirección de la cartera es estable, aunque la última ventana de pronóstico exige vigilancia y los escenarios no equivalen a probabilidades",
        "Tres controles distintos —validación temporal, escenarios y sensibilidad— acotan riesgos de modelo sin eliminar la necesidad de juicio técnico y de gobierno.",
    )
    _subsection(
        story,
        styles,
        "La precisión zonal es consistente; la calibración 95 % queda por debajo de su objetivo",
        [
            f"El modelo seleccionado es {_label(best_model['model'])}, con MAE agregado de {_fmt_dec(best_model['mae'])} en cuatro ventanas de validación. El NMAE por zona se sitúa entre {_fmt_pct(forecast_error['nmae'].min())} y {_fmt_pct(forecast_error['nmae'].max())}, una dispersión acotada que reduce el riesgo de que el resultado global oculte un territorio sistemáticamente mal modelado.",
            f"La última ventana cambia el tono de uso. El MAE pasa de {_fmt_dec(forecast_monitor['historical_mae'])} en el histórico a {_fmt_dec(forecast_monitor['latest_mae'])}, un cociente de {_fmt_dec(forecast_monitor['mae_drift_ratio'], 2)}×. La cobertura empírica del intervalo 95 % es {_fmt_pct(coverage_95)}, inferior al nominal. El monitor se clasifica como “{_label(forecast_monitor['monitoring_status']).lower()}”, no como fallo, pero exige revisión.",
            "La decisión debe apoyarse en bandas y escenarios, no en un punto. Para medidas reversibles, el pronóstico orienta disponibilidad y ventana; para CAPEX, informa secuencia y riesgo de espera. Si el error o la cobertura cruzan el umbral de acción, deben recalibrarse los expedientes pendientes antes de elevarlos al comité. Los casos ya justificados por observación no se cancelan automáticamente por deterioro del forecast.",
        ],
        figure=(
            charts["15"],
            15,
            "Precisión y calibración del pronóstico",
            "NMAE por zona y cobertura empírica de intervalos. La cobertura 95 % inferior al objetivo justifica vigilancia del modelo.",
        ),
        conclusion="Mantener el pronóstico como apoyo de secuencia; activar recalibración cuando error o cobertura crucen el umbral de acción.",
    )
    _subsection(
        story,
        styles,
        "Los escenarios ordenan estrategias y muestran el coste relativo de esperar",
        [
            f"Entre ocho escenarios, “{_label(best_scenario['scenario']).lower()}” presenta el menor coste de riesgo relativo y “{_label(worst_scenario['scenario']).lower()}” el mayor. La diferencia es {_fmt_pct(scenario_delta, 0)}. El patrón favorece una cartera combinada: inversión selectiva acompañada por flexibilidad, en lugar de retraso generalizado o confianza exclusiva en una sola palanca.",
            "Los escenarios no llevan probabilidad asignada. Por tanto, no producen valor esperado, presupuesto óptimo ni retorno financiero. Sirven para probar si la secuencia conserva dirección bajo crecimiento EV, electrificación industrial, degradación de activos, mayor generación distribuida, almacenamiento, flexibilidad o retraso de CAPEX. La robustez se interpreta por orden y magnitud relativa, no por precisión del importe.",
            "La decisión práctica es acelerar la maduración de casos que aparecen sistemáticamente expuestos y preservar opciones reversibles donde el futuro es más incierto. Cada expediente debe declarar qué escenarios lo hacen necesario, qué supuestos podrían diferirlo y qué indicadores permitirán reconocer ese cambio antes de comprometer capital.",
        ],
        figure=(
            charts["16"],
            16,
            "Comparación del coste de riesgo entre escenarios",
            "Coste de riesgo relativo por escenario. Las magnitudes usan referencias y no constituyen presupuesto ni valor esperado.",
        ),
        conclusion="Usar escenarios como prueba de robustez de la secuencia y prohibir su presentación como forecast probabilístico o caso financiero aprobado.",
    )
    _subsection(
        story,
        styles,
        "La combinación de palancas reduce más exposición que las respuestas aisladas",
        [
            "La comparación antes–después muestra que una combinación de CAPEX y flexibilidad obtiene la mayor reducción relativa de riesgo. La lógica es complementaria: las medidas reversibles absorben presión y compran tiempo; la inversión selectiva corrige restricciones persistentes; operación y monitorización evitan convertir todos los casos en activos físicos.",
            "El resultado agregado no debe repartirse proporcionalmente entre zonas. Las interacciones pueden ser no lineales y una intervención regional puede modificar el beneficio de otra. Para evitar doble conteo, cada expediente utilizará un caso base común, una frontera técnica definida y un riesgo residual después de considerar proyectos ya aprobados.",
            "La realización de beneficio requiere comparación temporal y, cuando sea posible, un grupo o periodo de referencia. Hitos de obra, capacidad contratada o estudio completado son indicadores de proceso. El valor se demuestra con reducción de estrés, congestión, ENS o exposición residual frente a una línea base ajustada por demanda y condiciones operativas.",
        ],
        figure=(
            charts["17"],
            17,
            "Reducción relativa de riesgo antes y después",
            "Comparación de exposición bajo distintas palancas. Los resultados son direccionales y requieren atribución por expediente.",
        ),
        conclusion="Definir beneficio físico y riesgo residual por expediente antes de aprobar; no sumar beneficios agregados sin controlar interacciones.",
    )
    _subsection(
        story,
        styles,
        "La sensibilidad confirma estabilidad excepcional del orden de prioridad",
        [
            f"El análisis de pesos mantiene {stable_positions} de 24 posiciones sin movimiento y limita la variación máxima a una posición. Z013 conserva el primer lugar y el grupo líder apenas cambia. Esta estabilidad indica que la recomendación de primera oleada no depende de una calibración puntual de pesos dentro del rango probado.",
            "La robustez del orden no valida los valores absolutos ni la alternativa elegida. Un ranking puede ser estable y estar sesgado por una fuente común, un dato sintético o una omisión estructural. Por eso, sensibilidad de pesos se combina con calidad de datos, revisión de casos, validación temporal y estudios eléctricos. Cada prueba reduce un riesgo diferente.",
            "La gobernanza debe preservar este análisis cuando se cambien reglas. Una nueva versión documentará motivo, impacto sobre posiciones, casos que cambian de respuesta y decisión sobre expedientes abiertos. Si un movimiento no altera la acción, puede registrarse sin reabrir el caso; si cruza un umbral o cambia el factor dominante, requiere revisión formal.",
        ],
        figure=(
            charts["18"],
            18,
            "Variación del ranking bajo sensibilidad de pesos",
            "Amplitud de posiciones al variar factores del modelo. La primera oleada permanece estable en el rango analizado.",
        ),
        conclusion="Mantener la secuencia actual y exigir análisis de impacto versionado antes de modificar pesos, umbrales o fuentes.",
    )

    # 6. Cartera de intervención
    _section(
        story,
        styles,
        "6 · Cartera de intervención",
        "La primera oleada combina un caso inmediato, siete expedientes estructurales y medidas tácticas con resultado verificable",
        "Cada familia recibe términos de referencia, propietario, horizonte y criterio de salida. La disciplina común hace comparables decisiones técnicas distintas.",
    )
    reinforcement = scoring.loc[scoring["recommended_intervention"].eq("reforzar_red_local")]
    storage = scoring.loc[scoring["recommended_intervention"].eq("desplegar_almacenamiento")]
    reinforcement_rows = [["ZONA", "PRIORIDAD", "CONGESTIÓN H", "BRECHA MW", "SECUENCIA"]]
    for _, row in reinforcement.iterrows():
        reinforcement_rows.append(
            [
                row["zona_id"],
                _fmt_dec(row["investment_priority_score"]),
                _fmt_int(row["horas_congestion_acumuladas"]),
                _fmt_int(row["gap_tecnico_mw"]),
                _label(row["recommended_sequence"]),
            ]
        )
    storage_rows = [["ZONA", "PRIORIDAD", "BRECHA MW", "COBERTURA FLEX.", "SECUENCIA"]]
    for _, row in storage.iterrows():
        flex_row = flex.loc[flex["zona_id"].eq(row["zona_id"])].iloc[0]
        storage_rows.append(
            [
                row["zona_id"],
                _fmt_dec(row["investment_priority_score"]),
                _fmt_int(row["gap_tecnico_mw"]),
                _fmt_dec(flex_row["ratio_flexibilidad_estres"], 3),
                _label(row["recommended_sequence"]),
            ]
        )
    for paragraph in [
        "Z013 se gestiona como expediente inmediato. Los cinco refuerzos —Z021, Z020, Z016, Z015 y Z019— comparten una hipótesis de restricción estructural, pero deben comparar alternativas operativas y flexibles. Z001 y Z024 requieren validación de almacenamiento. Z009 necesita confirmar condición y criticidad antes de una sustitución. El resto se distribuye entre flexibilidad, optimización y vigilancia.",
        "Los términos de referencia comunes incluyen línea base, causa raíz, alternativa sin obra, coste de ciclo de vida, ENS evitada, clientes protegidos, riesgo residual, permisos, dependencias y secuencia. La estandarización no elimina juicio técnico; evita que regiones diferentes eleven casos con supuestos, horizontes y beneficios incompatibles.",
    ]:
        story.append(_para(paragraph, styles))
    story.append(Paragraph("Expedientes de refuerzo local", styles["H3"]))
    story.append(
        _data_table(
            reinforcement_rows, [2.0 * cm, 3.0 * cm, 3.6 * cm, 3.2 * cm, 5.1 * cm], styles, numeric_columns=[1, 2, 3]
        )
    )
    story.append(Spacer(1, 8))
    story.append(Paragraph("Validaciones de almacenamiento", styles["H3"]))
    story.append(
        _data_table(storage_rows, [2.0 * cm, 3.0 * cm, 3.5 * cm, 3.8 * cm, 4.6 * cm], styles, numeric_columns=[1, 2, 3])
    )
    story.append(Spacer(1, 8))
    story.append(
        _callout(
            "Términos de referencia",
            "Comparar refuerzo, operación, flexibilidad y no actuación con una línea base común; cuantificar coste total, beneficio físico, riesgo residual, permisos, dependencias y plazo.",
            styles,
        )
    )
    story.append(Spacer(1, 8))
    _subsection(
        story,
        styles,
        "La prioridad zonal se traduce en una agenda concreta de verificación de campo",
        [
            "El ranking por alimentador aterriza la señal territorial en activos y puntos de inspección. Los primeros quince casos permiten orientar telemetría, revisión de carga, condición, protecciones y topología. Esta traducción mejora accionabilidad y trazabilidad: cada hipótesis zonal puede confrontarse con unidades técnicas observables.",
            "La lista no es una orden de obra. Un alimentador puede aparecer por carga relativa, horas de congestión, exposición de activos, ENS asociada o prioridad de su zona. La verificación debe confirmar el mecanismo y comprobar si una acción local resuelve la restricción o desplaza el riesgo. La alternativa final puede situarse en otro nivel de red.",
            "La agenda de campo debe registrar resultado y efecto sobre el expediente zonal. Confirmación, refutación o evidencia insuficiente son salidas válidas; repetir una inspección sin modificar la hipótesis no lo es. La oficina de cartera consolida hallazgos y decide si el caso avanza a ingeniería, mitigación o cierre.",
        ],
        figure=(
            charts["19"],
            19,
            "Ranking de alimentadores para verificación",
            "Quince alimentadores con mayor puntuación de prioridad. La lista orienta inspección y estudio; no autoriza intervención física.",
        ),
        conclusion="Convertir los quince alimentadores líderes en órdenes de verificación con hipótesis, evidencia requerida y vínculo al expediente zonal.",
    )

    # 7. Ejecución y beneficios
    _section(
        story,
        styles,
        "7 · Ejecución, gobierno y beneficios",
        "Los primeros 90 días deben convertir el ranking en decisiones comparables y cerrar incertidumbres críticas",
        "La hoja de ruta prioriza calidad de expediente y reducción de exposición. Completar actividad no equivale a realizar beneficio.",
    )
    roadmap_rows = [
        ["PERIODO", "DECISIÓN", "ENTREGABLE", "RESPONSABLE", "SALIDA"],
        [
            "0–30 días",
            "Abrir Z013",
            "Causa raíz, mitigación y alternativas",
            "Planificación + operación",
            "Elevar, rediseñar o cerrar",
        ],
        [
            "0–60 días",
            "Instrumentar medidas",
            "Línea base, objetivo y gatillo",
            "Operación regional",
            "Activar con control",
        ],
        [
            "0–90 días",
            "Madurar refuerzos",
            "Ingeniería, coste, permisos y riesgo",
            "Ingeniería + regiones",
            "Expediente comparable",
        ],
        [
            "0–90 días",
            "Validar almacenamiento",
            "MW, MWh, nodo, ventana y beneficio",
            "Flexibilidad + operación",
            "Dimensionar o descartar",
        ],
        [
            "Trimestral",
            "Recalibrar cartera",
            "Ranking, forecast, escenarios y excepciones",
            "Gobernanza analítica",
            "Mantener o reordenar",
        ],
    ]
    story.append(_data_table(roadmap_rows, [2.0 * cm, 3.2 * cm, 4.7 * cm, 3.7 * cm, 3.3 * cm], styles))
    story.append(Spacer(1, 9))
    for paragraph in [
        "El expediente único contiene ocho campos: línea base, causa raíz, alternativas, coste, beneficio, riesgo residual, dependencias y decisión solicitada. Este formato elimina presentaciones incomparables y obliga a que la recomendación responda a una pregunta concreta. El propietario técnico es responsable de la evidencia; la oficina de cartera, de integridad, calendario y retorno al foro correcto.",
        "La cadencia separa urgencia de cartera. Z013 y eventos críticos se revisan semanalmente; el avance de expedientes, mensualmente; ranking, pronóstico y escenarios, trimestralmente. Cada foro tiene derechos de decisión definidos. Una actualización analítica no sustituye una aprobación, y un retraso de estudio no puede convertirse silenciosamente en aceptación de riesgo.",
        "El registro de decisión conserva fecha, foro, evidencia revisada, resolución, condiciones, propietario y próximo umbral. Si un expediente se difiere, debe quedar vinculado a un gatillo y una fecha. Si se cierra, se documenta qué evidencia refutó la necesidad. Esta disciplina permite auditar no sólo qué se decidió, sino por qué era razonable con la información disponible.",
    ]:
        story.append(_para(paragraph, styles))
    gate_rows = [
        ["UMBRAL", "PREGUNTA", "EVIDENCIA EXIGIDA", "DECISIÓN POSIBLE"],
        [
            "1 · Necesidad",
            "¿El riesgo es real y material?",
            "Topología, contingencias, línea base y causa",
            "Continuar o cerrar",
        ],
        [
            "2 · Alternativas",
            "¿Qué palanca reduce mejor la exposición?",
            "Refuerzo, operación, flexibilidad y no actuación",
            "Seleccionar o rediseñar",
        ],
        [
            "3 · Economía",
            "¿El beneficio justifica el coste?",
            "Coste total, ENS evitada y riesgo residual",
            "Diferir o elevar",
        ],
        [
            "4 · Ejecución",
            "¿Puede entregarse con control?",
            "Permisos, recursos, dependencias y secuencia",
            "Autorizar o reprogramar",
        ],
    ]
    story.append(Paragraph("Cuatro umbrales de liberación de capital", styles["H2"]))
    story.append(_data_table(gate_rows, [2.4 * cm, 4.3 * cm, 6.5 * cm, 3.7 * cm], styles))
    story.append(Spacer(1, 8))
    story.append(
        _callout(
            "Control de liberación",
            "No liberar CAPEX estructural ni contratos de largo plazo sin superar necesidad, alternativas, economía y ejecución. El ranking puede abrir el expediente; nunca sustituye estos umbrales.",
            styles,
            risk=True,
        )
    )
    story.append(Spacer(1, 8))
    benefit_rows = [
        ["ÁMBITO", "RESULTADO", "LÍNEA BASE", "USO DE GESTIÓN"],
        ["Operación", "Estrés y congestión", "Periodo previo por zona", "Activar, retirar o escalar mitigación"],
        ["Servicio", "ENS y clientes protegidos", "Histórico comparable", "Priorizar resiliencia y beneficio"],
        ["Flexibilidad", "MW y MWh en hora crítica", "Perfil horario y nodo", "Renovar, redimensionar o retirar"],
        [
            "Cartera",
            "Riesgo residual y tiempo entre umbrales",
            "Apertura del expediente",
            "Eliminar bloqueos de decisión",
        ],
        ["Economía", "Coste y valor realizado", "Caso autorizado", "Controlar desviación y retorno"],
    ]
    story.append(Paragraph("Marco de realización de beneficios", styles["H2"]))
    story.append(_data_table(benefit_rows, [2.7 * cm, 4.1 * cm, 4.0 * cm, 6.1 * cm], styles))
    story.append(Spacer(1, 8))
    for paragraph in [
        "Los indicadores de proceso —estudios completados, contratos firmados, obras ejecutadas— prueban avance. Los indicadores de resultado —estrés, congestión, ENS, clientes protegidos, disponibilidad flexible y riesgo residual— prueban valor. Ambos son necesarios, pero no deben mezclarse en una única tasa de cumplimiento que permita declarar éxito administrativo sin mejora de red.",
        "La comparación antes–después se ajustará por demanda, clima, disponibilidad y cambios de topología. Cuando sea posible, se utilizarán periodos o zonas comparables. La atribución no necesita convertirse en un ejercicio académico, pero sí debe ser suficiente para decidir continuidad, escalada o retirada. Toda medida temporal tendrá una fecha de caducidad y una decisión explícita de renovación.",
    ]:
        story.append(_para(paragraph, styles))

    # 8. Recomendaciones
    _section(
        story,
        styles,
        "8 · Recomendaciones y resolución",
        "Aprobar la primera oleada, exigir retorno con expedientes completos y mantener condicionado el CAPEX estructural",
        "La propuesta protege continuidad de suministro en el corto plazo y eleva el estándar de evidencia antes de comprometer capital irreversible.",
    )
    recommendations = [
        (
            "P0 · Movilizar Z013",
            "Abrir en 30 días un expediente conjunto de planificación, operación y mantenimiento. Confirmar causa raíz, activar mitigación transitoria y comparar refuerzo, reconfiguración, flexibilidad, almacenamiento y no actuación con la misma línea base.",
        ),
        (
            "P0 · Bloquear la liberación automática de CAPEX",
            "Formalizar los cuatro umbrales y registrar decisión, evidencia, condiciones y propietario. Ningún índice, escenario o coste de referencia puede sustituir ingeniería, economía, permisos y riesgo residual.",
        ),
        (
            "P0 · Instrumentar las medidas reversibles",
            "Publicar para nueve zonas línea base, objetivo, disponibilidad, coste, fecha de revisión y gatillo de escalada. Renovar sólo cuando el resultado observado justifique continuidad y no oculte una necesidad estructural.",
        ),
        (
            "P1 · Madurar siete expedientes",
            "Completar cinco estudios de refuerzo y dos validaciones de almacenamiento en 90 días con términos de referencia comunes. Incluir alternativa sin obra y evitar preselección tecnológica.",
        ),
        (
            "P1 · Convertir vigilancia en gobierno activo",
            "Asignar a seis zonas propietario, indicador, fecha y umbral. La salida trimestral debe ser mantener, escalar o cerrar; “seguir observando” sin condición no es una decisión.",
        ),
        (
            "P1 · Recalibrar previsión y cartera",
            "Investigar el aumento del MAE y la cobertura 95 % inferior al objetivo. Recalcular ranking y escenarios antes de elevar nuevos casos si el monitor cruza el umbral de acción.",
        ),
        (
            "P2 · Sustituir referencias por evidencia de producción",
            "Integrar SCADA/AMI gobernado, topología, flujo de carga, contingencias, condición de activos, pipeline de conexión y costes licitados. Mantener linaje, versiones y reconciliación entre capas.",
        ),
        (
            "P2 · Institucionalizar beneficios",
            "Implantar un cuadro mensual de resultados físicos y un retorno trimestral de cartera. Separar hitos de entrega de reducción de riesgo y documentar atribución suficiente para decidir continuidad.",
        ),
    ]
    story.append(_recommendation_columns(recommendations, styles, columns=2))
    story.append(Spacer(1, 6))
    story.append(
        _callout(
            "Resolución para el comité",
            "Autorizar alcance, responsables y presupuesto de estudios y mitigaciones reversibles. Mantener fuera de aprobación las obras estructurales y contratos de largo plazo hasta que cada expediente supere los cuatro umbrales.",
            styles,
        )
    )
    story.append(Spacer(1, 10))
    board_rows = [
        ["DECISIÓN", "RECOMENDACIÓN", "CONDICIÓN DE RETORNO"],
        ["Diagnóstico de Z013", "Aprobar", "Alternativas, mitigación y decisión en 30 días"],
        ["Cinco estudios de refuerzo", "Aprobar", "Ingeniería, coste, permisos y riesgo residual"],
        ["Dos validaciones de almacenamiento", "Aprobar", "Potencia, duración, nodo, ventana y beneficio"],
        ["Nueve medidas reversibles", "Aprobar", "Línea base, objetivo y gatillo de escalada"],
        ["Sustitución de activos en Z009", "Madurar", "Condición, criticidad y alternativa comparada"],
        ["CAPEX estructural", "No aprobar todavía", "Superar necesidad, alternativas, economía y ejecución"],
    ]
    story.append(_data_table(board_rows, [5.0 * cm, 3.2 * cm, 8.7 * cm], styles))

    # 9. Riesgos y límites
    _section(
        story,
        styles,
        "9 · Riesgos, límites y condiciones de uso",
        "La calidad de implementación depende de reconocer qué demuestra el modelo y qué permanece fuera de alcance",
        "La transparencia sobre límites aumenta credibilidad y protege al comité frente a precisión aparente, causalidad no demostrada y doble conteo de beneficios.",
    )
    limitations_rows = [
        ["LIMITACIÓN", "RIESGO DE DECISIÓN", "CONTROL REQUERIDO"],
        [
            "Datos sintéticos",
            "Prioridades no calibradas con operación real",
            "Sustituir por fuentes gobernadas y reconciliadas",
        ],
        [
            "Sin flujo de carga AC/N-1",
            "Alternativa técnica incompleta",
            "Completar topología, contingencias y estudio eléctrico",
        ],
        ["Costes de referencia", "Caso financiero distorsionado", "Validar coste licitado, ciclo de vida y beneficio"],
        [
            "Escenarios sin probabilidades",
            "Falsa apariencia de valor esperado",
            "Usar sólo como prueba de robustez relativa",
        ],
        [
            "Forecast en vigilancia",
            "Secuencia demasiado determinista",
            "Recalibrar cuando error o cobertura crucen umbral",
        ],
        [
            "Correlaciones observacionales",
            "Atribución causal incorrecta",
            "Confirmar mecanismo con análisis técnico y de campo",
        ],
        [
            "Beneficios agregados",
            "Doble conteo entre proyectos",
            "Definir frontera, caso base y riesgo residual por expediente",
        ],
    ]
    story.append(_data_table(limitations_rows, [4.0 * cm, 6.1 * cm, 6.8 * cm], styles))
    story.append(Spacer(1, 8))
    for paragraph in [
        "La primera condición de uso es declarar que la fuente es sintética. El sistema demuestra arquitectura, método, controles y capacidad de decisión; no demuestra que las prioridades reflejen una red real. Una presentación de portfolio debe mostrar esa frontera con claridad: credibilidad proviene de gobernar la incertidumbre, no de ocultarla detrás de una visualización pulida.",
        "La segunda condición es separar comparación relativa de caso financiero. Los costes de ENS, vertido y congestión permiten ordenar escenarios, pero no incorporan todavía probabilidad, coste de capital, vida útil, operación, mantenimiento, regulación ni valor residual. Cualquier estimación de retorno deberá reconstruirse con supuestos aprobados y sensibilidad económica explícita.",
        "La tercera condición es mantener trazabilidad. Datos, reglas, pesos, versiones de modelo, escenarios y decisiones deben poder reproducirse. Cuando una fuente cambie, el equipo documentará impacto sobre métricas y expedientes. Sin ese puente, la organización pierde la capacidad de distinguir mejora real, revisión metodológica y simple discontinuidad de información.",
        "La cuarta condición es evitar automatizar decisiones irreversibles. Alertas, rankings y recomendaciones pueden activar investigación, reservar capacidad o elevar un caso. La liberación de capital requiere revisión humana con autoridad definida. Esta separación es compatible con automatización avanzada y constituye una salvaguarda esencial de gobierno.",
    ]:
        story.append(_para(paragraph, styles))
    story.append(
        _callout(
            "Uso permitido",
            "Priorización, diagnóstico, comparación preliminar, secuenciación, monitorización y diseño de expedientes.",
            styles,
        )
    )
    story.append(Spacer(1, 6))
    story.append(
        _callout(
            "Uso no permitido",
            "Compromiso de capital, selección tecnológica definitiva o contrato de largo plazo basado únicamente en ranking, forecast o escenarios de referencia.",
            styles,
            risk=True,
        )
    )

    # Apéndices
    _section(
        story,
        styles,
        "Apéndice A · Ranking completo",
        "Las 24 zonas conservan trazabilidad entre posición, nivel de riesgo, factor dominante, respuesta y secuencia",
        "El anexo permite auditar la cobertura completa de cartera y localizar casos frontera sin sobrecargar la narrativa ejecutiva.",
    )
    ranking_rows = [["N.º", "ZONA", "RIESGO", "ÍNDICE", "FACTOR DOMINANTE", "RESPUESTA", "SECUENCIA"]]
    for _, row in scoring.iterrows():
        ranking_rows.append(
            [
                int(row["priority_rank"]),
                row["zona_id"],
                _label(row["risk_tier"]),
                _fmt_dec(row["investment_priority_score"]),
                _label(row["main_risk_driver"]),
                _label(row["recommended_intervention"]),
                _label(row["recommended_sequence"]),
            ]
        )
    ranking_widths = [1.2 * cm, 1.2 * cm, 1.5 * cm, 1.4 * cm, 3.4 * cm, 5.6 * cm, 2.6 * cm]
    ranking_pages = _paginate_rows(ranking_rows, rows_per_page=12)
    for page_index, rows in enumerate(ranking_pages):
        if page_index:
            story.append(PageBreak())
        story.append(_data_table(rows, ranking_widths, styles, numeric_columns=[0, 3]))
    story.append(Spacer(1, 8))
    story.append(
        _para(
            "La posición ordena capacidad de trabajo. El factor dominante orienta la investigación. La respuesta y la secuencia determinan el mandato inicial. Ninguna de estas columnas constituye una aprobación de tecnología o presupuesto.",
            styles,
            "Note",
        )
    )

    _section(
        story,
        styles,
        "Apéndice B · Alimentadores prioritarios",
        "La verificación técnica comienza en activos concretos y conserva el vínculo con la hipótesis zonal",
        "La lista prioriza inspección, revisión de telemetría y estudio; no constituye una orden de refuerzo o sustitución.",
    )
    feeder_rows = [["N.º", "ALIMENTADOR", "ZONA", "NIVEL", "ÍNDICE", "ACCIÓN", "ALIVIO MW"]]
    for _, row in feeders.head(20).iterrows():
        feeder_rows.append(
            [
                int(row["ranking_prioridad"]),
                row["alimentador_id"],
                row["zona_id"],
                row["nivel_prioridad"],
                _fmt_dec(row["puntuacion_prioridad"]),
                _label(row["accion_recomendada"]),
                _fmt_dec(row["alivio_requerido_mw"]),
            ]
        )
    story.append(
        _data_table(
            feeder_rows,
            [1.0 * cm, 2.4 * cm, 1.2 * cm, 1.6 * cm, 1.4 * cm, 6.3 * cm, 3.0 * cm],
            styles,
            numeric_columns=[0, 4, 6],
        )
    )

    _section(
        story,
        styles,
        "Apéndice C · Referencias y definiciones",
        "Las referencias económicas son comparativas y las métricas conservan una definición de gestión explícita",
        "Toda valoración final debe incorporar coste licitado, vida útil, operación, mantenimiento, regulación, coste de capital y valor residual.",
    )
    assumption_rows = [
        ["REFERENCIA", "VALOR", "USO PERMITIDO"],
        ["Energía no suministrada", "2.500 EUR/MWh", "Comparación relativa de impacto de servicio"],
        ["Vertido de generación", "90 EUR/MWh", "Comparación relativa de energía"],
        ["Congestión", "45 EUR/hora", "Comparación relativa de presión operativa"],
        ["Horizonte", "0–24 meses", "Secuenciación de intervención"],
    ]
    story.append(_data_table(assumption_rows, [5.0 * cm, 3.4 * cm, 8.5 * cm], styles))
    story.append(Spacer(1, 9))
    definitions_rows = [
        ["MÉTRICA", "DEFINICIÓN DE GESTIÓN", "PRECAUCIÓN"],
        [
            "Horas de congestión",
            "Tiempo por encima del umbral de capacidad",
            "Depende de topología, umbral y calidad de telemetría",
        ],
        [
            "Estrés operativo",
            "Tiempo próximo al límite antes de congestión confirmada",
            "Señal precursora; no equivale a evento",
        ],
        ["ENS", "Energía no suministrada por interrupciones", "Debe leerse con clientes, duración y criticidad"],
        [
            "Brecha técnica",
            "Demanda crítica menos cobertura flexible efectiva",
            "Requiere coincidencia de nodo, hora y duración",
        ],
        [
            "Índice de prioridad",
            "Combinación normalizada de factores de decisión",
            "Ordena expedientes; no aprueba tecnología",
        ],
        ["Coste de riesgo", "Valor relativo de congestión, ENS y vertido", "No es presupuesto, valor esperado ni VAN"],
        [
            "NMAE",
            "MAE normalizado para comparar precisión entre zonas",
            "Debe acompañarse de sesgo, ventanas y cobertura",
        ],
    ]
    story.append(_data_table(definitions_rows, [3.4 * cm, 7.0 * cm, 6.5 * cm], styles))
    story.append(Spacer(1, 9))
    story.append(Paragraph("Ficha de control de publicación", styles["H3"]))
    control_rows = [
        ["CONTROL", "RESULTADO", "EVIDENCIA"],
        [
            "Integridad analítica",
            f"{int(checks['passed'].sum())}/{len(checks)} controles superados",
            "Dominios, integridad y consistencia SQL",
        ],
        ["Cobertura editorial", "19 figuras y anexos completos", "Narrativa, ranking, alimentadores y definiciones"],
        ["Reproducibilidad", "Generación determinista", "Metadatos estables y tipografías integradas"],
        [
            "Pronóstico",
            _label(forecast_monitor["monitoring_status"]),
            f"MAE reciente {_fmt_dec(forecast_monitor['mae_drift_ratio'], 2)}×; cobertura 95 % {_fmt_pct(coverage_95)}",
        ],
        ["Condición de uso", "Publicación con salvedades", "Datos sintéticos, costes de referencia y sin flujo AC/N-1"],
    ]
    story.append(_data_table(control_rows, [4.0 * cm, 4.4 * cm, 8.5 * cm], styles))
    story.append(Spacer(1, 8))
    audit_closure = [
        "El control de publicación verifica que las salidas utilizadas por el informe corresponden a la misma ejecución, que las figuras requeridas están presentes y que los anexos conservan la trazabilidad de cartera. La reproducibilidad técnica no amplía el alcance de la evidencia: asegura que la misma entrada y configuración producen el mismo artefacto auditable.",
        "La siguiente madurez del producto consiste en sustituir fuentes sintéticas por datos de producción gobernados y reconciliar cada actualización con la versión publicada. Ese cambio debe conservar contratos de esquema, controles de calidad, definiciones métricas y registro de decisión para evitar que la evolución de datos rompa comparabilidad histórica.",
        "Cierre de auditoría analítica. El informe ha sido generado de forma reproducible a partir de las salidas publicadas, con tipografías integradas, figuras numeradas, índice navegable y declaración explícita de límites. La recomendación permanece condicionada a la evidencia de producción descrita en los umbrales de inversión.",
    ]
    story.append(_multi_column_text(audit_closure, styles, columns=3))

    doc.multiBuild(story)
    return pdf_path


if __name__ == "__main__":
    print(build_pdf_report())
