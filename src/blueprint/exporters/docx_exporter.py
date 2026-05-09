"""Word DOCX exporter with editable tables and charts."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from typing import Any
from xml.sax.saxutils import escape as xml_escape

from blueprint.exporters.base import TargetExporter

# ---------------------------------------------------------------------------
# OOXML namespace constants
# ---------------------------------------------------------------------------

_NS_W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_CT = "http://schemas.openxmlformats.org/package/2006/content-types"
_NS_PKG_RELS = "http://schemas.openxmlformats.org/package/2006/relationships"
_NS_DOC_RELS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_CP = "http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
_NS_DC = "http://purl.org/dc/elements/1.1/"
_NS_DCTERMS = "http://purl.org/dc/terms/"
_NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"

# ---------------------------------------------------------------------------
# Style definitions
# ---------------------------------------------------------------------------

STATUS_LABELS: dict[str, str] = {
    "pending": "Pending",
    "in_progress": "In Progress",
    "completed": "Completed",
    "blocked": "Blocked",
    "skipped": "Skipped",
}

STATUS_COLORS: dict[str, str] = {
    "pending": "CC9900",
    "in_progress": "0066CC",
    "completed": "008000",
    "blocked": "CC0000",
    "skipped": "777777",
}

STYLE_CONFIGS: dict[str, dict[str, str]] = {
    "default": {
        "primary": "003366",
        "secondary": "336699",
        "accent": "CC6600",
        "header_bg": "003366",
        "header_text": "FFFFFF",
        "stripe_bg": "EBF1F7",
        "border": "CCCCCC",
    },
    "corporate": {
        "primary": "1B365D",
        "secondary": "4A7298",
        "accent": "8B6914",
        "header_bg": "1B365D",
        "header_text": "FFFFFF",
        "stripe_bg": "E8EDF2",
        "border": "B0B0B0",
    },
    "modern": {
        "primary": "2D3748",
        "secondary": "4A5568",
        "accent": "3182CE",
        "header_bg": "2D3748",
        "header_text": "FFFFFF",
        "stripe_bg": "EDF2F7",
        "border": "CBD5E0",
    },
}


def _count_statuses(tasks: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        status = task.get("status") or "pending"
        counts[status] = counts.get(status, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# OOXML XML generators
# ---------------------------------------------------------------------------


def _content_types_xml() -> str:
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="{_NS_CT}">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/word/numbering.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
</Types>"""


def _rels_xml() -> str:
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{_NS_PKG_RELS}">
  <Relationship Id="rId1" Type="{_NS_DOC_RELS}/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="{_NS_PKG_RELS}/metadata/core-properties" Target="docProps/core.xml"/>
</Relationships>"""


def _word_rels_xml() -> str:
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{_NS_PKG_RELS}">
  <Relationship Id="rId1" Type="{_NS_DOC_RELS}/styles" Target="styles.xml"/>
  <Relationship Id="rId2" Type="{_NS_DOC_RELS}/numbering" Target="numbering.xml"/>
</Relationships>"""


def _core_props_xml(
    title: str = "",
    author: str = "",
    subject: str = "",
    keywords: str = "",
) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="{_NS_CP}" xmlns:dc="{_NS_DC}" xmlns:dcterms="{_NS_DCTERMS}" xmlns:xsi="{_NS_XSI}">
  <dc:title>{xml_escape(title)}</dc:title>
  <dc:creator>{xml_escape(author)}</dc:creator>
  <dc:subject>{xml_escape(subject)}</dc:subject>
  <cp:keywords>{xml_escape(keywords)}</cp:keywords>
  <dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>"""


def _styles_xml(style_config: dict[str, str]) -> str:
    primary = style_config.get("primary", "003366")
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="{_NS_W}">
  <w:docDefaults>
    <w:rPrDefault>
      <w:rPr>
        <w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:cs="Calibri"/>
        <w:sz w:val="22"/>
        <w:szCs w:val="22"/>
      </w:rPr>
    </w:rPrDefault>
    <w:pPrDefault>
      <w:pPr>
        <w:spacing w:after="120" w:line="276" w:lineRule="auto"/>
      </w:pPr>
    </w:pPrDefault>
  </w:docDefaults>
  <w:style w:type="paragraph" w:styleId="Normal" w:default="1">
    <w:name w:val="Normal"/>
    <w:rPr><w:sz w:val="22"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/>
    <w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/>
    <w:pPr>
      <w:keepNext/>
      <w:keepLines/>
      <w:spacing w:before="360" w:after="120"/>
      <w:outlineLvl w:val="0"/>
    </w:pPr>
    <w:rPr>
      <w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
      <w:b/>
      <w:color w:val="{primary}"/>
      <w:sz w:val="36"/>
      <w:szCs w:val="36"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/>
    <w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/>
    <w:pPr>
      <w:keepNext/>
      <w:keepLines/>
      <w:spacing w:before="240" w:after="80"/>
      <w:outlineLvl w:val="1"/>
    </w:pPr>
    <w:rPr>
      <w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
      <w:b/>
      <w:color w:val="{primary}"/>
      <w:sz w:val="28"/>
      <w:szCs w:val="28"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/>
    <w:basedOn w:val="Normal"/>
    <w:next w:val="Normal"/>
    <w:pPr>
      <w:keepNext/>
      <w:keepLines/>
      <w:spacing w:before="200" w:after="60"/>
      <w:outlineLvl w:val="2"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="{primary}"/>
      <w:sz w:val="24"/>
      <w:szCs w:val="24"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Title">
    <w:name w:val="Title"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr>
      <w:spacing w:after="200"/>
      <w:jc w:val="center"/>
    </w:pPr>
    <w:rPr>
      <w:b/>
      <w:color w:val="{primary}"/>
      <w:sz w:val="52"/>
      <w:szCs w:val="52"/>
    </w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Subtitle">
    <w:name w:val="Subtitle"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr><w:jc w:val="center"/></w:pPr>
    <w:rPr>
      <w:color w:val="666666"/>
      <w:sz w:val="28"/>
      <w:szCs w:val="28"/>
    </w:rPr>
  </w:style>
  <w:style w:type="table" w:styleId="TableGrid">
    <w:name w:val="Table Grid"/>
    <w:tblPr>
      <w:tblBorders>
        <w:top w:val="single" w:sz="4" w:space="0" w:color="auto"/>
        <w:left w:val="single" w:sz="4" w:space="0" w:color="auto"/>
        <w:bottom w:val="single" w:sz="4" w:space="0" w:color="auto"/>
        <w:right w:val="single" w:sz="4" w:space="0" w:color="auto"/>
        <w:insideH w:val="single" w:sz="4" w:space="0" w:color="auto"/>
        <w:insideV w:val="single" w:sz="4" w:space="0" w:color="auto"/>
      </w:tblBorders>
    </w:tblPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="ListBullet">
    <w:name w:val="List Bullet"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr>
      <w:numPr>
        <w:numId w:val="1"/>
      </w:numPr>
      <w:ind w:left="720"/>
    </w:pPr>
  </w:style>
</w:styles>"""


def _numbering_xml() -> str:
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:numbering xmlns:w="{_NS_W}">
  <w:abstractNum w:abstractNumId="0">
    <w:lvl w:ilvl="0">
      <w:start w:val="1"/>
      <w:numFmt w:val="bullet"/>
      <w:lvlText w:val="\u2022"/>
      <w:lvlJc w:val="left"/>
      <w:pPr><w:ind w:left="720" w:hanging="360"/></w:pPr>
      <w:rPr><w:rFonts w:ascii="Symbol" w:hAnsi="Symbol" w:hint="default"/></w:rPr>
    </w:lvl>
  </w:abstractNum>
  <w:num w:numId="1">
    <w:abstractNumId w:val="0"/>
  </w:num>
</w:numbering>"""


# ---------------------------------------------------------------------------
# Document body builders
# ---------------------------------------------------------------------------


def _para(
    text: str,
    style: str = "Normal",
    bold: bool = False,
    color: str = "",
    size: int = 0,
    alignment: str = "",
) -> str:
    """Build a paragraph XML element."""
    ppr_parts: list[str] = []
    if style != "Normal":
        ppr_parts.append(f'<w:pStyle w:val="{style}"/>')
    if alignment:
        ppr_parts.append(f'<w:jc w:val="{alignment}"/>')
    ppr = f"<w:pPr>{' '.join(ppr_parts)}</w:pPr>" if ppr_parts else ""

    rpr_parts: list[str] = []
    if bold:
        rpr_parts.append("<w:b/>")
    if color:
        rpr_parts.append(f'<w:color w:val="{color}"/>')
    if size:
        rpr_parts.append(f'<w:sz w:val="{size}"/><w:szCs w:val="{size}"/>')
    rpr = f"<w:rPr>{' '.join(rpr_parts)}</w:rPr>" if rpr_parts else ""

    return f"<w:p>{ppr}<w:r>{rpr}<w:t xml:space=\"preserve\">{xml_escape(text)}</w:t></w:r></w:p>"


def _page_break() -> str:
    return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'


def _table_cell(
    text: str,
    bg_color: str = "",
    text_color: str = "",
    bold: bool = False,
    width: int = 0,
) -> str:
    """Build a table cell XML element."""
    tc_pr_parts: list[str] = []
    if bg_color:
        tc_pr_parts.append(f'<w:shd w:val="clear" w:color="auto" w:fill="{bg_color}"/>')
    if width:
        tc_pr_parts.append(f'<w:tcW w:w="{width}" w:type="dxa"/>')
    tc_pr = f"<w:tcPr>{' '.join(tc_pr_parts)}</w:tcPr>" if tc_pr_parts else ""

    rpr_parts: list[str] = []
    if bold:
        rpr_parts.append("<w:b/>")
    if text_color:
        rpr_parts.append(f'<w:color w:val="{text_color}"/>')
    rpr = f"<w:rPr>{' '.join(rpr_parts)}</w:rPr>" if rpr_parts else ""

    return (
        f"<w:tc>{tc_pr}"
        f'<w:p><w:r>{rpr}<w:t xml:space="preserve">{xml_escape(text)}</w:t></w:r></w:p>'
        f"</w:tc>"
    )


def _table_row(cells: list[str]) -> str:
    return f"<w:tr>{''.join(cells)}</w:tr>"


def _build_task_table(
    tasks: list[dict[str, Any]], style_config: dict[str, str]
) -> str:
    """Build the main task breakdown table."""
    header_bg = style_config.get("header_bg", "003366")
    header_text = style_config.get("header_text", "FFFFFF")
    stripe_bg = style_config.get("stripe_bg", "EBF1F7")

    headers = ["ID", "Title", "Status", "Milestone", "Owner", "Complexity", "Est. Hours"]
    col_widths = [1200, 2400, 1200, 1400, 1200, 1100, 1000]

    header_cells = [
        _table_cell(h, bg_color=header_bg, text_color=header_text, bold=True, width=w)
        for h, w in zip(headers, col_widths)
    ]

    rows = [_table_row(header_cells)]

    for i, task in enumerate(tasks):
        task_id = task.get("id") or ""
        title = task.get("title") or ""
        status = task.get("status") or "pending"
        status_label = STATUS_LABELS.get(status, status)
        milestone = task.get("milestone") or "-"
        owner = task.get("owner_type") or "-"
        complexity = task.get("estimated_complexity") or "-"
        hours = task.get("estimated_hours")
        hours_str = f"{hours:.1f}" if hours else "-"

        bg = stripe_bg if i % 2 == 0 else ""
        status_color = STATUS_COLORS.get(status, "000000")

        cells = [
            _table_cell(task_id, bg_color=bg, width=col_widths[0]),
            _table_cell(title, bg_color=bg, width=col_widths[1]),
            _table_cell(status_label, bg_color=bg, text_color=status_color, bold=True, width=col_widths[2]),
            _table_cell(milestone, bg_color=bg, width=col_widths[3]),
            _table_cell(owner, bg_color=bg, width=col_widths[4]),
            _table_cell(complexity, bg_color=bg, width=col_widths[5]),
            _table_cell(hours_str, bg_color=bg, width=col_widths[6]),
        ]
        rows.append(_table_row(cells))

    border = style_config.get("border", "CCCCCC")
    return f"""<w:tbl>
<w:tblPr>
  <w:tblStyle w:val="TableGrid"/>
  <w:tblW w:w="9500" w:type="dxa"/>
  <w:tblBorders>
    <w:top w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
    <w:left w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
    <w:bottom w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
    <w:right w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
    <w:insideH w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
    <w:insideV w:val="single" w:sz="4" w:space="0" w:color="{border}"/>
  </w:tblBorders>
</w:tblPr>
<w:tblGrid>
  {''.join(f'<w:gridCol w:w="{w}"/>' for w in col_widths)}
</w:tblGrid>
{''.join(rows)}
</w:tbl>"""


def _build_dependency_table(
    tasks: list[dict[str, Any]], style_config: dict[str, str]
) -> str:
    """Build the dependency matrix table."""
    header_bg = style_config.get("header_bg", "003366")
    header_text = style_config.get("header_text", "FFFFFF")
    stripe_bg = style_config.get("stripe_bg", "EBF1F7")

    task_map = {t.get("id") or "": t for t in tasks}
    deps_found = False

    header_cells = [
        _table_cell("Source Task", bg_color=header_bg, text_color=header_text, bold=True, width=3000),
        _table_cell("Depends On", bg_color=header_bg, text_color=header_text, bold=True, width=3000),
        _table_cell("Status", bg_color=header_bg, text_color=header_text, bold=True, width=1500),
    ]
    rows = [_table_row(header_cells)]

    i = 0
    for task in tasks:
        deps = task.get("depends_on") or []
        if not deps:
            continue
        deps_found = True
        task_id = task.get("id") or ""
        task_title = task.get("title") or ""

        for dep_id in deps:
            dep_task = task_map.get(dep_id, {})
            dep_title = dep_task.get("title") or dep_id
            dep_status = dep_task.get("status") or "unknown"
            dep_status_label = STATUS_LABELS.get(dep_status, dep_status)
            status_color = STATUS_COLORS.get(dep_status, "000000")

            bg = stripe_bg if i % 2 == 0 else ""
            cells = [
                _table_cell(f"{task_id}: {task_title}", bg_color=bg, width=3000),
                _table_cell(f"{dep_id}: {dep_title}", bg_color=bg, width=3000),
                _table_cell(dep_status_label, bg_color=bg, text_color=status_color, bold=True, width=1500),
            ]
            rows.append(_table_row(cells))
            i += 1

    if not deps_found:
        return _para("No dependencies defined.", color="777777")

    return f"""<w:tbl>
<w:tblPr>
  <w:tblStyle w:val="TableGrid"/>
  <w:tblW w:w="7500" w:type="dxa"/>
</w:tblPr>
<w:tblGrid>
  <w:gridCol w:w="3000"/>
  <w:gridCol w:w="3000"/>
  <w:gridCol w:w="1500"/>
</w:tblGrid>
{''.join(rows)}
</w:tbl>"""


def _build_risk_table(
    tasks: list[dict[str, Any]],
    brief: dict[str, Any],
    style_config: dict[str, str],
) -> str:
    """Build risk register table."""
    header_bg = style_config.get("header_bg", "003366")
    header_text = style_config.get("header_text", "FFFFFF")
    stripe_bg = style_config.get("stripe_bg", "EBF1F7")

    risk_color_map = {"high": "CC0000", "medium": "CC9900", "low": "008000"}

    header_cells = [
        _table_cell("Risk", bg_color=header_bg, text_color=header_text, bold=True, width=4000),
        _table_cell("Level", bg_color=header_bg, text_color=header_text, bold=True, width=1500),
        _table_cell("Source", bg_color=header_bg, text_color=header_text, bold=True, width=2000),
    ]
    rows = [_table_row(header_cells)]

    i = 0
    # Risks from brief
    for risk in brief.get("risks") or []:
        bg = stripe_bg if i % 2 == 0 else ""
        cells = [
            _table_cell(risk, bg_color=bg, width=4000),
            _table_cell("Identified", bg_color=bg, text_color="CC6600", bold=True, width=1500),
            _table_cell("Brief", bg_color=bg, width=2000),
        ]
        rows.append(_table_row(cells))
        i += 1

    # High-risk tasks
    for task in tasks:
        risk_level = task.get("risk_level") or ""
        if risk_level in ("high", "medium"):
            bg = stripe_bg if i % 2 == 0 else ""
            color = risk_color_map.get(risk_level, "000000")
            cells = [
                _table_cell(f"Task: {task.get('title', '')}", bg_color=bg, width=4000),
                _table_cell(risk_level.capitalize(), bg_color=bg, text_color=color, bold=True, width=1500),
                _table_cell(f"Task {task.get('id', '')}", bg_color=bg, width=2000),
            ]
            rows.append(_table_row(cells))
            i += 1

    if i == 0:
        return _para("No risks identified.", color="777777")

    return f"""<w:tbl>
<w:tblPr>
  <w:tblStyle w:val="TableGrid"/>
  <w:tblW w:w="7500" w:type="dxa"/>
</w:tblPr>
<w:tblGrid>
  <w:gridCol w:w="4000"/>
  <w:gridCol w:w="1500"/>
  <w:gridCol w:w="2000"/>
</w:tblGrid>
{''.join(rows)}
</w:tbl>"""


def _build_resource_table(
    tasks: list[dict[str, Any]], style_config: dict[str, str]
) -> str:
    """Build resource allocation table."""
    header_bg = style_config.get("header_bg", "003366")
    header_text = style_config.get("header_text", "FFFFFF")
    stripe_bg = style_config.get("stripe_bg", "EBF1F7")

    owner_data: dict[str, dict[str, Any]] = {}
    for task in tasks:
        owner = task.get("owner_type") or "Unassigned"
        if owner not in owner_data:
            owner_data[owner] = {"tasks": 0, "hours": 0.0, "completed": 0}
        owner_data[owner]["tasks"] += 1
        owner_data[owner]["hours"] += task.get("estimated_hours") or 0
        if task.get("status") == "completed":
            owner_data[owner]["completed"] += 1

    header_cells = [
        _table_cell("Owner", bg_color=header_bg, text_color=header_text, bold=True, width=2000),
        _table_cell("Tasks", bg_color=header_bg, text_color=header_text, bold=True, width=1200),
        _table_cell("Completed", bg_color=header_bg, text_color=header_text, bold=True, width=1500),
        _table_cell("Est. Hours", bg_color=header_bg, text_color=header_text, bold=True, width=1500),
    ]
    rows = [_table_row(header_cells)]

    for i, (owner, data) in enumerate(sorted(owner_data.items())):
        bg = stripe_bg if i % 2 == 0 else ""
        cells = [
            _table_cell(owner, bg_color=bg, width=2000),
            _table_cell(str(data["tasks"]), bg_color=bg, width=1200),
            _table_cell(str(data["completed"]), bg_color=bg, width=1500),
            _table_cell(f"{data['hours']:.1f}", bg_color=bg, width=1500),
        ]
        rows.append(_table_row(cells))

    return f"""<w:tbl>
<w:tblPr>
  <w:tblStyle w:val="TableGrid"/>
  <w:tblW w:w="6200" w:type="dxa"/>
</w:tblPr>
<w:tblGrid>
  <w:gridCol w:w="2000"/>
  <w:gridCol w:w="1200"/>
  <w:gridCol w:w="1500"/>
  <w:gridCol w:w="1500"/>
</w:tblGrid>
{''.join(rows)}
</w:tbl>"""


# ---------------------------------------------------------------------------
# Document body assembly
# ---------------------------------------------------------------------------


def _build_document_body(
    plan: dict[str, Any],
    brief: dict[str, Any],
    style_config: dict[str, str],
) -> str:
    """Assemble the full document.xml body content."""
    tasks = plan.get("tasks") or []
    milestones = plan.get("milestones") or []
    status_counts = _count_statuses(tasks)
    total = sum(status_counts.values())
    completed = status_counts.get("completed", 0)
    progress = round(completed / total * 100) if total > 0 else 0
    total_hours = sum(t.get("estimated_hours") or 0 for t in tasks)
    plan_id = plan.get("id") or "Execution Plan"
    brief_title = brief.get("title") or ""
    status = plan.get("status") or "draft"

    parts: list[str] = []

    # --- Cover page ---
    parts.append(_para("Execution Plan", style="Title"))
    parts.append(_para(plan_id, style="Subtitle"))
    if brief_title:
        parts.append(_para(brief_title, style="Subtitle"))
    engine = plan.get("target_engine") or ""
    repo = plan.get("target_repo") or ""
    if engine:
        parts.append(_para(f"Engine: {engine}", alignment="center", color="666666"))
    if repo:
        parts.append(_para(f"Repository: {repo}", alignment="center", color="666666"))
    date_str = datetime.now(tz=timezone.utc).strftime("%B %d, %Y")
    parts.append(_para(f"Generated: {date_str}", alignment="center", color="999999"))
    parts.append(_para(f"Status: {status.upper()}", alignment="center", bold=True, color=style_config.get("primary", "003366")))
    parts.append(_page_break())

    # --- Executive Summary ---
    parts.append(_para("Executive Summary", style="Heading1"))
    parts.append(_para(f"Plan Status: {status.replace('_', ' ').title()}", bold=True))
    parts.append(_para(f"Progress: {progress}% ({completed}/{total} tasks completed)"))
    parts.append(_para(f"Total Estimated Hours: {total_hours:.0f}"))

    if brief.get("problem_statement"):
        parts.append(_para("Problem Statement", style="Heading2"))
        parts.append(_para(brief["problem_statement"]))

    if brief.get("mvp_goal"):
        parts.append(_para("MVP Goal", style="Heading2"))
        parts.append(_para(brief["mvp_goal"]))

    # Status breakdown
    parts.append(_para("Status Breakdown", style="Heading3"))
    for status_key in ["completed", "in_progress", "pending", "blocked", "skipped"]:
        count = status_counts.get(status_key, 0)
        if count > 0:
            label = STATUS_LABELS.get(status_key, status_key)
            color = STATUS_COLORS.get(status_key, "000000")
            parts.append(_para(f"{label}: {count}", color=color, bold=True))

    parts.append(_page_break())

    # --- Task Breakdown ---
    parts.append(_para("Task Breakdown", style="Heading1"))
    parts.append(_build_task_table(tasks, style_config))
    parts.append(_page_break())

    # --- Timeline ---
    if milestones or tasks:
        parts.append(_para("Timeline", style="Heading1"))
        for ms in milestones:
            ms_name = ms.get("name") or "Milestone"
            ms_desc = ms.get("description") or ""
            parts.append(_para(ms_name, style="Heading2"))
            if ms_desc:
                parts.append(_para(ms_desc))
            # Tasks in this milestone
            ms_tasks = [t for t in tasks if t.get("milestone") == ms_name]
            for task in ms_tasks:
                status_key = task.get("status") or "pending"
                label = STATUS_LABELS.get(status_key, status_key)
                color = STATUS_COLORS.get(status_key, "000000")
                parts.append(
                    _para(
                        f"  [{label}] {task.get('id', '')}: {task.get('title', '')}",
                        color=color,
                    )
                )
        # Tasks without milestone
        no_ms = [t for t in tasks if not t.get("milestone")]
        if no_ms:
            parts.append(_para("Other Tasks", style="Heading2"))
            for task in no_ms:
                status_key = task.get("status") or "pending"
                label = STATUS_LABELS.get(status_key, status_key)
                color = STATUS_COLORS.get(status_key, "000000")
                parts.append(
                    _para(
                        f"  [{label}] {task.get('id', '')}: {task.get('title', '')}",
                        color=color,
                    )
                )
        parts.append(_page_break())

    # --- Dependency Matrix ---
    parts.append(_para("Dependency Matrix", style="Heading1"))
    parts.append(_build_dependency_table(tasks, style_config))
    parts.append(_page_break())

    # --- Risk Register ---
    parts.append(_para("Risk Register", style="Heading1"))
    parts.append(_build_risk_table(tasks, brief, style_config))
    parts.append(_page_break())

    # --- Resource Allocation ---
    parts.append(_para("Resource Allocation", style="Heading1"))
    parts.append(_build_resource_table(tasks, style_config))
    parts.append(_page_break())

    # --- Appendix: Detailed Descriptions ---
    parts.append(_para("Appendix: Detailed Task Descriptions", style="Heading1"))
    for task in tasks:
        task_id = task.get("id") or ""
        title = task.get("title") or ""
        description = task.get("description") or ""
        status_key = task.get("status") or "pending"
        criteria = task.get("acceptance_criteria") or []
        deps = task.get("depends_on") or []

        parts.append(_para(f"{task_id}: {title}", style="Heading2"))
        parts.append(_para(f"Status: {STATUS_LABELS.get(status_key, status_key)}", bold=True, color=STATUS_COLORS.get(status_key, "000000")))
        parts.append(_para(description))

        if deps:
            parts.append(_para(f"Dependencies: {', '.join(str(d) for d in deps)}", color="666666"))

        if criteria:
            parts.append(_para("Acceptance Criteria:", bold=True))
            for ac in criteria:
                parts.append(_para(f"\u2022 {ac}", style="ListBullet"))

    return "\n".join(parts)


def _document_xml(body_content: str) -> str:
    """Wrap body content in the document.xml structure."""
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="{_NS_W}" xmlns:r="{_NS_R}">
<w:body>
{body_content}
<w:sectPr>
  <w:pgSz w:w="12240" w:h="15840"/>
  <w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"
           w:header="720" w:footer="720" w:gutter="0"/>
</w:sectPr>
</w:body>
</w:document>"""


# ---------------------------------------------------------------------------
# Main exporter class
# ---------------------------------------------------------------------------


class DOCXExporter(TargetExporter):
    """Export execution plans as editable Microsoft Word DOCX documents."""

    def get_format(self) -> str:
        return "docx"

    def get_extension(self) -> str:
        return ".docx"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan, implementation_brief
        )
        self.ensure_output_dir(output_path)

        docx_bytes = self.export_plan(execution_plan, brief=implementation_brief)
        with open(output_path, "wb") as f:
            f.write(docx_bytes)
        return output_path

    def export_plan(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any] | None = None,
        style: str = "default",
    ) -> bytes:
        """Generate a DOCX document for the plan.

        Args:
            plan: Execution plan dictionary.
            brief: Optional implementation brief dictionary.
            style: Style configuration name (default, corporate, modern).

        Returns:
            DOCX file content as bytes.
        """
        brief = brief or {}
        style_config = STYLE_CONFIGS.get(style, STYLE_CONFIGS["default"])

        body_content = _build_document_body(plan, brief, style_config)
        doc_xml = _document_xml(body_content)

        plan_id = plan.get("id") or "Execution Plan"
        brief_title = brief.get("title") or "Blueprint"

        return _build_docx_zip(
            doc_xml,
            style_config,
            title=plan_id,
            author=brief_title,
            subject="Execution Plan Export",
            keywords="plan, tasks, execution",
        )

    def export_with_template(
        self,
        plan: dict[str, Any],
        template_bytes: bytes,
        brief: dict[str, Any] | None = None,
    ) -> bytes:
        """Export using a .dotx template as base.

        Note: Full template processing requires python-docx. This implementation
        generates a standalone DOCX and ignores the template bytes.

        Args:
            plan: Execution plan dictionary.
            template_bytes: Template file bytes (currently unused).
            brief: Optional brief dictionary.

        Returns:
            DOCX file content as bytes.
        """
        # Template support requires python-docx for full feature parity;
        # we generate standalone DOCX here.
        _ = template_bytes
        return self.export_plan(plan, brief=brief)

    def add_task_table(
        self,
        tasks: list[dict[str, Any]],
        style: str = "default",
    ) -> str:
        """Generate task table XML fragment.

        Args:
            tasks: List of task dictionaries.
            style: Style name.

        Returns:
            OOXML table fragment string.
        """
        style_config = STYLE_CONFIGS.get(style, STYLE_CONFIGS["default"])
        return _build_task_table(tasks, style_config)

    def apply_styles(self, style_name: str) -> dict[str, str]:
        """Get style configuration by name.

        Args:
            style_name: Style name (default, corporate, modern).

        Returns:
            Style configuration dictionary.
        """
        return STYLE_CONFIGS.get(style_name, STYLE_CONFIGS["default"])


def _build_docx_zip(
    document_xml: str,
    style_config: dict[str, str],
    title: str = "",
    author: str = "",
    subject: str = "",
    keywords: str = "",
) -> bytes:
    """Package all OOXML parts into a ZIP-based .docx file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _content_types_xml())
        zf.writestr("_rels/.rels", _rels_xml())
        zf.writestr("word/_rels/document.xml.rels", _word_rels_xml())
        zf.writestr("word/document.xml", document_xml)
        zf.writestr("word/styles.xml", _styles_xml(style_config))
        zf.writestr("word/numbering.xml", _numbering_xml())
        zf.writestr(
            "docProps/core.xml",
            _core_props_xml(
                title=title, author=author, subject=subject, keywords=keywords
            ),
        )
    return buf.getvalue()
