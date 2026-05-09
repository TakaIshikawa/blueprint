"""Word DOCX exporter for execution plans."""

from __future__ import annotations

import io
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor

from blueprint.exporters.base import TargetExporter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATUS_LABELS: dict[str, str] = {
    "pending": "Pending",
    "in_progress": "In Progress",
    "completed": "Completed",
    "blocked": "Blocked",
    "skipped": "Skipped",
}

STATUS_COLORS: dict[str, RGBColor] = {
    "pending": RGBColor(0xFF, 0xC1, 0x07),
    "in_progress": RGBColor(0x0D, 0xCA, 0xF0),
    "completed": RGBColor(0x19, 0x87, 0x54),
    "blocked": RGBColor(0xDC, 0x35, 0x45),
    "skipped": RGBColor(0x6C, 0x75, 0x7D),
}

PRIORITY_COLORS: dict[str, RGBColor] = {
    "critical": RGBColor(0xDC, 0x35, 0x45),
    "high": RGBColor(0xFF, 0x85, 0x00),
    "medium": RGBColor(0xFF, 0xC1, 0x07),
    "low": RGBColor(0x19, 0x87, 0x54),
}

DEFAULT_STYLE_CONFIG: dict[str, Any] = {
    "font_name": "Calibri",
    "heading_color": RGBColor(0x0D, 0x6E, 0xFD),
    "header_bg": RGBColor(0x34, 0x3A, 0x40),
    "header_text": RGBColor(0xFF, 0xFF, 0xFF),
    "stripe_bg": RGBColor(0xF2, 0xF2, 0xF2),
    "border_color": RGBColor(0xDE, 0xE2, 0xE6),
}


# ---------------------------------------------------------------------------
# Helper: table cell shading
# ---------------------------------------------------------------------------


def _set_cell_shading(cell: Any, color: RGBColor) -> None:
    """Apply background shading to a table cell."""
    shading = OxmlElement("w:shd")
    shading.set(qn("w:fill"), f"{color}")
    shading.set(qn("w:val"), "clear")
    cell._tc.get_or_add_tcPr().append(shading)


def _set_cell_text(cell: Any, text: str, bold: bool = False,
                   color: RGBColor | None = None,
                   font_size: int | None = None) -> None:
    """Set cell text with optional formatting."""
    cell.text = ""
    paragraph = cell.paragraphs[0]
    run = paragraph.add_run(text)
    run.bold = bold
    if color:
        run.font.color.rgb = color
    if font_size:
        run.font.size = Pt(font_size)


def _format_header_row(row: Any, style_config: dict[str, Any] | None = None) -> None:
    """Format a table header row with background and white text."""
    cfg = style_config or DEFAULT_STYLE_CONFIG
    header_bg = cfg.get("header_bg", DEFAULT_STYLE_CONFIG["header_bg"])
    header_text = cfg.get("header_text", DEFAULT_STYLE_CONFIG["header_text"])

    for cell in row.cells:
        _set_cell_shading(cell, header_bg)
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.font.color.rgb = header_text
                run.bold = True


def _add_alternating_rows(table: Any, style_config: dict[str, Any] | None = None) -> None:
    """Apply alternating row colors to a table (skip header row)."""
    cfg = style_config or DEFAULT_STYLE_CONFIG
    stripe = cfg.get("stripe_bg", DEFAULT_STYLE_CONFIG["stripe_bg"])

    for i, row in enumerate(table.rows):
        if i == 0:
            continue
        if i % 2 == 0:
            for cell in row.cells:
                _set_cell_shading(cell, stripe)


# ---------------------------------------------------------------------------
# Main exporter
# ---------------------------------------------------------------------------


class DOCXExporter(TargetExporter):
    """Export execution plans as editable Word DOCX documents."""

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
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        doc_bytes = self.export_plan(execution_plan, brief=implementation_brief)
        with open(output_path, "wb") as f:
            f.write(doc_bytes)
        return output_path

    def export_plan(
        self,
        plan: dict[str, Any],
        template: str | None = None,
        brief: dict[str, Any] | None = None,
        style_config: dict[str, Any] | None = None,
    ) -> bytes:
        """Generate a Word DOCX document for the plan.

        Args:
            plan: Execution plan dictionary.
            template: Unused string template name (kept for API symmetry).
            brief: Optional implementation brief dictionary.
            style_config: Optional style overrides.

        Returns:
            DOCX file content as bytes.
        """
        doc = Document()
        brief = brief or {}
        cfg = style_config or DEFAULT_STYLE_CONFIG

        self.apply_styles(doc, cfg)
        self._add_document_properties(doc, plan, brief)
        self._add_cover_page(doc, plan, brief)
        self._add_toc_field(doc)
        self._add_executive_summary(doc, plan, brief)

        tasks = plan.get("tasks") or []
        self.add_task_table(doc, tasks, cfg)
        self.add_timeline_chart(doc, plan)
        self._add_dependency_matrix(doc, tasks)
        self._add_risk_register(doc, tasks)
        self._add_resource_allocation(doc, tasks)
        self._add_appendix(doc, tasks)

        buf = io.BytesIO()
        doc.save(buf)
        return buf.getvalue()

    def export_with_template(
        self,
        plan: dict[str, Any],
        docx_template: str | bytes,
    ) -> bytes:
        """Generate DOCX using an existing .dotx / .docx template.

        Loads the template, maps plan data into content controls where
        available, and falls back to appending sections.

        Args:
            plan: Execution plan dictionary.
            docx_template: Path to a .dotx/.docx template file, or raw bytes.

        Returns:
            DOCX file content as bytes.
        """
        if isinstance(docx_template, (str, Path)):
            doc = Document(str(docx_template))
        else:
            doc = Document(io.BytesIO(docx_template))

        tasks = plan.get("tasks") or []

        self._map_content_controls(doc, plan)
        self.add_task_table(doc, tasks)
        self.add_timeline_chart(doc, plan)

        buf = io.BytesIO()
        doc.save(buf)
        return buf.getvalue()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def add_task_table(
        self,
        doc: Document,
        tasks: list[dict[str, Any]],
        style_config: dict[str, Any] | None = None,
    ) -> None:
        """Add the task breakdown table to the document."""
        doc.add_page_break()
        doc.add_heading("Task Breakdown", level=1)

        if not tasks:
            doc.add_paragraph("No tasks defined.")
            return

        headers = ["ID", "Title", "Status", "Assignee", "Due Date", "Priority"]
        table = doc.add_table(rows=1, cols=len(headers))
        table.style = "Table Grid"
        table.alignment = WD_TABLE_ALIGNMENT.CENTER

        # Header row
        for i, header in enumerate(headers):
            cell = table.rows[0].cells[i]
            _set_cell_text(cell, header, bold=True)

        _format_header_row(table.rows[0], style_config)

        # Data rows
        for task in tasks:
            row = table.add_row()
            task_id = task.get("id") or ""
            title = task.get("title") or ""
            status = task.get("status") or "pending"
            assignee = task.get("owner_type") or ""
            due_date = task.get("due_date") or ""
            priority = task.get("risk_level") or ""

            row.cells[0].text = task_id
            row.cells[1].text = title

            status_label = STATUS_LABELS.get(status, status)
            _set_cell_text(row.cells[2], status_label)
            status_color = STATUS_COLORS.get(status)
            if status_color:
                _set_cell_shading(row.cells[2], status_color)
                if status in ("completed", "blocked"):
                    for run in row.cells[2].paragraphs[0].runs:
                        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

            row.cells[3].text = assignee
            row.cells[4].text = str(due_date) if due_date else ""

            if priority:
                _set_cell_text(row.cells[5], priority.capitalize())
                p_color = PRIORITY_COLORS.get(priority.lower())
                if p_color:
                    for run in row.cells[5].paragraphs[0].runs:
                        run.font.color.rgb = p_color

        _add_alternating_rows(table, style_config)

    def add_timeline_chart(
        self,
        doc: Document,
        plan: dict[str, Any],
    ) -> None:
        """Add a timeline visualization section.

        Since python-docx cannot natively embed Excel chart objects,
        this renders a text-based timeline as a table. For true editable
        charts, use export_with_template with a pre-built chart template.
        """
        doc.add_page_break()
        doc.add_heading("Timeline", level=1)

        tasks = plan.get("tasks") or []
        milestones = plan.get("milestones") or []

        if not tasks and not milestones:
            doc.add_paragraph("No timeline data available.")
            return

        # Milestone summary
        if milestones:
            doc.add_heading("Milestones", level=2)
            for ms in milestones:
                name = ms.get("name") or "Milestone"
                desc = ms.get("description") or ""
                p = doc.add_paragraph(style="List Bullet")
                run = p.add_run(name)
                run.bold = True
                if desc:
                    p.add_run(f" — {desc}")

        # Task timeline table
        if tasks:
            doc.add_heading("Task Schedule", level=2)
            table = doc.add_table(rows=1, cols=4)
            table.style = "Table Grid"
            for i, h in enumerate(["ID", "Title", "Status", "Milestone"]):
                _set_cell_text(table.rows[0].cells[i], h, bold=True)
            _format_header_row(table.rows[0])

            for task in tasks:
                row = table.add_row()
                row.cells[0].text = task.get("id") or ""
                row.cells[1].text = task.get("title") or ""
                status = task.get("status") or "pending"
                row.cells[2].text = STATUS_LABELS.get(status, status)
                row.cells[3].text = task.get("milestone") or ""

            _add_alternating_rows(table)

    def apply_styles(
        self,
        doc: Document,
        style_config: dict[str, Any] | None = None,
    ) -> None:
        """Apply document-wide styles (font, heading colors)."""
        cfg = style_config or DEFAULT_STYLE_CONFIG
        font_name = cfg.get("font_name", "Calibri")
        heading_color = cfg.get("heading_color", DEFAULT_STYLE_CONFIG["heading_color"])

        style = doc.styles["Normal"]
        style.font.name = font_name
        style.font.size = Pt(11)

        for level in range(1, 4):
            heading_style_name = f"Heading {level}"
            if heading_style_name in doc.styles:
                h_style = doc.styles[heading_style_name]
                h_style.font.name = font_name
                h_style.font.color.rgb = heading_color

    # ------------------------------------------------------------------
    # Private section builders
    # ------------------------------------------------------------------

    def _add_document_properties(
        self, doc: Document, plan: dict[str, Any], brief: dict[str, Any]
    ) -> None:
        """Set document core properties."""
        props = doc.core_properties
        plan_id = plan.get("id") or "Execution Plan"
        props.title = f"Execution Plan: {plan_id}"
        props.subject = brief.get("problem_statement") or ""
        props.author = "Blueprint"
        props.keywords = ",".join([
            plan.get("target_engine") or "",
            plan.get("project_type") or "",
        ])
        props.category = "Execution Plan"

    def _add_cover_page(
        self, doc: Document, plan: dict[str, Any], brief: dict[str, Any]
    ) -> None:
        """Add a cover page with title, author, and date."""
        # Spacer
        for _ in range(6):
            doc.add_paragraph("")

        title = doc.add_paragraph()
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = title.add_run(f"Execution Plan: {plan.get('id') or ''}")
        run.bold = True
        run.font.size = Pt(28)
        run.font.color.rgb = RGBColor(0x0D, 0x6E, 0xFD)

        if brief.get("title"):
            subtitle = doc.add_paragraph()
            subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
            sub_run = subtitle.add_run(brief["title"])
            sub_run.font.size = Pt(16)
            sub_run.font.color.rgb = RGBColor(0x6C, 0x75, 0x7D)

        # Metadata
        meta = doc.add_paragraph()
        meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
        parts = []
        if plan.get("target_engine"):
            parts.append(f"Engine: {plan['target_engine']}")
        if plan.get("target_repo"):
            parts.append(f"Repo: {plan['target_repo']}")
        parts.append(f"Generated: {datetime.now().strftime('%Y-%m-%d')}")
        meta.add_run(" | ".join(parts)).font.color.rgb = RGBColor(0x6C, 0x75, 0x7D)

        doc.add_paragraph("")
        doc.add_paragraph("")

        author_p = doc.add_paragraph()
        author_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        author_p.add_run("Author: Blueprint").font.size = Pt(12)

    def _add_toc_field(self, doc: Document) -> None:
        """Insert an auto-updating Table of Contents field."""
        doc.add_page_break()
        doc.add_heading("Table of Contents", level=1)

        paragraph = doc.add_paragraph()
        run = paragraph.add_run()
        fldChar = OxmlElement("w:fldChar")
        fldChar.set(qn("w:fldCharType"), "begin")
        run._r.append(fldChar)

        instrText = OxmlElement("w:instrText")
        instrText.set(qn("xml:space"), "preserve")
        instrText.text = ' TOC \\o "1-3" \\h \\z \\u '
        run._r.append(instrText)

        fldChar2 = OxmlElement("w:fldChar")
        fldChar2.set(qn("w:fldCharType"), "separate")
        run._r.append(fldChar2)

        # Placeholder text
        run2 = paragraph.add_run("[Update this field to generate Table of Contents]")
        run2.font.color.rgb = RGBColor(0x6C, 0x75, 0x7D)
        run2.font.italic = True

        fldChar3 = OxmlElement("w:fldChar")
        fldChar3.set(qn("w:fldCharType"), "end")
        run3 = paragraph.add_run()
        run3._r.append(fldChar3)

    def _add_executive_summary(
        self, doc: Document, plan: dict[str, Any], brief: dict[str, Any]
    ) -> None:
        """Add the executive summary section."""
        doc.add_page_break()
        doc.add_heading("Executive Summary", level=1)

        tasks = plan.get("tasks") or []
        total = len(tasks)
        completed = sum(1 for t in tasks if t.get("status") == "completed")
        progress = round(completed / total * 100) if total > 0 else 0

        status = plan.get("status") or "draft"
        doc.add_paragraph(
            f"Plan Status: {STATUS_LABELS.get(status, status)}"
        )
        doc.add_paragraph(
            f"Progress: {progress}% ({completed}/{total} tasks completed)"
        )

        if brief.get("problem_statement"):
            doc.add_heading("Problem Statement", level=2)
            doc.add_paragraph(str(brief["problem_statement"]))

        if brief.get("mvp_goal"):
            doc.add_heading("MVP Goal", level=2)
            doc.add_paragraph(str(brief["mvp_goal"]))

        if brief.get("risks"):
            doc.add_heading("Key Risks", level=2)
            for risk in brief["risks"]:
                doc.add_paragraph(str(risk), style="List Bullet")

        # Status summary table
        if tasks:
            doc.add_heading("Status Overview", level=2)
            status_counts = Counter(t.get("status") or "pending" for t in tasks)
            table = doc.add_table(rows=1, cols=2)
            table.style = "Table Grid"
            _set_cell_text(table.rows[0].cells[0], "Status", bold=True)
            _set_cell_text(table.rows[0].cells[1], "Count", bold=True)
            _format_header_row(table.rows[0])

            for s_key in ["completed", "in_progress", "pending", "blocked", "skipped"]:
                count = status_counts.get(s_key, 0)
                if count > 0:
                    row = table.add_row()
                    _set_cell_text(row.cells[0], STATUS_LABELS.get(s_key, s_key))
                    row.cells[1].text = str(count)
                    s_color = STATUS_COLORS.get(s_key)
                    if s_color:
                        _set_cell_shading(row.cells[0], s_color)
                        if s_key in ("completed", "blocked"):
                            for run in row.cells[0].paragraphs[0].runs:
                                run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

    def _add_dependency_matrix(
        self, doc: Document, tasks: list[dict[str, Any]]
    ) -> None:
        """Add a dependency matrix table."""
        doc.add_page_break()
        doc.add_heading("Dependency Matrix", level=1)

        deps_exist = any(task.get("depends_on") for task in tasks)
        if not deps_exist:
            doc.add_paragraph("No dependencies defined.")
            return

        task_map = {t.get("id") or "": t for t in tasks}
        table = doc.add_table(rows=1, cols=3)
        table.style = "Table Grid"

        for i, h in enumerate(["Task", "Depends On", "Status"]):
            _set_cell_text(table.rows[0].cells[i], h, bold=True)
        _format_header_row(table.rows[0])

        for task in tasks:
            deps = task.get("depends_on") or []
            if not deps:
                continue
            for dep_id in deps:
                row = table.add_row()
                task_id = task.get("id") or ""
                task_title = task.get("title") or ""
                row.cells[0].text = f"{task_id}: {task_title}"

                dep_task = task_map.get(dep_id, {})
                dep_title = dep_task.get("title") or str(dep_id)
                row.cells[1].text = f"{dep_id}: {dep_title}"

                dep_status = dep_task.get("status") or "unknown"
                row.cells[2].text = STATUS_LABELS.get(dep_status, dep_status)

        _add_alternating_rows(table)

    def _add_risk_register(
        self, doc: Document, tasks: list[dict[str, Any]]
    ) -> None:
        """Add a risk register table."""
        doc.add_page_break()
        doc.add_heading("Risk Register", level=1)

        risk_tasks = [t for t in tasks if t.get("risk_level")]
        if not risk_tasks:
            doc.add_paragraph("No risk levels assigned.")
            return

        table = doc.add_table(rows=1, cols=4)
        table.style = "Table Grid"

        for i, h in enumerate(["ID", "Title", "Risk Level", "Complexity"]):
            _set_cell_text(table.rows[0].cells[i], h, bold=True)
        _format_header_row(table.rows[0])

        for task in risk_tasks:
            row = table.add_row()
            row.cells[0].text = task.get("id") or ""
            row.cells[1].text = task.get("title") or ""

            risk = (task.get("risk_level") or "").lower()
            _set_cell_text(row.cells[2], risk.capitalize())
            r_color = PRIORITY_COLORS.get(risk)
            if r_color:
                for run in row.cells[2].paragraphs[0].runs:
                    run.font.color.rgb = r_color

            row.cells[3].text = task.get("estimated_complexity") or ""

        _add_alternating_rows(table)

    def _add_resource_allocation(
        self, doc: Document, tasks: list[dict[str, Any]]
    ) -> None:
        """Add resource allocation table grouped by owner type."""
        doc.add_page_break()
        doc.add_heading("Resource Allocation", level=1)

        owner_tasks: dict[str, list[dict[str, Any]]] = {}
        for task in tasks:
            owner = task.get("owner_type") or "unassigned"
            owner_tasks.setdefault(owner, []).append(task)

        if not owner_tasks:
            doc.add_paragraph("No resource data available.")
            return

        table = doc.add_table(rows=1, cols=4)
        table.style = "Table Grid"

        for i, h in enumerate(["Owner", "Task Count", "Est. Hours", "Completed"]):
            _set_cell_text(table.rows[0].cells[i], h, bold=True)
        _format_header_row(table.rows[0])

        for owner, o_tasks in sorted(owner_tasks.items()):
            row = table.add_row()
            row.cells[0].text = owner
            row.cells[1].text = str(len(o_tasks))
            total_hours = sum(t.get("estimated_hours") or 0 for t in o_tasks)
            row.cells[2].text = f"{total_hours:.1f}" if total_hours else "—"
            done = sum(1 for t in o_tasks if t.get("status") == "completed")
            row.cells[3].text = f"{done}/{len(o_tasks)}"

        _add_alternating_rows(table)

    def _add_appendix(
        self, doc: Document, tasks: list[dict[str, Any]]
    ) -> None:
        """Add appendix with detailed task descriptions."""
        doc.add_page_break()
        doc.add_heading("Appendix: Task Details", level=1)

        if not tasks:
            doc.add_paragraph("No tasks to detail.")
            return

        for task in tasks:
            task_id = task.get("id") or ""
            title = task.get("title") or ""
            doc.add_heading(f"{task_id}: {title}", level=2)

            desc = task.get("description") or "No description provided."
            doc.add_paragraph(desc)

            # Acceptance criteria
            criteria = task.get("acceptance_criteria") or []
            if criteria:
                doc.add_heading("Acceptance Criteria", level=3)
                for criterion in criteria:
                    doc.add_paragraph(str(criterion), style="List Bullet")

            # Files
            files = task.get("files_or_modules") or []
            if files:
                doc.add_heading("Files / Modules", level=3)
                for f in files:
                    doc.add_paragraph(str(f), style="List Bullet")

            # Test command
            test_cmd = task.get("test_command")
            if test_cmd:
                doc.add_paragraph(f"Test command: {test_cmd}")

    def _map_content_controls(
        self, doc: Document, plan: dict[str, Any]
    ) -> None:
        """Map plan data to Word content controls (structured document tags).

        Searches for SDT elements with specific aliases and replaces
        their content with plan data. Falls back gracefully if no
        content controls are found.
        """
        mapping = {
            "plan_id": plan.get("id") or "",
            "plan_status": STATUS_LABELS.get(plan.get("status") or "", plan.get("status") or ""),
            "target_engine": plan.get("target_engine") or "",
            "target_repo": plan.get("target_repo") or "",
        }

        body = doc.element.body
        for sdt in body.iter(qn("w:sdt")):
            sdt_pr = sdt.find(qn("w:sdtPr"))
            if sdt_pr is None:
                continue
            alias = sdt_pr.find(qn("w:alias"))
            if alias is None:
                continue
            alias_val = alias.get(qn("w:val"), "")
            if alias_val in mapping:
                sdt_content = sdt.find(qn("w:sdtContent"))
                if sdt_content is not None:
                    for p in sdt_content.findall(qn("w:p")):
                        for r in p.findall(qn("w:r")):
                            for t in r.findall(qn("w:t")):
                                t.text = mapping[alias_val]
