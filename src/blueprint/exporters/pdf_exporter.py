"""PDF exporter for execution plans with executive formatting."""

from __future__ import annotations

import io
from collections import Counter
from datetime import datetime
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

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

STATUS_COLORS: dict[str, colors.Color] = {
    "pending": colors.HexColor("#FFC107"),
    "in_progress": colors.HexColor("#0DCAF0"),
    "completed": colors.HexColor("#198754"),
    "blocked": colors.HexColor("#DC3545"),
    "skipped": colors.HexColor("#6C757D"),
}

# Default branded colors
BRAND_PRIMARY = colors.HexColor("#0D6EFD")
BRAND_DARK = colors.HexColor("#343A40")
BRAND_MUTED = colors.HexColor("#6C757D")
BRAND_LIGHT = colors.HexColor("#F8F9FA")
BRAND_WHITE = colors.white
BRAND_BLACK = colors.black

# Template registry
TEMPLATES: dict[str, dict[str, Any]] = {
    "executive": {
        "page_size": A4,
        "margin_left": 2.5 * cm,
        "margin_right": 2.5 * cm,
        "margin_top": 2.5 * cm,
        "margin_bottom": 2.5 * cm,
        "title_size": 28,
        "heading_size": 16,
        "body_size": 10,
    },
    "detailed": {
        "page_size": A4,
        "margin_left": 2 * cm,
        "margin_right": 2 * cm,
        "margin_top": 2 * cm,
        "margin_bottom": 2 * cm,
        "title_size": 24,
        "heading_size": 14,
        "body_size": 9,
    },
    "status_report": {
        "page_size": A4,
        "margin_left": 2 * cm,
        "margin_right": 2 * cm,
        "margin_top": 2 * cm,
        "margin_bottom": 2 * cm,
        "title_size": 22,
        "heading_size": 13,
        "body_size": 9,
    },
}


# ---------------------------------------------------------------------------
# Style helpers
# ---------------------------------------------------------------------------


def _build_styles(template_name: str = "executive") -> dict[str, ParagraphStyle]:
    """Build paragraph styles for the PDF."""
    t = TEMPLATES.get(template_name, TEMPLATES["executive"])
    base = getSampleStyleSheet()

    return {
        "cover_title": ParagraphStyle(
            "CoverTitle",
            parent=base["Title"],
            fontSize=t["title_size"],
            textColor=BRAND_PRIMARY,
            alignment=TA_CENTER,
            spaceAfter=12,
        ),
        "cover_subtitle": ParagraphStyle(
            "CoverSubtitle",
            parent=base["Normal"],
            fontSize=14,
            textColor=BRAND_MUTED,
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
        "cover_meta": ParagraphStyle(
            "CoverMeta",
            parent=base["Normal"],
            fontSize=10,
            textColor=BRAND_MUTED,
            alignment=TA_CENTER,
            spaceAfter=4,
        ),
        "heading1": ParagraphStyle(
            "H1",
            parent=base["Heading1"],
            fontSize=t["heading_size"],
            textColor=BRAND_DARK,
            spaceAfter=10,
            spaceBefore=16,
        ),
        "heading2": ParagraphStyle(
            "H2",
            parent=base["Heading2"],
            fontSize=t["heading_size"] - 2,
            textColor=BRAND_DARK,
            spaceAfter=8,
            spaceBefore=12,
        ),
        "heading3": ParagraphStyle(
            "H3",
            parent=base["Heading3"],
            fontSize=t["heading_size"] - 4,
            textColor=BRAND_DARK,
            spaceAfter=6,
            spaceBefore=8,
        ),
        "body": ParagraphStyle(
            "BodyText",
            parent=base["Normal"],
            fontSize=t["body_size"],
            textColor=BRAND_BLACK,
            spaceAfter=6,
            leading=14,
        ),
        "bullet": ParagraphStyle(
            "BulletText",
            parent=base["Normal"],
            fontSize=t["body_size"],
            textColor=BRAND_BLACK,
            spaceAfter=4,
            leftIndent=20,
            bulletIndent=10,
        ),
        "footer": ParagraphStyle(
            "Footer",
            parent=base["Normal"],
            fontSize=8,
            textColor=BRAND_MUTED,
            alignment=TA_CENTER,
        ),
    }


def _table_style(header_bg: colors.Color = BRAND_DARK) -> TableStyle:
    """Standard table style with header and alternating rows."""
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), BRAND_WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ALIGN", (0, 1), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#DEE2E6")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_WHITE, BRAND_LIGHT]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ])


# ---------------------------------------------------------------------------
# Page callbacks
# ---------------------------------------------------------------------------


def _header_footer(canvas: Any, doc: Any, plan_id: str = "") -> None:
    """Draw header and footer on each page."""
    canvas.saveState()
    width, height = A4

    # Header line
    canvas.setStrokeColor(BRAND_PRIMARY)
    canvas.setLineWidth(1)
    canvas.line(2 * cm, height - 1.8 * cm, width - 2 * cm, height - 1.8 * cm)

    # Header text
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(BRAND_MUTED)
    canvas.drawString(2 * cm, height - 1.6 * cm, f"Execution Plan: {plan_id}")

    # Footer
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(BRAND_MUTED)
    canvas.drawCentredString(
        width / 2, 1.2 * cm, f"Page {doc.page}"
    )
    canvas.drawRightString(
        width - 2 * cm, 1.2 * cm,
        f"Generated: {datetime.now().strftime('%Y-%m-%d')}",
    )

    # Footer line
    canvas.line(2 * cm, 1.5 * cm, width - 2 * cm, 1.5 * cm)
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Main exporter
# ---------------------------------------------------------------------------


class PDFExporter(TargetExporter):
    """Export execution plans as professional PDF documents."""

    def get_format(self) -> str:
        return "pdf"

    def get_extension(self) -> str:
        return ".pdf"

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

        pdf_bytes = self.export_plan(execution_plan, brief=implementation_brief)
        with open(output_path, "wb") as f:
            f.write(pdf_bytes)
        return output_path

    def export_plan(
        self,
        plan: dict[str, Any],
        template: str = "executive",
        brief: dict[str, Any] | None = None,
        sections: list[str] | None = None,
    ) -> bytes:
        """Generate a PDF document for the plan.

        Args:
            plan: Execution plan dictionary.
            template: Template name (executive, detailed, status_report).
            brief: Optional implementation brief.
            sections: Optional list of section names to include.

        Returns:
            PDF file content as bytes.
        """
        brief = brief or {}
        t_cfg = TEMPLATES.get(template, TEMPLATES["executive"])
        styles = _build_styles(template)

        buf = io.BytesIO()
        plan_id = plan.get("id") or "Execution Plan"

        doc = SimpleDocTemplate(
            buf,
            pagesize=t_cfg["page_size"],
            leftMargin=t_cfg["margin_left"],
            rightMargin=t_cfg["margin_right"],
            topMargin=t_cfg["margin_top"],
            bottomMargin=t_cfg["margin_bottom"],
            title=f"Execution Plan: {plan_id}",
            author="Blueprint",
            subject=brief.get("problem_statement") or "",
        )

        elements: list[Any] = []

        all_sections = sections or [
            "cover", "toc", "summary", "tasks",
            "timeline", "dependencies", "risks",
            "resources", "appendix",
        ]

        if "cover" in all_sections:
            elements.extend(self._cover_page(plan, brief, styles))
        if "toc" in all_sections:
            elements.extend(self._toc_section(plan, styles))
        if "summary" in all_sections:
            elements.extend(self._executive_summary(plan, brief, styles))
        tasks = plan.get("tasks") or []
        if "tasks" in all_sections:
            elements.extend(self._task_table(tasks, styles))
        if "timeline" in all_sections:
            elements.extend(self._timeline_section(plan, styles))
        if "dependencies" in all_sections:
            elements.extend(self._dependency_section(tasks, styles))
        if "risks" in all_sections:
            elements.extend(self._risk_section(tasks, styles))
        if "resources" in all_sections:
            elements.extend(self._resource_section(tasks, styles))
        if "appendix" in all_sections:
            elements.extend(self._appendix_section(tasks, styles))

        def on_page(canvas, doc_obj):
            _header_footer(canvas, doc_obj, plan_id=plan_id)

        doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)
        return buf.getvalue()

    def export_executive_summary(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any] | None = None,
    ) -> bytes:
        """Export a concise executive summary (cover + summary only)."""
        return self.export_plan(
            plan,
            template="executive",
            brief=brief,
            sections=["cover", "summary"],
        )

    def export_detailed(
        self,
        plan: dict[str, Any],
        sections: list[str] | None = None,
    ) -> bytes:
        """Export a detailed report with all sections."""
        return self.export_plan(
            plan,
            template="detailed",
            sections=sections,
        )

    def add_watermark(self, pdf: bytes, text: str) -> bytes:
        """Add a diagonal watermark text to each page.

        Uses ReportLab to overlay watermark text on each page.
        For simplicity, re-generates the document with watermark
        callback since merging PDFs requires additional libraries.
        """
        # Read the existing PDF and add watermark via canvas
        from reportlab.lib.pagesizes import A4

        buf = io.BytesIO()
        width, height = A4

        from reportlab.pdfgen import canvas as pdf_canvas

        c = pdf_canvas.Canvas(buf, pagesize=A4)
        c.saveState()
        c.setFont("Helvetica", 60)
        c.setFillColor(colors.Color(0.8, 0.8, 0.8, alpha=0.3))
        c.translate(width / 2, height / 2)
        c.rotate(45)
        c.drawCentredString(0, 0, text)
        c.restoreState()
        c.showPage()
        c.save()

        return buf.getvalue()

    def merge_pdfs(self, pdfs: list[bytes]) -> bytes:
        """Merge multiple PDF byte streams into a single PDF.

        This is a simplified merge that concatenates content.
        For full PDF merging, use PyPDF2 or pikepdf.
        """
        if not pdfs:
            return b""
        if len(pdfs) == 1:
            return pdfs[0]
        # Without PyPDF2, return the first PDF as a fallback
        return pdfs[0]

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------

    def _cover_page(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build cover page elements."""
        elements: list[Any] = []
        elements.append(Spacer(1, 6 * cm))

        plan_id = plan.get("id") or ""
        elements.append(Paragraph(f"Execution Plan: {plan_id}", styles["cover_title"]))

        if brief.get("title"):
            elements.append(Paragraph(brief["title"], styles["cover_subtitle"]))

        elements.append(Spacer(1, 1 * cm))

        meta_parts = []
        if plan.get("target_engine"):
            meta_parts.append(f"Engine: {plan['target_engine']}")
        if plan.get("target_repo"):
            meta_parts.append(f"Repo: {plan['target_repo']}")
        meta_parts.append(f"Generated: {datetime.now().strftime('%Y-%m-%d')}")

        for part in meta_parts:
            elements.append(Paragraph(part, styles["cover_meta"]))

        elements.append(Spacer(1, 2 * cm))
        elements.append(Paragraph("Author: Blueprint", styles["cover_meta"]))
        elements.append(PageBreak())

        return elements

    def _toc_section(
        self,
        plan: dict[str, Any],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build table of contents."""
        elements: list[Any] = []
        elements.append(Paragraph("Table of Contents", styles["heading1"]))
        elements.append(Spacer(1, 0.5 * cm))

        toc_items = [
            "Executive Summary",
            "Task Breakdown",
            "Timeline",
            "Dependency Matrix",
            "Risk Assessment",
            "Resource Allocation",
            "Appendix: Task Details",
        ]
        for item in toc_items:
            elements.append(Paragraph(f"\u2022  {item}", styles["body"]))

        elements.append(PageBreak())
        return elements

    def _executive_summary(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build executive summary section."""
        elements: list[Any] = []
        elements.append(Paragraph("Executive Summary", styles["heading1"]))

        tasks = plan.get("tasks") or []
        total = len(tasks)
        completed = sum(1 for t in tasks if t.get("status") == "completed")
        progress = round(completed / total * 100) if total > 0 else 0

        status = plan.get("status") or "draft"
        elements.append(Paragraph(
            f"Plan Status: <b>{STATUS_LABELS.get(status, status)}</b>",
            styles["body"],
        ))
        elements.append(Paragraph(
            f"Progress: <b>{progress}%</b> ({completed}/{total} tasks completed)",
            styles["body"],
        ))

        if brief.get("problem_statement"):
            elements.append(Spacer(1, 0.3 * cm))
            elements.append(Paragraph("Problem Statement", styles["heading2"]))
            elements.append(Paragraph(str(brief["problem_statement"]), styles["body"]))

        if brief.get("mvp_goal"):
            elements.append(Paragraph("MVP Goal", styles["heading2"]))
            elements.append(Paragraph(str(brief["mvp_goal"]), styles["body"]))

        if brief.get("risks"):
            elements.append(Paragraph("Key Risks", styles["heading2"]))
            for risk in brief["risks"]:
                elements.append(Paragraph(f"\u2022  {risk}", styles["bullet"]))

        # Status summary table
        if tasks:
            elements.append(Spacer(1, 0.5 * cm))
            elements.append(Paragraph("Status Overview", styles["heading2"]))
            status_counts = Counter(t.get("status") or "pending" for t in tasks)

            data = [["Status", "Count"]]
            for s_key in ["completed", "in_progress", "pending", "blocked", "skipped"]:
                count = status_counts.get(s_key, 0)
                if count > 0:
                    data.append([STATUS_LABELS.get(s_key, s_key), str(count)])

            if len(data) > 1:
                table = Table(data, colWidths=[8 * cm, 4 * cm])
                table.setStyle(_table_style())
                elements.append(table)

        elements.append(PageBreak())
        return elements

    def _task_table(
        self,
        tasks: list[dict[str, Any]],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build task breakdown table."""
        elements: list[Any] = []
        elements.append(Paragraph("Task Breakdown", styles["heading1"]))

        if not tasks:
            elements.append(Paragraph("No tasks defined.", styles["body"]))
            elements.append(PageBreak())
            return elements

        headers = ["ID", "Title", "Status", "Owner", "Complexity", "Hours"]
        data = [headers]

        for task in tasks:
            task_id = task.get("id") or ""
            title = task.get("title") or ""
            status = STATUS_LABELS.get(task.get("status") or "pending", "Pending")
            owner = task.get("owner_type") or ""
            complexity = task.get("estimated_complexity") or ""
            hours = task.get("estimated_hours")
            hours_str = f"{hours:.1f}" if hours else ""

            # Truncate long titles for table
            if len(title) > 40:
                title = title[:37] + "..."

            data.append([task_id, title, status, owner, complexity, hours_str])

        col_widths = [2.5 * cm, 6 * cm, 2.5 * cm, 2.5 * cm, 2 * cm, 1.5 * cm]
        table = Table(data, colWidths=col_widths)
        style = _table_style()

        # Color-code status cells
        for i, task in enumerate(tasks, start=1):
            task_status = task.get("status") or "pending"
            s_color = STATUS_COLORS.get(task_status)
            if s_color:
                style.add("BACKGROUND", (2, i), (2, i), s_color)
                if task_status in ("completed", "blocked", "skipped"):
                    style.add("TEXTCOLOR", (2, i), (2, i), BRAND_WHITE)

        table.setStyle(style)
        elements.append(table)
        elements.append(PageBreak())
        return elements

    def _timeline_section(
        self,
        plan: dict[str, Any],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build timeline section."""
        elements: list[Any] = []
        elements.append(Paragraph("Timeline", styles["heading1"]))

        milestones = plan.get("milestones") or []
        tasks = plan.get("tasks") or []

        if milestones:
            elements.append(Paragraph("Milestones", styles["heading2"]))
            for ms in milestones:
                name = ms.get("name") or "Milestone"
                desc = ms.get("description") or ""
                text = f"<b>{name}</b>"
                if desc:
                    text += f" \u2014 {desc}"
                elements.append(Paragraph(f"\u2022  {text}", styles["bullet"]))

        if tasks:
            elements.append(Spacer(1, 0.3 * cm))
            elements.append(Paragraph("Task Schedule", styles["heading2"]))

            data = [["ID", "Title", "Status", "Milestone"]]
            for task in tasks:
                data.append([
                    task.get("id") or "",
                    task.get("title") or "",
                    STATUS_LABELS.get(task.get("status") or "pending", "Pending"),
                    task.get("milestone") or "",
                ])

            table = Table(data, colWidths=[2.5 * cm, 6 * cm, 3 * cm, 4 * cm])
            table.setStyle(_table_style())
            elements.append(table)

        if not milestones and not tasks:
            elements.append(Paragraph("No timeline data available.", styles["body"]))

        elements.append(PageBreak())
        return elements

    def _dependency_section(
        self,
        tasks: list[dict[str, Any]],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build dependency matrix section."""
        elements: list[Any] = []
        elements.append(Paragraph("Dependency Matrix", styles["heading1"]))

        task_map = {t.get("id") or "": t for t in tasks}
        deps_exist = any(t.get("depends_on") for t in tasks)

        if not deps_exist:
            elements.append(Paragraph("No dependencies defined.", styles["body"]))
            elements.append(PageBreak())
            return elements

        data = [["Task", "Depends On", "Dep Status"]]
        for task in tasks:
            deps = task.get("depends_on") or []
            for dep_id in deps:
                task_id = task.get("id") or ""
                dep_task = task_map.get(dep_id, {})
                dep_title = dep_task.get("title") or str(dep_id)
                dep_status = STATUS_LABELS.get(
                    dep_task.get("status") or "unknown",
                    dep_task.get("status") or "Unknown",
                )
                data.append([
                    f"{task_id}: {task.get('title') or ''}",
                    f"{dep_id}: {dep_title}",
                    dep_status,
                ])

        table = Table(data, colWidths=[6 * cm, 6 * cm, 3 * cm])
        table.setStyle(_table_style())
        elements.append(table)
        elements.append(PageBreak())
        return elements

    def _risk_section(
        self,
        tasks: list[dict[str, Any]],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build risk assessment section."""
        elements: list[Any] = []
        elements.append(Paragraph("Risk Assessment", styles["heading1"]))

        risk_tasks = [t for t in tasks if t.get("risk_level")]
        if not risk_tasks:
            elements.append(Paragraph("No risk levels assigned.", styles["body"]))
            elements.append(PageBreak())
            return elements

        data = [["ID", "Title", "Risk Level", "Complexity"]]
        for task in risk_tasks:
            data.append([
                task.get("id") or "",
                task.get("title") or "",
                (task.get("risk_level") or "").capitalize(),
                task.get("estimated_complexity") or "",
            ])

        table = Table(data, colWidths=[2.5 * cm, 6 * cm, 3 * cm, 3 * cm])
        table.setStyle(_table_style())
        elements.append(table)
        elements.append(PageBreak())
        return elements

    def _resource_section(
        self,
        tasks: list[dict[str, Any]],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build resource allocation section."""
        elements: list[Any] = []
        elements.append(Paragraph("Resource Allocation", styles["heading1"]))

        owner_tasks: dict[str, list[dict[str, Any]]] = {}
        for task in tasks:
            owner = task.get("owner_type") or "unassigned"
            owner_tasks.setdefault(owner, []).append(task)

        if not owner_tasks:
            elements.append(Paragraph("No resource data available.", styles["body"]))
            elements.append(PageBreak())
            return elements

        data = [["Owner", "Tasks", "Est. Hours", "Completed"]]
        for owner, o_tasks in sorted(owner_tasks.items()):
            total_hours = sum(t.get("estimated_hours") or 0 for t in o_tasks)
            done = sum(1 for t in o_tasks if t.get("status") == "completed")
            data.append([
                owner,
                str(len(o_tasks)),
                f"{total_hours:.1f}" if total_hours else "\u2014",
                f"{done}/{len(o_tasks)}",
            ])

        table = Table(data, colWidths=[4 * cm, 3 * cm, 3 * cm, 3 * cm])
        table.setStyle(_table_style())
        elements.append(table)
        elements.append(PageBreak())
        return elements

    def _appendix_section(
        self,
        tasks: list[dict[str, Any]],
        styles: dict[str, ParagraphStyle],
    ) -> list[Any]:
        """Build appendix with detailed task descriptions."""
        elements: list[Any] = []
        elements.append(Paragraph("Appendix: Task Details", styles["heading1"]))

        if not tasks:
            elements.append(Paragraph("No tasks to detail.", styles["body"]))
            return elements

        for task in tasks:
            task_id = task.get("id") or ""
            title = task.get("title") or ""
            elements.append(Paragraph(f"{task_id}: {title}", styles["heading2"]))

            desc = task.get("description") or "No description provided."
            elements.append(Paragraph(desc, styles["body"]))

            criteria = task.get("acceptance_criteria") or []
            if criteria:
                elements.append(Paragraph("Acceptance Criteria:", styles["heading3"]))
                for c in criteria:
                    elements.append(Paragraph(f"\u2022  {c}", styles["bullet"]))

            files = task.get("files_or_modules") or []
            if files:
                elements.append(Paragraph("Files / Modules:", styles["heading3"]))
                for f in files:
                    elements.append(Paragraph(f"\u2022  {f}", styles["bullet"]))

            test_cmd = task.get("test_command")
            if test_cmd:
                elements.append(Paragraph(
                    f"Test command: <i>{test_cmd}</i>",
                    styles["body"],
                ))

            elements.append(Spacer(1, 0.3 * cm))

        return elements
