"""Self-contained HTML execution plan report exporter."""

from __future__ import annotations

from collections import Counter
from html import escape
from typing import Any

from blueprint.exporters.base import TargetExporter


class HtmlReportExporter(TargetExporter):
    """Export an execution plan as a portable, escaped HTML report."""

    STATUS_ORDER = ["pending", "in_progress", "completed", "blocked", "skipped"]

    def get_format(self) -> str:
        """Get export format."""
        return "html"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".html"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an execution plan report to HTML."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render a complete HTML report for a validated plan and brief."""
        title = f"Execution Plan Report: {brief['title']}"
        body = "\n".join(
            [
                self._header(plan, brief),
                self._brief_summary(brief),
                self._status_counts(plan.get("tasks", [])),
                self._milestones(plan),
                self._tasks_table(plan.get("tasks", [])),
                self._dependencies(plan.get("tasks", [])),
                self._risks(brief),
                self._validation_plan(plan, brief),
            ]
        )
        return (
            "<!doctype html>\n"
            '<html lang="en">\n'
            "<head>\n"
            '  <meta charset="utf-8">\n'
            '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f"  <title>{self._text(title)}</title>\n"
            f"  <style>{self._css()}</style>\n"
            "</head>\n"
            "<body>\n"
            f"{body}\n"
            "</body>\n"
            "</html>\n"
        )

    def _header(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        return (
            '<header class="report-header" data-section="summary">\n'
            '  <p class="eyebrow">Blueprint execution plan</p>\n'
            f"  <h1>{self._text(brief['title'])}</h1>\n"
            '  <dl class="metadata-grid">\n'
            f"    {self._term('Plan ID', plan['id'])}\n"
            f"    {self._term('Brief ID', brief['id'])}\n"
            f"    {self._term('Status', plan.get('status') or 'N/A')}\n"
            f"    {self._term('Target Engine', plan.get('target_engine') or 'N/A')}\n"
            f"    {self._term('Target Repository', plan.get('target_repo') or 'N/A')}\n"
            f"    {self._term('Project Type', plan.get('project_type') or 'N/A')}\n"
            "  </dl>\n"
            "</header>"
        )

    def _brief_summary(self, brief: dict[str, Any]) -> str:
        return (
            '<section data-section="brief-summary">\n'
            "  <h2>Brief Context</h2>\n"
            f"  {self._field('Problem', brief.get('problem_statement'))}\n"
            f"  {self._field('MVP Goal', brief.get('mvp_goal'))}\n"
            f"  {self._field('Target User', brief.get('target_user'))}\n"
            f"  {self._field('Workflow Context', brief.get('workflow_context'))}\n"
            f"  {self._field('Product Surface', brief.get('product_surface'))}\n"
            "</section>"
        )

    def _status_counts(self, tasks: list[dict[str, Any]]) -> str:
        counts = Counter(task.get("status") or "pending" for task in tasks)
        rows = [self._count_row("total", len(tasks))]
        for status in self.STATUS_ORDER:
            rows.append(self._count_row(status, counts.get(status, 0)))
        for status in sorted(set(counts) - set(self.STATUS_ORDER)):
            rows.append(self._count_row(status, counts[status]))
        return (
            '<section data-section="status-counts">\n'
            "  <h2>Status Counts</h2>\n"
            '  <div class="counts">\n'
            f"{''.join(rows)}"
            "  </div>\n"
            "</section>"
        )

    def _milestones(self, plan: dict[str, Any]) -> str:
        tasks = plan.get("tasks", [])
        rendered_names: set[str] = set()
        cards: list[str] = []
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            name = self._milestone_name(milestone, index)
            rendered_names.add(name)
            milestone_tasks = [task for task in tasks if task.get("milestone") == name]
            cards.append(self._milestone_card(name, milestone, milestone_tasks))

        ungrouped = [
            task for task in tasks if (task.get("milestone") or "") not in rendered_names
        ]
        if ungrouped:
            cards.append(self._milestone_card("Ungrouped", {}, ungrouped))

        content = "\n".join(cards) if cards else "  <p>No milestones defined.</p>"
        return (
            '<section data-section="milestones">\n'
            "  <h2>Milestones</h2>\n"
            f"{content}\n"
            "</section>"
        )

    def _milestone_card(
        self,
        name: str,
        milestone: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> str:
        counts = Counter(task.get("status") or "pending" for task in tasks)
        completed = counts.get("completed", 0)
        total = len(tasks)
        return (
            '  <article class="milestone">\n'
            f"    <h3>{self._text(name)}</h3>\n"
            f"    <p>{self._text(milestone.get('description') or 'N/A')}</p>\n"
            f"    <p><strong>Progress:</strong> {completed}/{total} completed</p>\n"
            f"    <p><strong>Status:</strong> {self._text(self._count_summary(counts))}</p>\n"
            "  </article>"
        )

    def _tasks_table(self, tasks: list[dict[str, Any]]) -> str:
        rows = "\n".join(
            self._task_row(task) for task in sorted(tasks, key=lambda item: item["id"])
        )
        if not rows:
            rows = '      <tr><td colspan="8">No tasks defined.</td></tr>'
        return (
            '<section data-section="tasks">\n'
            "  <h2>Tasks</h2>\n"
            '  <table id="task-table" data-task-table="true">\n'
            "    <thead><tr>"
            "<th>ID</th><th>Title</th><th>Milestone</th><th>Status</th>"
            "<th>Owner</th><th>Engine</th><th>Dependencies</th><th>Acceptance Criteria</th>"
            "</tr></thead>\n"
            "    <tbody>\n"
            f"{rows}\n"
            "    </tbody>\n"
            "  </table>\n"
            "</section>"
        )

    def _task_row(self, task: dict[str, Any]) -> str:
        task_id = str(task["id"])
        return (
            f'      <tr data-task-id="{self._attr(task_id)}">'
            f'<td class="task-id">{self._text(task_id)}</td>'
            f"<td>{self._text(task.get('title') or '')}</td>"
            f"<td>{self._text(task.get('milestone') or 'Ungrouped')}</td>"
            f"<td>{self._text(task.get('status') or 'pending')}</td>"
            f"<td>{self._text(task.get('owner_type') or 'unassigned')}</td>"
            f"<td>{self._text(task.get('suggested_engine') or 'unassigned')}</td>"
            f"<td>{self._text(self._inline_list(task.get('depends_on')))}</td>"
            f"<td>{self._text(self._inline_list(task.get('acceptance_criteria')))}</td>"
            "</tr>"
        )

    def _dependencies(self, tasks: list[dict[str, Any]]) -> str:
        dependency_lines = []
        for task in sorted(tasks, key=lambda item: item["id"]):
            dependencies = task.get("depends_on") or []
            if not dependencies:
                continue
            dependency_lines.append(
                f"<li><strong>{self._text(task['id'])}</strong> depends on "
                f"{self._text(', '.join(str(item) for item in dependencies))}</li>"
            )
        content = "\n".join(dependency_lines) if dependency_lines else "<li>None.</li>"
        return (
            '<section data-section="dependencies">\n'
            "  <h2>Dependency Summary</h2>\n"
            f"  <ul>{content}</ul>\n"
            "</section>"
        )

    def _risks(self, brief: dict[str, Any]) -> str:
        risk_items = [f"<li>{self._text(risk)}</li>" for risk in brief.get("risks") or []]
        content = "\n".join(risk_items) if risk_items else "<li>No risks listed.</li>"
        return (
            '<section data-section="risks">\n'
            "  <h2>Risks</h2>\n"
            f"  <ul>{content}</ul>\n"
            "</section>"
        )

    def _validation_plan(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        return (
            '<section data-section="validation-plan">\n'
            "  <h2>Validation Plan</h2>\n"
            f"  {self._field('Brief Validation', brief.get('validation_plan'))}\n"
            f"  {self._field('Execution Test Strategy', plan.get('test_strategy'))}\n"
            "  "
            f"{self._field('Definition of Done', self._inline_list(brief.get('definition_of_done')))}\n"
            "</section>"
        )

    def _term(self, label: str, value: Any) -> str:
        return f"<dt>{self._text(label)}</dt><dd>{self._text(value)}</dd>"

    def _field(self, label: str, value: Any) -> str:
        return f'<p><strong>{self._text(label)}:</strong> {self._text(value or "N/A")}</p>'

    def _count_row(self, label: str, count: int) -> str:
        return (
            '    <div class="count">'
            f"<span>{self._text(label)}</span><strong>{count}</strong>"
            "</div>\n"
        )

    def _count_summary(self, counts: Counter[str]) -> str:
        if not counts:
            return "none"
        ordered = [
            f"{status}: {counts[status]}"
            for status in self.STATUS_ORDER
            if counts.get(status)
        ]
        ordered.extend(
            f"{status}: {counts[status]}"
            for status in sorted(set(counts) - set(self.STATUS_ORDER))
        )
        return ", ".join(ordered)

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        return str(milestone.get("name") or milestone.get("title") or f"Milestone {index}")

    def _inline_list(self, value: Any) -> str:
        if not value:
            return "None"
        if isinstance(value, list):
            return ", ".join(str(item) for item in value if item is not None) or "None"
        return str(value)

    def _text(self, value: Any) -> str:
        return escape(str(value), quote=True)

    def _attr(self, value: Any) -> str:
        return escape(str(value), quote=True)

    def _css(self) -> str:
        return (
            "body{font-family:Arial,sans-serif;line-height:1.5;color:#17202a;"
            "margin:0;background:#f7f8fa;}header,section{max-width:1100px;margin:0 auto;"
            "padding:24px;}h1,h2,h3{line-height:1.2;}h1{margin:0 0 16px;font-size:32px;}"
            "h2{margin:0 0 12px;font-size:22px;}section{background:#fff;border-top:1px solid #d8dee4;}"
            ".report-header{background:#1f2937;color:#fff;}.eyebrow{text-transform:uppercase;"
            "font-size:12px;letter-spacing:.08em;color:#cbd5e1;}.metadata-grid{display:grid;"
            "grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:0;}"
            "dt{font-size:12px;color:#64748b;}header dt{color:#cbd5e1;}dd{margin:0;font-weight:700;}"
            ".counts{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;}"
            ".count{border:1px solid #d8dee4;border-radius:8px;padding:12px;background:#fbfcfd;}"
            ".count span{display:block;color:#64748b;font-size:12px;text-transform:uppercase;}"
            ".count strong{font-size:24px;}.milestone{border:1px solid #d8dee4;border-radius:8px;"
            "padding:14px;margin:12px 0;background:#fbfcfd;}table{width:100%;border-collapse:collapse;"
            "font-size:14px;}th,td{border:1px solid #d8dee4;padding:8px;text-align:left;vertical-align:top;}"
            "th{background:#eef2f7;}tr:nth-child(even) td{background:#fbfcfd;}.task-id{font-weight:700;}"
        )
