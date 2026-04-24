"""Notion-friendly Markdown exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.exporters.base import TargetExporter


class NotionMarkdownExporter(TargetExporter):
    """Export execution plans as Markdown structured for Notion import."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an execution plan to Notion-importable Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the Notion Markdown artifact for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        risks = _list_values(brief.get("risks"))
        lines = [
            f"# Execution Plan: {brief['title']}",
            "",
            "## Plan Overview",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            f"- Validation Plan: {brief.get('validation_plan') or 'N/A'}",
            "",
            "## Milestones",
            "",
            "| Milestone | Description | Task Count |",
            "| --- | --- | --- |",
        ]

        lines.extend(self._milestone_rows(plan))
        lines.extend(
            [
                "",
                "## Task Database",
                "",
                (
                    "| Task ID | Title | Status | Owner Type | Suggested Engine | Milestone | "
                    "Dependencies | Files/Modules | Acceptance Criteria |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for task in tasks:
            row = [
                f"`{task['id']}`",
                _table_cell(task["title"]),
                _table_cell(task.get("status") or "pending"),
                _table_cell(task.get("owner_type") or "Unassigned"),
                _table_cell(task.get("suggested_engine") or "Unassigned"),
                _table_cell(task.get("milestone") or "Ungrouped"),
                _table_cell(_inline_list(task.get("depends_on"), empty="none", code=True)),
                _table_cell(_inline_list(task.get("files_or_modules"), empty="none")),
                _table_cell(_inline_list(task.get("acceptance_criteria"), empty="None")),
            ]
            lines.append("| " + " | ".join(row) + " |")

        lines.extend(
            [
                "",
                "## Dependency Table",
                "",
                "| Task ID | Depends On | Dependency Count |",
                "| --- | --- | --- |",
            ]
        )
        if tasks:
            for task in tasks:
                dependencies = task.get("depends_on") or []
                row = [
                    f"`{task['id']}`",
                    _table_cell(_inline_list(dependencies, empty="none", code=True)),
                    str(len(dependencies)),
                ]
                lines.append("| " + " | ".join(row) + " |")
        else:
            lines.append("| none | none | 0 |")

        lines.extend(
            [
                "",
                "## Risks",
                "",
                "| Risk ID | Risk | Mitigation Signal |",
                "| --- | --- | --- |",
            ]
        )
        if risks:
            for index, risk in enumerate(risks, start=1):
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            f"`RISK-{index:03d}`",
                            _table_cell(risk),
                            _table_cell(brief.get("validation_plan") or "Track during execution"),
                        ]
                    )
                    + " |"
                )
        else:
            lines.append("| none | No implementation risks listed | N/A |")

        lines.extend(
            [
                "",
                "## Validation Checklist",
                "",
                f"- [ ] Confirm all {len(tasks)} execution tasks are represented once.",
                "- [ ] Review dependency ordering before work starts.",
                "- [ ] Confirm acceptance criteria are clear enough to validate.",
                f"- [ ] Run validation: `{plan.get('test_strategy') or brief['validation_plan']}`",
            ]
        )

        return "\n".join(lines).rstrip() + "\n"

    def _milestone_rows(self, plan: dict[str, Any]) -> list[str]:
        """Render milestone summary rows while preserving plan order."""
        rows: list[str] = []
        tasks = plan.get("tasks", [])
        seen_milestones: set[str] = set()

        for milestone in plan.get("milestones", []):
            name = _milestone_name(milestone)
            if not name:
                continue
            seen_milestones.add(name)
            description = _milestone_description(milestone)
            task_count = sum(1 for task in tasks if task.get("milestone") == name)
            rows.append(
                "| "
                + " | ".join(
                    [
                        _table_cell(name),
                        _table_cell(description or "N/A"),
                        str(task_count),
                    ]
                )
                + " |"
            )

        ungrouped_count = sum(1 for task in tasks if not task.get("milestone"))
        if ungrouped_count:
            rows.append(f"| Ungrouped | N/A | {ungrouped_count} |")

        remaining = sorted(
            {
                str(task["milestone"])
                for task in tasks
                if task.get("milestone") and task.get("milestone") not in seen_milestones
            }
        )
        for milestone_name in remaining:
            task_count = sum(1 for task in tasks if task.get("milestone") == milestone_name)
            rows.append(f"| {_table_cell(milestone_name)} | N/A | {task_count} |")

        if not rows:
            rows.append("| none | No milestones defined | 0 |")
        return rows


def _milestone_name(milestone: Any) -> str | None:
    if isinstance(milestone, dict):
        value = milestone.get("name") or milestone.get("id")
        return str(value) if value else None
    if isinstance(milestone, str):
        return milestone
    return None


def _milestone_description(milestone: Any) -> str | None:
    if isinstance(milestone, dict) and milestone.get("description"):
        return str(milestone["description"])
    return None


def _inline_list(value: Any, *, empty: str, code: bool = False) -> str:
    items = _list_values(value)
    if not items:
        return empty
    if code:
        return "<br>".join(f"`{item}`" for item in items)
    return "<br>".join(items)


def _list_values(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _table_cell(value: Any) -> str:
    """Escape Markdown table delimiters and normalize line breaks inside cells."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")
