"""Confluence-oriented Markdown exporter for execution plans."""

from __future__ import annotations

from typing import Any, Mapping

from blueprint.exporters.base import TargetExporter
from blueprint.validation_commands import VALIDATION_CATEGORIES, flatten_validation_commands


class ConfluenceMarkdownExporter(TargetExporter):
    """Export execution plans as page-ready Markdown for Confluence."""

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
        """Export an execution plan to Confluence-friendly Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the Confluence Markdown artifact for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        lines = [
            f"# Implementation Plan: {brief['title']}",
            "",
            "## Source Brief Summary",
            f"- Source Brief ID: `{brief['source_brief_id']}`",
            f"- Implementation Brief ID: `{brief['id']}`",
            f"- Execution Plan ID: `{plan['id']}`",
            f"- Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Problem: {_inline_text(brief['problem_statement'])}",
            f"- MVP Goal: {_inline_text(brief['mvp_goal'])}",
            "",
            "## Task Table",
            "",
            (
                "| Task ID | Title | Status | Owner | Engine | Milestone | Dependencies | "
                "Files/Modules | Acceptance Criteria |"
            ),
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]

        if tasks:
            lines.extend(_task_table_rows(tasks))
        else:
            lines.append("| none | No tasks defined | N/A | N/A | N/A | N/A | none | none | None |")

        lines.extend(["", "## Dependencies", ""])
        lines.extend(_dependency_lines(tasks))

        lines.extend(
            [
                "",
                "## Risks",
                "",
                "| Risk ID | Risk | Mitigation Signal |",
                "| --- | --- | --- |",
            ]
        )
        lines.extend(_risk_rows(brief))

        lines.extend(["", "## Acceptance Criteria", ""])
        lines.extend(_acceptance_lines(tasks, brief))

        lines.extend(["", "## Validation Commands", ""])
        lines.extend(_validation_lines(plan, brief))

        return "\n".join(lines).rstrip() + "\n"


def _task_table_rows(tasks: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
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
        rows.append("| " + " | ".join(row) + " |")
    return rows


def _dependency_lines(tasks: list[dict[str, Any]]) -> list[str]:
    if not tasks:
        return ["No task dependencies defined."]

    lines: list[str] = []
    for task in tasks:
        dependencies = _list_values(task.get("depends_on"))
        if dependencies:
            dependency_text = ", ".join(f"`{dependency}`" for dependency in dependencies)
            lines.append(f"- `{task['id']}` depends on {dependency_text}.")
        else:
            lines.append(f"- `{task['id']}` has no dependencies.")
    return lines


def _risk_rows(brief: dict[str, Any]) -> list[str]:
    risks = _list_values(brief.get("risks"))
    if not risks:
        return ["| none | No implementation risks listed | N/A |"]

    mitigation = brief.get("validation_plan") or "Track during implementation"
    return [
        "| "
        + " | ".join(
            [
                f"`RISK-{index:03d}`",
                _table_cell(risk),
                _table_cell(mitigation),
            ]
        )
        + " |"
        for index, risk in enumerate(risks, 1)
    ]


def _acceptance_lines(tasks: list[dict[str, Any]], brief: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if not tasks:
        lines.append("No task acceptance criteria defined.")
    else:
        for task in tasks:
            criteria = _list_values(task.get("acceptance_criteria"))
            lines.append(f"### `{task['id']}` {_inline_text(task['title'])}")
            if criteria:
                lines.extend(f"- {criterion}" for criterion in criteria)
            else:
                lines.append("- None")
            lines.append("")

    definition_of_done = _list_values(brief.get("definition_of_done"))
    lines.append("### Definition of Done")
    if definition_of_done:
        lines.extend(f"- {item}" for item in definition_of_done)
    else:
        lines.append("- None")
    return lines


def _validation_lines(plan: dict[str, Any], brief: dict[str, Any]) -> list[str]:
    lines = [
        f"- Test Strategy: {_inline_text(plan.get('test_strategy') or 'N/A')}",
        f"- Brief Validation Plan: {_inline_text(brief['validation_plan'])}",
    ]
    commands = _validation_commands(plan)
    if not commands:
        lines.append("- Commands: None detected")
        return lines

    lines.append("- Commands:")
    for category in VALIDATION_CATEGORIES:
        for command in commands.get(category, []):
            lines.append(f"  - {category}: `{command}`")
    return lines


def _validation_commands(plan: dict[str, Any]) -> dict[str, list[str]]:
    metadata_commands = (plan.get("metadata") or {}).get("validation_commands")
    commands = _normalized_command_mapping(metadata_commands)
    if not commands:
        commands = {category: [] for category in VALIDATION_CATEGORIES}

    for task in plan.get("tasks", []):
        command = str(task.get("test_command") or "").strip()
        if command:
            commands.setdefault("test", []).append(command)

    return {
        category: _dedupe(commands.get(category, []))
        for category in VALIDATION_CATEGORIES
        if commands.get(category)
    }


def _normalized_command_mapping(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, Mapping):
        return {}
    commands: dict[str, list[str]] = {}
    for category in VALIDATION_CATEGORIES:
        raw_values = value.get(category)
        if isinstance(raw_values, list):
            commands[category] = [str(item).strip() for item in raw_values if str(item).strip()]
    if not flatten_validation_commands(commands):
        return {}
    return commands


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


def _inline_text(value: Any) -> str:
    return str(value).replace("\n", " ").strip()


def _table_cell(value: Any) -> str:
    """Escape Markdown table delimiters and normalize line breaks inside cells."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", "<br>")


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped
