"""Markdown RACI matrix exporter for execution plans."""

from __future__ import annotations

import json
from typing import Any

from blueprint.exporters.base import TargetExporter


class RaciMatrixExporter(TargetExporter):
    """Export task responsibility alignment as a deterministic Markdown table."""

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
        """Export an execution plan RACI matrix to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the RACI matrix Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        lines = [
            f"# RACI Matrix: {brief['title']}",
            "",
            "## Plan Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            "",
            "## Responsibility Matrix",
            "",
            (
                "| Task ID | Task | Responsible | Accountable | Consulted | Informed | "
                "Milestone | Suggested Engine |"
            ),
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]

        for task in tasks:
            row = [
                f"`{_escape_table_cell(task['id'])}`",
                _escape_table_cell(task["title"]),
                _escape_table_cell(self._responsible(task)),
                _escape_table_cell(self._accountable(plan, task)),
                _escape_table_cell(_metadata_value(task, "consulted")),
                _escape_table_cell(_metadata_value(task, "informed")),
                _escape_table_cell(task.get("milestone") or "Ungrouped"),
                _escape_table_cell(task.get("suggested_engine") or "Unassigned"),
            ]
            lines.append("| " + " | ".join(row) + " |")

        return "\n".join(lines).rstrip() + "\n"

    def _responsible(self, task: dict[str, Any]) -> str:
        """Derive the responsible party from task metadata or ownership fields."""
        metadata_responsible = _metadata_value(task, "responsible")
        if metadata_responsible != "N/A":
            return metadata_responsible

        owner_type = task.get("owner_type")
        suggested_engine = task.get("suggested_engine")
        if owner_type and suggested_engine:
            return f"{owner_type}: {suggested_engine}"
        if owner_type:
            return str(owner_type)
        if suggested_engine:
            return str(suggested_engine)
        return "Unassigned"

    def _accountable(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Derive the accountable party from task metadata, plan metadata, or target."""
        task_accountable = _metadata_value(task, "accountable")
        if task_accountable != "N/A":
            return task_accountable

        plan_metadata = plan.get("metadata") or {}
        if isinstance(plan_metadata, dict):
            for key in (
                "accountable",
                "accountable_owner",
                "owner",
                "product_owner",
                "delivery_owner",
            ):
                if key in plan_metadata and plan_metadata[key]:
                    return _format_value(plan_metadata[key])

        if plan.get("target_engine"):
            return f"target engine: {plan['target_engine']}"
        return "N/A"


def _metadata_value(task: dict[str, Any], key: str) -> str:
    """Return a formatted task metadata value or N/A."""
    metadata = task.get("metadata") or {}
    if not isinstance(metadata, dict) or not metadata.get(key):
        return "N/A"
    return _format_value(metadata[key])


def _format_value(value: Any) -> str:
    """Format RACI field values without losing structured metadata."""
    if isinstance(value, str):
        return value.strip() or "N/A"
    if isinstance(value, (list, tuple, set)):
        values = [_format_value(item) for item in value]
        values = [item for item in values if item != "N/A"]
        return ", ".join(values) if values else "N/A"
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if value is None:
        return "N/A"
    return str(value)


def _escape_table_cell(value: Any) -> str:
    """Escape Markdown table delimiters inside a cell."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
