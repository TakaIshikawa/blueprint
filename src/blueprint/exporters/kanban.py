"""Markdown Kanban board exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.exporters.base import TargetExporter


class KanbanExporter(TargetExporter):
    """Export execution tasks as a workflow-oriented Markdown Kanban board."""

    STATUS_COLUMNS = ["pending", "in_progress", "blocked", "completed", "skipped"]

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
        """Export an execution plan Kanban board to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the Kanban board Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        tasks_by_id = {task["id"]: task for task in tasks}
        grouped_tasks = self._group_tasks(tasks)

        lines = [
            f"# Execution Plan Kanban Board: {plan['id']}",
            "",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Title: {brief['title']}",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            "",
        ]

        for status in self.STATUS_COLUMNS:
            column_tasks = grouped_tasks[status]
            lines.extend(
                [
                    f"## {status}",
                    "",
                    f"_Tasks: {len(column_tasks)}_",
                    "",
                ]
            )
            if not column_tasks:
                lines.extend(["No tasks.", ""])
                continue

            for task in column_tasks:
                lines.extend(self._task_card_lines(task, tasks_by_id))
                lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _group_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Group tasks into stable Kanban status columns."""
        grouped_tasks = {status: [] for status in self.STATUS_COLUMNS}
        for task in sorted(tasks, key=lambda item: item["id"]):
            status = task.get("status") or "pending"
            grouped_tasks[status].append(task)
        return grouped_tasks

    def _task_card_lines(
        self,
        task: dict[str, Any],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Render a compact Markdown card for one task."""
        dependencies = task.get("depends_on") or []
        lines = [
            f"### `{task['id']}` - {task['title']}",
            f"- Milestone: {task.get('milestone') or 'Ungrouped'}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            f"- Dependencies: {self._dependency_context(dependencies, tasks_by_id)}",
        ]

        if task.get("status") == "blocked":
            lines.append(f"- Blocked Reason: {self._blocked_reason(task)}")

        lines.append(
            f"- Acceptance Criteria: {len(task.get('acceptance_criteria') or [])}"
        )
        return lines

    def _dependency_context(
        self,
        dependencies: list[str],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> str:
        """Render dependency IDs with their current task statuses."""
        if not dependencies:
            return "none"

        parts = []
        for dependency_id in dependencies:
            dependency = tasks_by_id.get(dependency_id)
            if dependency:
                parts.append(f"{dependency_id} ({dependency.get('status') or 'pending'})")
            else:
                parts.append(f"{dependency_id} (missing)")
        return ", ".join(parts)

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        """Return blocked reason from canonical task fields or metadata."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason") or "N/A"
