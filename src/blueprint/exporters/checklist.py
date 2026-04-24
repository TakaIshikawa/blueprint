"""Markdown execution checklist exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.exporters.base import TargetExporter


class ChecklistExporter(TargetExporter):
    """Export execution tasks as a milestone-grouped Markdown checklist."""

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
        """Export an execution plan checklist to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the checklist Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        grouped_tasks = self._group_tasks_by_milestone(plan)

        lines = [
            f"# Execution Checklist: {brief['title']}",
            "",
            "## Plan Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            "",
            "## Milestones",
            "",
        ]

        if not tasks:
            lines.append("No execution tasks defined.")
            return "\n".join(lines).rstrip() + "\n"

        for milestone_name, milestone_description, milestone_tasks in grouped_tasks:
            lines.extend(
                [
                    f"### {milestone_name}",
                    "",
                ]
            )
            if milestone_description:
                lines.extend([milestone_description, ""])

            if not milestone_tasks:
                lines.extend(["No tasks.", ""])
                continue

            for task in milestone_tasks:
                lines.extend(self._task_lines(task))
                lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _group_tasks_by_milestone(
        self,
        plan: dict[str, Any],
    ) -> list[tuple[str, str | None, list[dict[str, Any]]]]:
        """Group tasks by milestone while preserving milestone and task order."""
        tasks = plan.get("tasks", [])
        grouped: list[tuple[str, str | None, list[dict[str, Any]]]] = []
        assigned_task_ids: set[str] = set()

        for milestone in plan.get("milestones", []):
            milestone_name = self._milestone_name(milestone)
            if milestone_name is None:
                continue

            milestone_tasks = [task for task in tasks if task.get("milestone") == milestone_name]
            assigned_task_ids.update(task["id"] for task in milestone_tasks)
            grouped.append(
                (
                    milestone_name,
                    self._milestone_description(milestone),
                    milestone_tasks,
                )
            )

        remaining_tasks = [task for task in tasks if task["id"] not in assigned_task_ids]
        for task in remaining_tasks:
            milestone_name = task.get("milestone") or "Ungrouped"
            for index, (group_name, description, milestone_tasks) in enumerate(grouped):
                if group_name == milestone_name:
                    grouped[index] = (group_name, description, [*milestone_tasks, task])
                    break
            else:
                grouped.append((milestone_name, None, [task]))

        return grouped

    def _task_lines(self, task: dict[str, Any]) -> list[str]:
        """Render one task checkbox with handoff context."""
        checked = "x" if task.get("status") == "completed" else " "
        lines = [
            f"- [{checked}] `{task['id']}` {task['title']}",
            f"  - Status: {task.get('status') or 'pending'}",
            f"  - Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            f"  - Dependencies: {self._inline_list(task.get('depends_on'))}",
            f"  - Affected Files: {self._inline_list(task.get('files_or_modules'))}",
            "  - Acceptance Criteria:",
        ]
        lines.extend(self._bullet_lines(task.get("acceptance_criteria"), indent="    "))
        return lines

    def _milestone_name(self, milestone: Any) -> str | None:
        """Return the milestone display name from supported milestone shapes."""
        if isinstance(milestone, dict):
            return milestone.get("name") or milestone.get("id")
        if isinstance(milestone, str):
            return milestone
        return None

    def _milestone_description(self, milestone: Any) -> str | None:
        """Return the milestone description when available."""
        if isinstance(milestone, dict):
            return milestone.get("description")
        return None

    def _inline_list(self, value: list[str] | None) -> str:
        """Render a short list field inline."""
        return ", ".join(value or []) or "none"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render a list field as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]
