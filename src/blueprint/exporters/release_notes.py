"""Stakeholder-facing release notes exporter for execution plans."""

from __future__ import annotations

from collections import Counter
from typing import Any

from blueprint.exporters.base import TargetExporter


class ReleaseNotesExporter(TargetExporter):
    """Export execution plans as concise Markdown release notes."""

    OPEN_STATUSES = {"pending", "in_progress", "blocked"}

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
        """Export an execution plan as Markdown release notes."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render release notes Markdown for a validated plan and brief."""
        lines = [
            f"# Release Notes: {brief['title']}",
            "",
            "## Summary",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- MVP Goal: {brief.get('mvp_goal') or 'N/A'}",
            f"- Problem: {brief.get('problem_statement') or 'N/A'}",
            f"- Validation Strategy: {plan.get('test_strategy') or brief['validation_plan']}",
            "",
            "## Milestones",
        ]

        lines.extend(self._milestone_sections(plan))
        lines.extend(["", "## Completed Tasks"])
        lines.extend(self._task_summary_lines(plan.get("tasks", []), {"completed"}))
        lines.extend(["", "## Pending Tasks"])
        lines.extend(self._task_summary_lines(plan.get("tasks", []), self.OPEN_STATUSES))
        lines.extend(["", "## Validation Notes"])
        lines.extend(self._validation_lines(plan, brief))
        lines.extend(["", "## Known Risks"])
        lines.extend(self._risk_lines(plan, brief))

        return "\n".join(lines).rstrip() + "\n"

    def _milestone_sections(self, plan: dict[str, Any]) -> list[str]:
        """Render task detail sections grouped by milestone."""
        tasks = plan.get("tasks", [])
        lines: list[str] = []
        rendered_milestones: set[str] = set()

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            rendered_milestones.add(milestone_name)
            milestone_tasks = [
                task for task in tasks if task.get("milestone") == milestone_name
            ]
            lines.extend(self._milestone_task_lines(milestone_name, milestone, milestone_tasks))

        ungrouped_tasks = [
            task for task in tasks if (task.get("milestone") or "") not in rendered_milestones
        ]
        if ungrouped_tasks:
            lines.extend(self._milestone_task_lines("Ungrouped", {}, ungrouped_tasks))

        return lines or ["No milestones defined."]

    def _milestone_task_lines(
        self,
        milestone_name: str,
        milestone: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render one milestone's release-note task details."""
        status_counts = Counter(task.get("status") or "pending" for task in tasks)
        lines = [
            "",
            f"### {milestone_name}",
            f"- Description: {milestone.get('description') or 'N/A'}",
            f"- Task Statuses: {self._status_summary(status_counts)}",
        ]
        if not tasks:
            lines.append("- Tasks: None")
            return lines

        for task in sorted(tasks, key=lambda item: item["id"]):
            lines.extend(
                [
                    "",
                    f"#### {task['id']} - {task['title']}",
                    f"- Status: {task.get('status') or 'pending'}",
                    f"- Description: {task.get('description') or 'N/A'}",
                    "- Acceptance Criteria:",
                ]
            )
            lines.extend(self._bullet_lines(task.get("acceptance_criteria"), indent="  "))
        return lines

    def _task_summary_lines(
        self,
        tasks: list[dict[str, Any]],
        statuses: set[str],
    ) -> list[str]:
        """Render compact task summary lines for selected statuses."""
        matching_tasks = [
            task for task in tasks if (task.get("status") or "pending") in statuses
        ]
        if not matching_tasks:
            return ["- None."]
        return [
            (
                f"- `{task['id']}` {task['title']} "
                f"({task.get('status') or 'pending'}, {task.get('milestone') or 'Ungrouped'})"
            )
            for task in sorted(matching_tasks, key=lambda item: item["id"])
        ]

    def _validation_lines(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> list[str]:
        """Render validation strategy and release acceptance notes."""
        lines = [
            f"- Plan Test Strategy: {plan.get('test_strategy') or 'N/A'}",
            f"- Brief Validation Plan: {brief['validation_plan']}",
            "- Definition of Done:",
        ]
        lines.extend(self._bullet_lines(brief.get("definition_of_done"), indent="  "))
        return lines

    def _risk_lines(self, plan: dict[str, Any], brief: dict[str, Any]) -> list[str]:
        """Render known risks from the brief and blocked execution tasks."""
        risks = list(brief.get("risks") or [])
        blocked_tasks = [
            task for task in plan.get("tasks", []) if task.get("status") == "blocked"
        ]
        risks.extend(
            f"{task['id']} blocked: {task.get('blocked_reason') or 'No reason provided'}"
            for task in blocked_tasks
        )
        return self._bullet_lines(risks)

    def _status_summary(self, status_counts: Counter[str]) -> str:
        """Render task status counts compactly."""
        if not status_counts:
            return "none"
        return ", ".join(
            f"{status}: {count}" for status, count in sorted(status_counts.items())
        )

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render a list field as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]
