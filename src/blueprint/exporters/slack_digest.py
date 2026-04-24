"""Slack-friendly Markdown digest exporter for execution plans."""

from __future__ import annotations

from collections import Counter, OrderedDict
from typing import Any

from blueprint.exporters.base import TargetExporter


class SlackDigestExporter(TargetExporter):
    """Export compact execution-plan status as Slack-compatible Markdown."""

    STATUS_ORDER = ["pending", "in_progress", "completed", "blocked", "skipped"]

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
        """Export an execution plan Slack digest to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the Slack digest Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        lines = [
            f"# Slack Digest: {plan['id']}",
            f"*Plan:* `{plan['id']}` - {brief['title']}",
            f"*Implementation Brief:* {self._brief_link(brief)}",
            "",
            "## Status Counts",
            self._status_count_line(tasks),
            "",
            "## Ready Tasks",
        ]
        lines.extend(self._grouped_task_lines(self._ready_tasks(tasks), empty="None."))
        lines.extend(["", "## Blocked Tasks"])
        lines.extend(
            self._grouped_task_lines(
                [task for task in tasks if task.get("status") == "blocked"],
                include_blocked_reason=True,
                empty="None.",
            )
        )
        lines.extend(["", "## Next Recommended Tasks"])
        lines.extend(self._next_recommended_lines(plan, tasks))

        return "\n".join(lines).rstrip() + "\n"

    def _brief_link(self, brief: dict[str, Any]) -> str:
        """Render a Slack mrkdwn link for the implementation brief."""
        return f"<blueprint://implementation-brief/{brief['id']}|{brief['title']}>"

    def _status_count_line(self, tasks: list[dict[str, Any]]) -> str:
        """Render status counts in a single compact line."""
        counts = Counter(task.get("status") or "pending" for task in tasks)
        return " | ".join(f"*{status}:* {counts.get(status, 0)}" for status in self.STATUS_ORDER)

    def _ready_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return pending tasks whose dependencies are completed or skipped."""
        tasks_by_id = {task["id"]: task for task in tasks}
        ready_tasks = []
        for task in tasks:
            if task.get("status") != "pending":
                continue

            dependencies = task.get("depends_on") or []
            if all(
                dependency_id in tasks_by_id
                and tasks_by_id[dependency_id].get("status") in {"completed", "skipped"}
                for dependency_id in dependencies
            ):
                ready_tasks.append(task)
        return ready_tasks

    def _grouped_task_lines(
        self,
        tasks: list[dict[str, Any]],
        *,
        include_blocked_reason: bool = False,
        empty: str,
    ) -> list[str]:
        """Render tasks grouped by milestone."""
        if not tasks:
            return [f"- {empty}"]

        lines: list[str] = []
        for milestone, milestone_tasks in self._group_by_milestone(tasks).items():
            lines.append(f"- *{milestone}*")
            for task in milestone_tasks:
                line = f"  - `{task['id']}` {task['title']}"
                if include_blocked_reason:
                    line += f" - {self._blocked_reason(task)}"
                lines.append(line)
        return lines

    def _next_recommended_lines(
        self,
        plan: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render ready tasks grouped by milestone in plan milestone order."""
        ready_tasks = self._ready_tasks(tasks)
        if not ready_tasks:
            return ["- None."]

        milestone_order = self._milestone_order(plan)
        grouped = self._group_by_milestone(ready_tasks)
        ordered_names = [name for name in milestone_order if name in grouped] + [
            name for name in grouped if name not in milestone_order
        ]

        lines: list[str] = []
        for milestone in ordered_names:
            lines.append(f"- *{milestone}*")
            for task in grouped[milestone]:
                dependency_note = self._dependency_note(task)
                lines.append(f"  - `{task['id']}` {task['title']} ({dependency_note})")
        return lines

    def _group_by_milestone(
        self,
        tasks: list[dict[str, Any]],
    ) -> "OrderedDict[str, list[dict[str, Any]]]":
        """Group tasks by milestone while preserving first-seen task order."""
        grouped: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
        for task in tasks:
            milestone = task.get("milestone") or "Ungrouped"
            grouped.setdefault(milestone, []).append(task)
        return grouped

    def _milestone_order(self, plan: dict[str, Any]) -> list[str]:
        """Return milestone names from the plan in declared order."""
        names = []
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            if isinstance(milestone, dict):
                names.append(
                    milestone.get("name") or milestone.get("title") or f"Milestone {index}"
                )
            elif isinstance(milestone, str):
                names.append(milestone)
        return names

    def _dependency_note(self, task: dict[str, Any]) -> str:
        """Render why a task is ready."""
        dependencies = task.get("depends_on") or []
        if dependencies:
            return "deps satisfied: " + ", ".join(dependencies)
        return "no dependencies"

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        """Get the best available blocked reason for a task."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason") or "No reason provided"
