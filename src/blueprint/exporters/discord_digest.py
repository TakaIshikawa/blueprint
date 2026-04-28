"""Discord-friendly Markdown digest exporter for execution plans."""

from __future__ import annotations

from collections import Counter, OrderedDict
from typing import Any

from blueprint.exporters.base import TargetExporter


class DiscordDigestExporter(TargetExporter):
    """Export compact execution-plan status as Discord-compatible Markdown."""

    STATUS_ORDER = ["pending", "in_progress", "completed", "blocked", "skipped"]
    MAX_TASK_TITLE_LENGTH = 96
    MAX_REASON_LENGTH = 140
    MAX_SECTION_TASKS = 8

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
        """Export an execution plan Discord digest to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render the Discord digest Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        lines = [
            f"# Discord Digest: {plan['id']}",
            f"**Plan:** `{plan['id']}` - {self._compact_text(brief['title'], 120)}",
            f"**Brief:** `{brief['id']}` - {self._compact_text(brief['title'], 120)}",
            f"**Status:** {self._status_count_line(tasks)}",
            f"**Summary:** {self._summary_line(plan, tasks)}",
            "",
            "## Priority",
        ]
        lines.extend(self._task_lines(self._priority_tasks(tasks), empty="No high-priority work."))
        lines.extend(["", "## Blocked"])
        lines.extend(
            self._task_lines(
                [task for task in tasks if task.get("status") == "blocked"],
                include_blocked_reason=True,
                empty="No blocked tasks.",
            )
        )
        lines.extend(["", "## Upcoming"])
        lines.extend(self._upcoming_milestone_lines(plan, tasks))
        lines.extend(["", "## Ready Next"])
        lines.extend(self._task_lines(self._ready_tasks(tasks), empty="No ready tasks."))

        return "\n".join(lines).rstrip() + "\n"

    def _status_count_line(self, tasks: list[dict[str, Any]]) -> str:
        """Render status counts in a single compact line."""
        counts = Counter(task.get("status") or "pending" for task in tasks)
        return " | ".join(f"**{status}:** {counts.get(status, 0)}" for status in self.STATUS_ORDER)

    def _summary_line(self, plan: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
        """Render a compact plan summary."""
        milestone_count = len(plan.get("milestones", [])) or len(self._group_by_milestone(tasks))
        return f"{len(tasks)} tasks across {milestone_count} milestones"

    def _priority_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return tasks marked as high priority or carrying high risk."""
        return [
            task
            for task in tasks
            if self._is_high_priority(task) and task.get("status") not in {"completed", "skipped"}
        ]

    def _is_high_priority(self, task: dict[str, Any]) -> bool:
        """Return whether a task should appear in the priority section."""
        metadata = task.get("metadata") or {}
        values = [
            task.get("priority"),
            task.get("risk_level"),
            metadata.get("priority"),
            metadata.get("discord_priority"),
            metadata.get("risk_level"),
        ]
        high_values = {"p0", "p1", "urgent", "critical", "high", "highest", "blocker"}
        return any(str(value).strip().lower() in high_values for value in values if value)

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

    def _task_lines(
        self,
        tasks: list[dict[str, Any]],
        *,
        include_blocked_reason: bool = False,
        empty: str,
    ) -> list[str]:
        """Render compact task lines with owner, status, and dependency context."""
        if not tasks:
            return [f"- {empty}"]

        lines = []
        visible_tasks = tasks[: self.MAX_SECTION_TASKS]
        for task in visible_tasks:
            line = (
                f"- `{task['id']}` {self._compact_text(task['title'], self.MAX_TASK_TITLE_LENGTH)}"
                f" - owner: {self._owner_text(task)}"
                f" | status: {task.get('status') or 'pending'}"
                f" | deps: {self._dependency_text(task)}"
            )
            priority = self._priority_text(task)
            if priority:
                line += f" | priority: {priority}"
            if include_blocked_reason:
                blocked_reason = self._compact_text(
                    self._blocked_reason(task),
                    self.MAX_REASON_LENGTH,
                )
                line += f" | blocked: {blocked_reason}"
            lines.append(line)

        hidden_count = len(tasks) - len(visible_tasks)
        if hidden_count > 0:
            lines.append(f"- ... and {hidden_count} more")
        return lines

    def _upcoming_milestone_lines(
        self,
        plan: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render upcoming milestone progress in plan milestone order."""
        grouped = self._group_by_milestone(tasks)
        if not grouped:
            return ["- No milestones with tasks."]

        milestone_order = self._milestone_order(plan)
        ordered_names = [name for name in milestone_order if name in grouped] + [
            name for name in grouped if name not in milestone_order
        ]

        lines = []
        for milestone in ordered_names:
            milestone_tasks = grouped[milestone]
            if all(task.get("status") in {"completed", "skipped"} for task in milestone_tasks):
                continue
            status_counts = Counter(task.get("status") or "pending" for task in milestone_tasks)
            ready_count = len(self._ready_tasks(milestone_tasks))
            lines.append(
                f"- **{milestone}** - "
                f"{status_counts.get('completed', 0)}/{len(milestone_tasks)} completed, "
                f"{ready_count} ready, {status_counts.get('blocked', 0)} blocked"
            )

        return lines or ["- No upcoming milestones."]

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

    def _owner_text(self, task: dict[str, Any]) -> str:
        """Render an owner mention as plain text."""
        metadata = task.get("metadata") or {}
        owner = (
            metadata.get("discord_owner")
            or metadata.get("owner")
            or metadata.get("assignee")
            or task.get("owner_type")
            or "unassigned"
        )
        return self._plain_mention(str(owner))

    def _plain_mention(self, value: str) -> str:
        """Convert Discord mention markup into readable plain text."""
        plain = value.strip()
        if plain.startswith("<@") and plain.endswith(">"):
            plain = "@" + plain.strip("<@!>")
        elif plain.startswith("<#") and plain.endswith(">"):
            plain = "#" + plain.strip("<#>")
        elif plain.startswith("<@&") and plain.endswith(">"):
            plain = "@" + plain.strip("<@&>")
        return plain or "unassigned"

    def _dependency_text(self, task: dict[str, Any]) -> str:
        """Render task dependency context."""
        dependencies = task.get("depends_on") or []
        if dependencies:
            return ", ".join(dependencies)
        return "none"

    def _priority_text(self, task: dict[str, Any]) -> str:
        """Return the first explicit priority-like value for display."""
        metadata = task.get("metadata") or {}
        for value in (
            task.get("priority"),
            metadata.get("discord_priority"),
            metadata.get("priority"),
            task.get("risk_level"),
            metadata.get("risk_level"),
        ):
            if value:
                return str(value)
        return ""

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        """Get the best available blocked reason for a task."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason") or "No reason provided"

    def _compact_text(self, value: Any, max_length: int) -> str:
        """Render a single-line value with deterministic truncation."""
        text = " ".join(str(value).split())
        if len(text) <= max_length:
            return text
        return text[: max_length - 3].rstrip() + "..."
