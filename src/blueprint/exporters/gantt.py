"""Mermaid Gantt exporter for execution plan schedules."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

from blueprint.exporters.base import TargetExporter


class GanttExporter(TargetExporter):
    """Export execution plans to Mermaid Gantt charts."""

    FALLBACK_START = date(2026, 1, 5)
    DEFAULT_DURATION_DAYS = 1

    def get_format(self) -> str:
        """Get export format."""
        return "mermaid"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".mmd"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an execution plan schedule as Mermaid Gantt text."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render Mermaid Gantt text for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        task_ids = {task["id"] for task in tasks}
        mermaid_ids = {
            task["id"]: self._mermaid_id(task["id"], index)
            for index, task in enumerate(tasks, 1)
        }
        schedule = self._fallback_schedule(tasks, task_ids)

        lines = [
            "gantt",
            f"    title {self._clean_text(brief.get('title') or plan['id'])}",
            "    dateFormat  YYYY-MM-DD",
            "    axisFormat  %Y-%m-%d",
        ]

        rendered_task_ids: set[str] = set()
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            lines.append(f"    section {self._clean_text(milestone_name)}")

            milestone_tasks = [
                task for task in tasks if (task.get("milestone") or "Ungrouped") == milestone_name
            ]
            if not milestone_tasks:
                lines.append("    No tasks :milestone, 0d")
                continue

            for task in milestone_tasks:
                lines.append(self._task_line(task, task_ids, mermaid_ids, schedule))
                rendered_task_ids.add(task["id"])

        ungrouped_tasks = [task for task in tasks if task["id"] not in rendered_task_ids]
        if ungrouped_tasks:
            lines.append("    section Ungrouped")
            for task in ungrouped_tasks:
                lines.append(self._task_line(task, task_ids, mermaid_ids, schedule))

        return "\n".join(lines) + "\n"

    def _task_line(
        self,
        task: dict[str, Any],
        task_ids: set[str],
        mermaid_ids: dict[str, str],
        schedule: dict[str, tuple[date, int]],
    ) -> str:
        """Render one Mermaid Gantt task line."""
        status_tags = self._status_tags(task)
        task_id = task["id"]
        mermaid_id = mermaid_ids[task_id]
        timing = self._timing(task, task_ids, mermaid_ids, schedule)
        fields = [*status_tags, mermaid_id, *timing]
        return f"    {self._clean_text(task['title'])} :{', '.join(fields)}"

    def _timing(
        self,
        task: dict[str, Any],
        task_ids: set[str],
        mermaid_ids: dict[str, str],
        schedule: dict[str, tuple[date, int]],
    ) -> list[str]:
        """Return Mermaid timing fields using dates first, then dependency references."""
        start_date = self._date_value(task, "start_date")
        due_date = self._date_value(task, "due_date")
        duration_days = self._duration_days(task)

        if start_date and due_date:
            if due_date < start_date:
                due_date = start_date
            return [start_date.isoformat(), due_date.isoformat()]

        if start_date:
            return [start_date.isoformat(), f"{duration_days}d"]

        if due_date:
            start_from_due = due_date - timedelta(days=duration_days - 1)
            return [start_from_due.isoformat(), due_date.isoformat()]

        dependency = self._last_known_dependency(task, task_ids)
        if dependency:
            return [f"after {mermaid_ids[dependency]}", f"{duration_days}d"]

        fallback_start, fallback_duration = schedule[task["id"]]
        return [fallback_start.isoformat(), f"{fallback_duration}d"]

    def _fallback_schedule(
        self,
        tasks: list[dict[str, Any]],
        task_ids: set[str],
    ) -> dict[str, tuple[date, int]]:
        """Build deterministic fallback starts for undated tasks."""
        schedule: dict[str, tuple[date, int]] = {}
        completion_by_id: dict[str, date] = {}
        next_start = self.FALLBACK_START

        for task in tasks:
            start_date = self._date_value(task, "start_date")
            due_date = self._date_value(task, "due_date")
            duration_days = self._duration_days(task)

            if start_date is None and due_date is not None:
                start_date = due_date - timedelta(days=duration_days - 1)
            if start_date is None:
                dependency_ends = [
                    completion_by_id[dependency_id] + timedelta(days=1)
                    for dependency_id in task.get("depends_on", []) or []
                    if dependency_id in task_ids and dependency_id in completion_by_id
                ]
                start_date = max([next_start, *dependency_ends])

            end_date = due_date or (start_date + timedelta(days=duration_days - 1))
            if end_date < start_date:
                end_date = start_date
            schedule[task["id"]] = (start_date, max((end_date - start_date).days + 1, 1))
            completion_by_id[task["id"]] = end_date
            next_start = max(next_start, end_date + timedelta(days=1))

        return schedule

    def _status_tags(self, task: dict[str, Any]) -> list[str]:
        """Return Mermaid tags for task status."""
        status = task.get("status") or "pending"
        tags: list[str] = []
        if status == "blocked":
            tags.append("crit")
        if status == "completed":
            tags.append("done")
        if status == "in_progress":
            tags.append("active")
        return tags

    def _last_known_dependency(self, task: dict[str, Any], task_ids: set[str]) -> str | None:
        """Return the final internal dependency for Mermaid after syntax."""
        dependencies = [
            dependency_id
            for dependency_id in task.get("depends_on", []) or []
            if dependency_id in task_ids
        ]
        return dependencies[-1] if dependencies else None

    def _duration_days(self, task: dict[str, Any]) -> int:
        """Read duration_days from task metadata with a deterministic default."""
        metadata = task.get("metadata") or {}
        raw_value = metadata.get("duration_days")
        if raw_value in (None, ""):
            return self.DEFAULT_DURATION_DAYS
        try:
            return max(int(raw_value), 1)
        except (TypeError, ValueError):
            return self.DEFAULT_DURATION_DAYS

    def _date_value(self, task: dict[str, Any], key: str) -> date | None:
        """Read an ISO date from task metadata."""
        metadata = task.get("metadata") or {}
        raw_value = metadata.get(key)
        if isinstance(raw_value, date):
            return raw_value
        if not isinstance(raw_value, str) or not raw_value.strip():
            return None
        try:
            return date.fromisoformat(raw_value.strip())
        except ValueError:
            return None

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _mermaid_id(self, task_id: str, index: int) -> str:
        """Convert task ids into Mermaid-safe section-local identifiers."""
        mermaid_id = re.sub(r"[^0-9A-Za-z_]", "_", task_id)
        if not mermaid_id or mermaid_id[0].isdigit():
            mermaid_id = f"task_{index}_{mermaid_id}"
        return mermaid_id

    def _clean_text(self, value: Any) -> str:
        """Clean text for Mermaid Gantt labels."""
        return re.sub(r"\s+", " ", str(value)).replace(":", "-").strip()
