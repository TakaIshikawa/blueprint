"""Milestone burndown CSV exporter for execution plans."""

from __future__ import annotations

import csv
from collections import Counter, OrderedDict
from typing import Any, get_args

from blueprint.domain.models import TaskStatus
from blueprint.exporters.base import TargetExporter


class MilestoneBurndownCsvExporter(TargetExporter):
    """Export execution progress as one CSV summary row per milestone."""

    FIELDNAMES = [
        "Milestone",
        "Total Tasks",
        "Pending",
        "In Progress",
        "Blocked",
        "Skipped",
        "Completed",
        "Completion Percent",
        "Blocked Percent",
    ]
    STATUS_ORDER = list(get_args(TaskStatus))
    UNASSIGNED_MILESTONE = "Unassigned"

    def get_format(self) -> str:
        """Get export format."""
        return "csv"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".csv"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export milestone burndown summary rows to CSV."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            writer.writerows(self.rows(plan, brief))

        return output_path

    def rows(
        self,
        plan: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> list[dict[str, str]]:
        """Build deterministic milestone summary rows for a validated plan."""
        del implementation_brief
        grouped_tasks = self._group_tasks(plan)
        return [
            self._row(milestone, grouped_tasks.get(milestone, []))
            for milestone in self._milestone_order(plan, grouped_tasks)
        ]

    def _group_tasks(
        self,
        plan: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        """Group tasks by milestone, using Unassigned for tasks without one."""
        grouped_tasks: dict[str, list[dict[str, Any]]] = {}
        for task in plan.get("tasks", []):
            milestone = task.get("milestone") or self.UNASSIGNED_MILESTONE
            grouped_tasks.setdefault(milestone, []).append(task)
        return grouped_tasks

    def _milestone_order(
        self,
        plan: dict[str, Any],
        grouped_tasks: dict[str, list[dict[str, Any]]],
    ) -> list[str]:
        """Return milestones in plan order, followed by extras and Unassigned."""
        ordered: OrderedDict[str, None] = OrderedDict()
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            ordered[self._milestone_name(milestone, index)] = None

        task_milestones = set(grouped_tasks)
        for milestone in sorted(
            task_milestones - set(ordered) - {self.UNASSIGNED_MILESTONE}
        ):
            ordered[milestone] = None

        if self.UNASSIGNED_MILESTONE in grouped_tasks:
            ordered[self.UNASSIGNED_MILESTONE] = None

        return list(ordered)

    def _row(self, milestone: str, tasks: list[dict[str, Any]]) -> dict[str, str]:
        """Build one CSV row from a milestone task group."""
        counts = Counter({status: 0 for status in self.STATUS_ORDER})
        counts.update(task.get("status") or "pending" for task in tasks)
        total = len(tasks)
        return {
            "Milestone": milestone,
            "Total Tasks": str(total),
            "Pending": str(counts.get("pending", 0)),
            "In Progress": str(counts.get("in_progress", 0)),
            "Blocked": str(counts.get("blocked", 0)),
            "Skipped": str(counts.get("skipped", 0)),
            "Completed": str(counts.get("completed", 0)),
            "Completion Percent": self._percent(counts.get("completed", 0), total),
            "Blocked Percent": self._percent(counts.get("blocked", 0), total),
        }

    def _percent(self, count: int, total: int) -> str:
        """Format a count as a stable percentage of a milestone total."""
        if total == 0:
            return "0.00%"
        return f"{(count / total) * 100:.2f}%"

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"
