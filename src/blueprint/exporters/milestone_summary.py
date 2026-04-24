"""Lead-facing milestone summary exporter for execution plans."""

from __future__ import annotations

from collections import Counter
from typing import Any

from blueprint.exporters.base import TargetExporter


class MilestoneSummaryExporter(TargetExporter):
    """Export a concise Markdown overview grouped by milestone."""

    STATUS_ORDER = ["pending", "in_progress", "blocked", "completed", "skipped"]

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
        """Export an execution plan as a Markdown milestone summary."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render milestone summary Markdown for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        lines = [
            f"# Milestone Summary: {brief['title']}",
            "",
            "## Plan Overview",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            f"- Status Breakdown: {self._status_summary(Counter(self._task_status(task) for task in tasks))}",
            "",
            "## Cross-Milestone Dependencies",
        ]
        lines.extend(self._cross_milestone_dependency_lines(plan))
        lines.extend(["", "## Milestones"])
        lines.extend(self._milestone_sections(plan))

        return "\n".join(lines).rstrip() + "\n"

    def _milestone_sections(self, plan: dict[str, Any]) -> list[str]:
        """Render one summary section for each milestone."""
        tasks = plan.get("tasks", [])
        lines: list[str] = []
        rendered_milestones: set[str] = set()

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            rendered_milestones.add(milestone_name)
            milestone_tasks = [
                task for task in tasks if task.get("milestone") == milestone_name
            ]
            lines.extend(self._milestone_lines(milestone_name, milestone, milestone_tasks))

        ungrouped_tasks = [
            task for task in tasks if (task.get("milestone") or "") not in rendered_milestones
        ]
        if ungrouped_tasks:
            lines.extend(self._milestone_lines("Ungrouped", {}, ungrouped_tasks))

        return lines or ["No milestones defined."]

    def _milestone_lines(
        self,
        milestone_name: str,
        milestone: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render one milestone's task counts, engines, risks, and exit criteria."""
        status_counts = Counter(self._task_status(task) for task in tasks)
        engine_counts = Counter(
            task.get("suggested_engine") or "unassigned" for task in tasks
        )
        lines = [
            "",
            f"### {milestone_name}",
            f"- Description: {milestone.get('description') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            f"- Status Breakdown: {self._status_summary(status_counts)}",
            f"- Suggested Engines: {self._counter_summary(engine_counts)}",
            "- Dependency Highlights:",
        ]
        lines.extend(self._milestone_dependency_lines(milestone_name, tasks))
        lines.append("- Risk Notes:")
        lines.extend(self._risk_lines(tasks))
        lines.append("- Exit Criteria:")
        lines.extend(self._exit_criteria_lines(tasks))
        lines.append("- Tasks:")
        lines.extend(self._task_lines(tasks))
        return lines

    def _cross_milestone_dependency_lines(self, plan: dict[str, Any]) -> list[str]:
        """Render dependencies where source and dependent tasks are in different milestones."""
        tasks = plan.get("tasks", [])
        tasks_by_id = {task["id"]: task for task in tasks}
        lines: list[str] = []
        for task in sorted(tasks, key=lambda item: item["id"]):
            task_milestone = task.get("milestone") or "Ungrouped"
            for dependency_id in task.get("depends_on") or []:
                dependency = tasks_by_id.get(dependency_id)
                if dependency is None:
                    continue
                dependency_milestone = dependency.get("milestone") or "Ungrouped"
                if dependency_milestone == task_milestone:
                    continue
                lines.append(
                    f"- `{task['id']}` ({task_milestone}) depends on "
                    f"`{dependency_id}` ({dependency_milestone})"
                )
        return lines or ["- None."]

    def _milestone_dependency_lines(
        self,
        milestone_name: str,
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render dependency highlights within a milestone section."""
        lines: list[str] = []
        for task in sorted(tasks, key=lambda item: item["id"]):
            dependencies = task.get("depends_on") or []
            if not dependencies:
                continue
            lines.append(f"  - `{task['id']}` depends on {', '.join(dependencies)}")
        if not lines:
            return ["  - None."]

        cross_count = sum(
            1
            for task in tasks
            for dependency_id in task.get("depends_on") or []
            if dependency_id and task.get("milestone") == milestone_name
        )
        lines.append(f"  - Dependency Edges: {cross_count}")
        return lines

    def _risk_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render risk-related task metadata."""
        lines: list[str] = []
        for task in sorted(tasks, key=lambda item: item["id"]):
            notes = self._task_risk_notes(task)
            if notes:
                lines.append(f"  - `{task['id']}` {task['title']}: {'; '.join(notes)}")
        return lines or ["  - None."]

    def _task_risk_notes(self, task: dict[str, Any]) -> list[str]:
        """Extract risk signals from task fields and metadata."""
        metadata = task.get("metadata") or {}
        notes: list[str] = []
        if task.get("status") == "blocked":
            notes.append(f"blocked: {task.get('blocked_reason') or 'No reason provided'}")
        if task.get("estimated_complexity") in {"high", "very_high"}:
            notes.append(f"complexity: {task['estimated_complexity']}")

        for key in ("risk", "risk_level", "risk_note", "risk_notes"):
            value = metadata.get(key)
            if not value:
                continue
            if isinstance(value, list):
                notes.extend(str(item) for item in value if item)
            else:
                notes.append(f"{key}: {value}")
        return notes

    def _exit_criteria_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Derive milestone exit criteria from task acceptance criteria."""
        criteria = [
            criterion
            for task in sorted(tasks, key=lambda item: item["id"])
            for criterion in task.get("acceptance_criteria") or []
        ]
        if not criteria:
            return ["  - None."]
        return [f"  - {criterion}" for criterion in criteria]

    def _task_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render compact task summaries."""
        if not tasks:
            return ["  - None."]
        return [
            (
                f"  - `{task['id']}` {task['title']} "
                f"({self._task_status(task)}, engine: {task.get('suggested_engine') or 'unassigned'})"
            )
            for task in sorted(tasks, key=lambda item: item["id"])
        ]

    def _status_summary(self, status_counts: Counter[str]) -> str:
        """Render status counts in a stable order."""
        return self._counter_summary(status_counts, preferred_order=self.STATUS_ORDER)

    def _counter_summary(
        self,
        counts: Counter[str],
        *,
        preferred_order: list[str] | None = None,
    ) -> str:
        """Render non-zero counter values with stable ordering."""
        if not counts:
            return "none"
        preferred_order = preferred_order or []
        parts = [
            f"{key}: {counts[key]}" for key in preferred_order if counts.get(key)
        ]
        parts.extend(
            f"{key}: {counts[key]}"
            for key in sorted(set(counts) - set(preferred_order))
        )
        return ", ".join(parts)

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _task_status(self, task: dict[str, Any]) -> str:
        """Return a normalized task status."""
        return task.get("status") or "pending"
