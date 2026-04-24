"""Markdown critical path report exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.audits.critical_path import analyze_critical_path
from blueprint.exporters.base import TargetExporter


class CriticalPathReportExporter(TargetExporter):
    """Export critical path analysis as a shareable Markdown report."""

    INCOMPLETE_STATUSES = {"pending", "in_progress", "blocked"}

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
        """Export critical path analysis to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render critical path report Markdown for a validated plan and brief."""
        result = analyze_critical_path(plan)
        tasks = plan.get("tasks", [])
        tasks_by_id = {task["id"]: task for task in tasks}
        path_ids = set(result.task_ids)
        off_path_tasks = [task for task in tasks if task["id"] not in path_ids]

        lines = [
            f"# Critical Path Report: {brief['title']}",
            "",
            "## Plan Overview",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Tasks: {len(tasks)}",
            f"- Critical Path Length: {len(result.tasks)} tasks",
            f"- Critical Path Weight: {result.total_weight}",
            "",
            "## Critical Path",
        ]
        lines.extend(self._critical_path_lines(result.tasks, tasks_by_id))
        lines.extend(["", "## Blocked Or Incomplete Critical Path Tasks"])
        lines.extend(self._critical_path_attention_lines(result.task_ids, tasks_by_id))
        lines.extend(["", "## Parallelizable Off-Path Tasks"])
        lines.extend(self._off_path_task_lines(off_path_tasks))
        lines.extend(["", "## Per-Task Dependency Details"])
        lines.extend(self._dependency_detail_lines(tasks))

        return "\n".join(lines).rstrip() + "\n"

    def _critical_path_lines(
        self,
        critical_path_tasks: list[Any],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Render critical path tasks in dependency order."""
        if not critical_path_tasks:
            return ["- No tasks found."]

        lines: list[str] = []
        for index, path_task in enumerate(critical_path_tasks, 1):
            task = tasks_by_id[path_task.id]
            markers = self._attention_markers(task)
            marker_text = f" {' '.join(markers)}" if markers else ""
            dependencies = self._dependencies_text(task)
            lines.append(
                f"{index}. `{path_task.id}` {path_task.title}{marker_text} "
                f"(status: {self._task_status(task)}, complexity: "
                f"{path_task.estimated_complexity or 'unspecified'}, weight: "
                f"{path_task.weight}, cumulative: {path_task.cumulative_weight}, "
                f"depends on: {dependencies})"
            )
        return lines

    def _critical_path_attention_lines(
        self,
        task_ids: list[str],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Render blocked and incomplete critical-path tasks."""
        lines: list[str] = []
        for task_id in task_ids:
            task = tasks_by_id[task_id]
            status = self._task_status(task)
            if status == "completed":
                continue

            reason = self._blocked_reason(task)
            if status == "blocked":
                lines.append(
                    f"- **BLOCKED** `{task_id}` {task['title']}: "
                    f"{reason or 'No blocked reason provided.'}"
                )
            else:
                lines.append(
                    f"- **INCOMPLETE** `{task_id}` {task['title']}: status is {status}"
                )
        return lines or ["- None."]

    def _off_path_task_lines(self, off_path_tasks: list[dict[str, Any]]) -> list[str]:
        """Render non-critical tasks separately from the critical path."""
        if not off_path_tasks:
            return ["- None."]

        lines = [
            f"- Non-Critical Task Count: {len(off_path_tasks)}",
            "- Tasks:",
        ]
        for task in sorted(off_path_tasks, key=lambda item: item["id"]):
            lines.append(
                f"  - `{task['id']}` {task['title']} "
                f"(status: {self._task_status(task)}, complexity: "
                f"{task.get('estimated_complexity') or 'unspecified'}, "
                f"depends on: {self._dependencies_text(task)})"
            )
        return lines

    def _dependency_detail_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render dependency and dependent details for every task."""
        if not tasks:
            return ["- No tasks found."]

        dependents_by_task_id = self._dependents_by_task_id(tasks)
        lines: list[str] = []
        for task in sorted(tasks, key=lambda item: item["id"]):
            dependents = dependents_by_task_id.get(task["id"], [])
            lines.append(
                f"- `{task['id']}` {task['title']}: "
                f"depends on {self._dependencies_text(task)}; "
                f"unblocks {self._id_list_text(dependents)}"
            )
        return lines

    def _dependents_by_task_id(
        self,
        tasks: list[dict[str, Any]],
    ) -> dict[str, list[str]]:
        """Build a dependent-task lookup."""
        dependents: dict[str, list[str]] = {}
        for task in tasks:
            for dependency_id in task.get("depends_on") or []:
                dependents.setdefault(str(dependency_id), []).append(task["id"])
        for dependent_ids in dependents.values():
            dependent_ids.sort()
        return dependents

    def _attention_markers(self, task: dict[str, Any]) -> list[str]:
        """Return visible markers for critical-path risk state."""
        status = self._task_status(task)
        if status == "blocked":
            return ["**BLOCKED**"]
        if status in self.INCOMPLETE_STATUSES:
            return ["**INCOMPLETE**"]
        return []

    def _task_status(self, task: dict[str, Any]) -> str:
        """Return the normalized task status."""
        return str(task.get("status") or "pending")

    def _blocked_reason(self, task: dict[str, Any]) -> str | None:
        """Return a blocked reason from top-level or metadata fields."""
        metadata = task.get("metadata") or {}
        reason = task.get("blocked_reason") or metadata.get("blocked_reason")
        return str(reason) if reason else None

    def _dependencies_text(self, task: dict[str, Any]) -> str:
        """Render a task dependency list."""
        return self._id_list_text([str(item) for item in task.get("depends_on") or []])

    def _id_list_text(self, task_ids: list[str]) -> str:
        """Render task IDs as Markdown code references."""
        if not task_ids:
            return "none"
        return ", ".join(f"`{task_id}`" for task_id in task_ids)
