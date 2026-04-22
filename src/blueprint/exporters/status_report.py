"""Markdown status report exporter for execution plans."""

from collections import Counter
from typing import Any

from blueprint.domain import ExecutionPlan, ImplementationBrief
from blueprint.exporters.base import TargetExporter


class StatusReportExporter(TargetExporter):
    """Export execution-plan progress as a Markdown status report."""

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
        """Export an execution plan status report to Markdown."""
        raw_plan = dict(execution_plan)
        execution_plan, implementation_brief = self._validated_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        content = self._build_report(execution_plan, implementation_brief, raw_plan)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def _validated_payload(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Validate supported plan fields while allowing report-only metadata."""
        plan_fields = ExecutionPlan.model_fields
        plan_payload = {
            key: value for key, value in execution_plan.items() if key in plan_fields
        }
        plan = ExecutionPlan.model_validate(plan_payload).model_dump(mode="python")
        brief = ImplementationBrief.model_validate(implementation_brief).model_dump(
            mode="python"
        )
        return plan, brief

    def _build_report(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        raw_plan: dict[str, Any],
    ) -> str:
        """Build the full Markdown report."""
        lines = [
            f"# Execution Plan Status Report: {plan['id']}",
            "",
            "## Plan Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repo: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Implementation Brief ID: `{plan['implementation_brief_id']}`",
            f"- Created: {plan.get('created_at') or 'N/A'}",
            f"- Updated: {plan.get('updated_at') or 'N/A'}",
            "",
            "## Implementation Brief Summary",
            f"- Title: {brief['title']}",
            f"- Domain: {brief.get('domain') or 'N/A'}",
            f"- Target User: {brief.get('target_user') or 'N/A'}",
            f"- MVP Goal: {brief.get('mvp_goal') or 'N/A'}",
            f"- Problem Statement: {brief.get('problem_statement') or 'N/A'}",
            f"- Validation Plan: {brief.get('validation_plan') or 'N/A'}",
            "",
            "## Task Counts By Status",
        ]

        lines.extend(self._task_count_lines(plan.get("tasks", [])))
        lines.extend(["", "## Milestone Progress"])
        lines.extend(self._milestone_progress_lines(plan))
        lines.extend(["", "## Blocked Tasks"])
        lines.extend(self._blocked_task_lines(plan.get("tasks", [])))
        lines.extend(["", "## Ready Tasks"])
        lines.extend(self._ready_task_lines(plan.get("tasks", [])))

        recent_exports = self._recent_export_metadata(raw_plan)
        if recent_exports:
            lines.extend(["", "## Recent Export Metadata"])
            lines.extend(self._recent_export_lines(recent_exports))

        return "\n".join(lines) + "\n"

    def _task_count_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render status counts in a stable order."""
        counts = Counter(task.get("status") or "pending" for task in tasks)
        lines = [f"- Total: {len(tasks)}"]
        for status in self.STATUS_ORDER:
            lines.append(f"- {status}: {counts.get(status, 0)}")
        for status in sorted(set(counts) - set(self.STATUS_ORDER)):
            lines.append(f"- {status}: {counts[status]}")
        return lines

    def _milestone_progress_lines(self, plan: dict[str, Any]) -> list[str]:
        """Render completed-task progress for each milestone."""
        tasks = plan.get("tasks", [])
        lines: list[str] = []
        rendered_milestones: set[str] = set()

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            rendered_milestones.add(milestone_name)
            milestone_tasks = [
                task for task in tasks if task.get("milestone") == milestone_name
            ]
            lines.append(self._milestone_line(milestone_name, milestone_tasks))

        ungrouped_tasks = [
            task for task in tasks if (task.get("milestone") or "") not in rendered_milestones
        ]
        if ungrouped_tasks:
            lines.append(self._milestone_line("Ungrouped", ungrouped_tasks))

        return lines or ["- No milestones defined."]

    def _milestone_line(self, milestone_name: str, tasks: list[dict[str, Any]]) -> str:
        """Render one milestone progress line."""
        total = len(tasks)
        completed = sum(1 for task in tasks if task.get("status") == "completed")
        percent = round((completed / total) * 100) if total else 0
        status_counts = Counter(task.get("status") or "pending" for task in tasks)
        count_summary = self._status_count_summary(status_counts)
        return (
            f"- {milestone_name}: {completed}/{total} completed "
            f"({percent}%)"
            + (f" - {count_summary}" if count_summary else "")
        )

    def _blocked_task_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render blocked tasks and their reasons."""
        blocked_tasks = [task for task in tasks if task.get("status") == "blocked"]
        if not blocked_tasks:
            return ["- None."]

        return [
            f"- `{task['id']}` {task['title']}: {self._blocked_reason(task)}"
            for task in blocked_tasks
        ]

    def _ready_task_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render pending tasks whose dependencies are completed or skipped."""
        ready_tasks = self._ready_tasks(tasks)
        if not ready_tasks:
            return ["- None."]

        lines = []
        for task in ready_tasks:
            dependencies = task.get("depends_on") or []
            if dependencies:
                reason = "dependencies satisfied: " + ", ".join(dependencies)
            else:
                reason = "no dependencies"
            lines.append(f"- `{task['id']}` {task['title']} ({reason})")
        return lines

    def _ready_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return pending tasks whose dependencies are satisfied."""
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

    def _recent_export_metadata(self, raw_plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Read recent export metadata from common plan dictionary locations."""
        metadata = raw_plan.get("metadata") or {}
        value = (
            raw_plan.get("recent_exports")
            or raw_plan.get("exports")
            or raw_plan.get("export_metadata")
            or metadata.get("recent_exports")
            or metadata.get("exports")
            or metadata.get("export_metadata")
        )
        if not value:
            return []
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]
        return [{"value": value}]

    def _recent_export_lines(self, exports: list[dict[str, Any]]) -> list[str]:
        """Render recent export metadata."""
        lines = []
        for export in exports:
            label = (
                export.get("target_engine")
                or export.get("target")
                or export.get("id")
                or "export"
            )
            details = []
            for key in ("export_format", "output_path", "exported_at"):
                if export.get(key):
                    details.append(f"{key}: {export[key]}")
            extra_metadata = export.get("export_metadata")
            if isinstance(extra_metadata, dict):
                details.extend(f"{key}: {value}" for key, value in extra_metadata.items())

            if details:
                lines.append(f"- {label}: " + "; ".join(details))
            else:
                lines.append(f"- {label}")
        return lines

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _status_count_summary(self, status_counts: Counter) -> str:
        """Render non-zero status counts in a stable order."""
        parts = [
            f"{status}: {status_counts[status]}"
            for status in self.STATUS_ORDER
            if status_counts.get(status)
        ]
        parts.extend(
            f"{status}: {status_counts[status]}"
            for status in sorted(set(status_counts) - set(self.STATUS_ORDER))
        )
        return ", ".join(parts)

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        """Get the best available blocked reason for a task."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason") or "N/A"
