"""JSON Lines task queue exporter for autonomous agents."""

from __future__ import annotations

import json
from typing import Any

from blueprint.exporters.base import TargetExporter


class TaskQueueJsonlExporter(TargetExporter):
    """Export execution-plan tasks as one JSON object per line."""

    def get_format(self) -> str:
        """Get export format."""
        return "jsonl"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".jsonl"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution-plan tasks to JSON Lines."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        with open(output_path, "w", encoding="utf-8") as f:
            for task in sorted(execution_plan.get("tasks", []), key=lambda item: item["id"]):
                f.write(json.dumps(self._task_object(execution_plan, task), sort_keys=False))
                f.write("\n")

        return output_path

    def _task_object(self, plan: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
        """Build one machine-readable queue task."""
        dependencies = task.get("depends_on") or []
        return {
            "plan_id": plan["id"],
            "task_id": task["id"],
            "title": task["title"],
            "description": task["description"],
            "milestone": task.get("milestone"),
            "suggested_engine": task.get("suggested_engine"),
            "dependency_ids": dependencies,
            "files_or_modules": task.get("files_or_modules") or [],
            "acceptance_criteria": task.get("acceptance_criteria") or [],
            "status": task.get("status") or "pending",
            "ready": self._is_ready(plan, task),
        }

    def _is_ready(self, plan: dict[str, Any], task: dict[str, Any]) -> bool:
        """Return True when the task is not blocked and all dependencies are complete."""
        if task.get("status") == "blocked":
            return False

        tasks_by_id = {current_task["id"]: current_task for current_task in plan.get("tasks", [])}
        for dependency_id in task.get("depends_on") or []:
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None or dependency.get("status") != "completed":
                return False
        return True
