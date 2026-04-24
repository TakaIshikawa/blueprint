"""Dependency matrix exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.exporters.plan_graph import UnknownDependencyError


class DependencyMatrixExporter:
    """Build a deterministic machine-readable task dependency matrix."""

    SUPPORTED_FORMATS = {"json"}

    def render(self, plan: dict[str, Any], output_format: str = "json") -> dict[str, Any]:
        """Render the whole plan dependency graph as a JSON-compatible payload."""
        if output_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported dependency matrix format: {output_format}")

        self._validate_dependencies(plan)
        tasks = sorted(plan.get("tasks", []), key=lambda task: task["id"])
        task_ids = [task["id"] for task in tasks]
        blocked_by = {
            task["id"]: sorted(task.get("depends_on", []) or [])
            for task in tasks
        }
        unblocks = {task_id: [] for task_id in task_ids}

        for task_id, dependency_ids in blocked_by.items():
            for dependency_id in dependency_ids:
                unblocks[dependency_id].append(task_id)

        unblocks = {
            task_id: sorted(dependent_ids)
            for task_id, dependent_ids in sorted(unblocks.items())
        }

        return {
            "plan_id": plan["id"],
            "implementation_brief_id": plan["implementation_brief_id"],
            "nodes": [self._node_payload(task) for task in tasks],
            "edges": [
                {
                    "from": dependency_id,
                    "to": task_id,
                }
                for task_id, dependency_ids in sorted(blocked_by.items())
                for dependency_id in dependency_ids
            ],
            "blocked_by": blocked_by,
            "unblocks": unblocks,
        }

    def _node_payload(self, task: dict[str, Any]) -> dict[str, Any]:
        """Return the task fields needed by schedulers and orchestration tools."""
        return {
            "id": task["id"],
            "title": task.get("title", "Untitled task"),
            "status": task.get("status", "pending"),
            "milestone": task.get("milestone"),
            "owner_type": task.get("owner_type"),
            "suggested_engine": task.get("suggested_engine"),
            "estimated_complexity": task.get("estimated_complexity"),
        }

    def _validate_dependencies(self, plan: dict[str, Any]) -> None:
        """Reject dependency references to tasks that are not in the plan."""
        task_ids = {task["id"] for task in plan.get("tasks", [])}
        unknown_dependencies: dict[str, list[str]] = {}
        for task in plan.get("tasks", []):
            missing = [
                dependency_id
                for dependency_id in task.get("depends_on", []) or []
                if dependency_id not in task_ids
            ]
            if missing:
                unknown_dependencies[task["id"]] = sorted(missing)

        if unknown_dependencies:
            raise UnknownDependencyError(unknown_dependencies)
