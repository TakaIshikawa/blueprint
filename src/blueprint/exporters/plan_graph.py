"""Plan graph exporters for DOT and JSON dependency views."""

import json
import re
from typing import Any


class UnknownDependencyError(ValueError):
    """Raised when a task depends on IDs that are not present in the plan."""

    def __init__(self, unknown_dependencies: dict[str, list[str]]):
        self.unknown_dependencies = unknown_dependencies
        details = "; ".join(
            f"{task_id}: {', '.join(dependency_ids)}"
            for task_id, dependency_ids in sorted(unknown_dependencies.items())
        )
        super().__init__(f"Unknown dependency IDs found: {details}")


class PlanGraphExporter:
    """Render execution plan task dependencies as DOT or JSON."""

    SUPPORTED_FORMATS = {"dot", "json"}

    def render(self, plan: dict[str, Any], output_format: str) -> str:
        """Render the plan graph in the requested format."""
        if output_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported graph format: {output_format}")

        self._validate_dependencies(plan)
        if output_format == "dot":
            return self._render_dot(plan)
        return self._render_json(plan)

    def _render_json(self, plan: dict[str, Any]) -> str:
        """Build a structured task dependency graph."""
        milestone_lookup = self._milestone_lookup(plan.get("milestones", []))
        payload = {
            "plan_id": plan["id"],
            "implementation_brief_id": plan["implementation_brief_id"],
            "milestones": [
                {
                    "id": self._milestone_id(milestone, index),
                    "name": self._milestone_name(milestone, index),
                    "description": milestone.get("description"),
                }
                for index, milestone in enumerate(plan.get("milestones", []), 1)
            ],
            "nodes": [
                {
                    "id": task["id"],
                    "title": task.get("title", "Untitled task"),
                    "description": task.get("description"),
                    "status": task.get("status", "pending"),
                    "milestone": task.get("milestone"),
                    "milestone_id": milestone_lookup.get(task.get("milestone")),
                    "depends_on": task.get("depends_on", []) or [],
                    "owner_type": task.get("owner_type"),
                    "suggested_engine": task.get("suggested_engine"),
                    "files_or_modules": task.get("files_or_modules") or [],
                    "acceptance_criteria": task.get("acceptance_criteria") or [],
                    "estimated_complexity": task.get("estimated_complexity"),
                }
                for task in plan.get("tasks", [])
            ],
            "edges": [
                {
                    "from": dependency_id,
                    "to": task["id"],
                }
                for task in plan.get("tasks", [])
                for dependency_id in task.get("depends_on", []) or []
            ],
        }
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    def _render_dot(self, plan: dict[str, Any]) -> str:
        """Build a DOT graph with milestone clusters."""
        lines = [
            f"digraph {self._dot_id(plan['id'])} {{",
            "  rankdir=LR;",
            '  graph [fontname="Helvetica"];',
            '  node [shape=box, style="rounded,filled", fontname="Helvetica"];',
            '  edge [fontname="Helvetica"];',
        ]

        rendered_task_ids: set[str] = set()
        tasks = plan.get("tasks", [])

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            cluster_id = f"cluster_{self._safe_id(self._milestone_id(milestone, index))}"
            lines.append(f"  subgraph {cluster_id} {{")
            lines.append(f"    label={self._dot_string(milestone_name)};")
            lines.append('    color="#999999";')

            milestone_aliases = self._milestone_aliases(milestone, index)
            milestone_tasks = [
                task
                for task in tasks
                if task["id"] not in rendered_task_ids
                and task.get("milestone") in milestone_aliases
            ]
            if not milestone_tasks:
                lines.append(
                    f"    {self._dot_id(f'{cluster_id}_empty')} "
                    '[label="No tasks", style="dashed"];'
                )

            for task in milestone_tasks:
                lines.append(f"    {self._dot_task_node(task)}")
                rendered_task_ids.add(task["id"])

            lines.append("  }")

        ungrouped_tasks = [
            task for task in tasks if task["id"] not in rendered_task_ids
        ]
        if ungrouped_tasks:
            lines.append("  subgraph cluster_ungrouped {")
            lines.append('    label="Ungrouped Tasks";')
            lines.append('    color="#999999";')
            for task in ungrouped_tasks:
                lines.append(f"    {self._dot_task_node(task)}")
            lines.append("  }")

        for task in plan.get("tasks", []):
            for dependency_id in task.get("depends_on", []) or []:
                lines.append(
                    f"  {self._dot_id(dependency_id)} -> {self._dot_id(task['id'])};"
                )

        lines.append("}")
        return "\n".join(lines) + "\n"

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

    def _dot_task_node(self, task: dict[str, Any]) -> str:
        """Render a DOT node for one task."""
        status = task.get("status", "pending")
        label = "\n".join(
            [
                task["id"],
                task.get("title", "Untitled task"),
                f"Status: {status}",
            ]
        )
        return (
            f"{self._dot_id(task['id'])} [label={self._dot_string(label)}, "
            f'fillcolor="{self._status_color(status)}"];'
        )

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _milestone_id(self, milestone: dict[str, Any], index: int) -> str:
        """Get a stable milestone ID for graph payloads and DOT clusters."""
        return milestone.get("id") or self._milestone_name(milestone, index)

    def _milestone_aliases(self, milestone: dict[str, Any], index: int) -> set[str]:
        """Get possible task milestone references for a milestone."""
        aliases = {
            milestone.get("id"),
            milestone.get("name"),
            milestone.get("title"),
            self._milestone_name(milestone, index),
        }
        return {alias for alias in aliases if alias}

    def _milestone_lookup(self, milestones: list[dict[str, Any]]) -> dict[str, str]:
        """Map milestone aliases to stable milestone IDs."""
        lookup = {}
        for index, milestone in enumerate(milestones, 1):
            milestone_id = self._milestone_id(milestone, index)
            for alias in self._milestone_aliases(milestone, index):
                lookup[alias] = milestone_id
        return lookup

    def _dot_id(self, raw_id: str) -> str:
        """Quote a DOT identifier."""
        return self._dot_string(raw_id)

    def _dot_string(self, value: Any) -> str:
        """Escape a value as a DOT string."""
        text = str(value)
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'

    def _safe_id(self, raw_id: str) -> str:
        """Convert source IDs into DOT-safe cluster identifiers."""
        node_id = re.sub(r"[^0-9A-Za-z_]", "_", raw_id)
        if not node_id or node_id[0].isdigit():
            node_id = f"node_{node_id}"
        return node_id

    def _status_color(self, status: str) -> str:
        """Map task status to readable DOT fill colors."""
        colors = {
            "pending": "#fff7cc",
            "in_progress": "#d9ecff",
            "completed": "#dff3df",
            "blocked": "#ffe0df",
            "skipped": "#eeeeee",
        }
        return colors.get(status, "#ffffff")
