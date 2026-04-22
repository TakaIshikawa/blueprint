"""Mermaid exporter - flowchart task graph format."""

import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class MermaidExporter(TargetExporter):
    """Export execution plans to Mermaid flowchart graphs."""

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
        """Export to Mermaid flowchart format."""
        self.ensure_output_dir(output_path)
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        content = self._build_flowchart(execution_plan)

        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def _build_flowchart(self, plan: dict[str, Any]) -> str:
        """Build a Mermaid flowchart for milestones, tasks, and dependencies."""
        task_ids = {task["id"] for task in plan.get("tasks", [])}
        node_ids = {task_id: self._node_id(task_id) for task_id in task_ids}
        lines = ["flowchart TD"]

        rendered_tasks: set[str] = set()
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            milestone_id = self._node_id(f"milestone-{index}-{milestone_name}")
            lines.append(f'  subgraph {milestone_id}["{self._escape_label(milestone_name)}"]')

            milestone_tasks = [
                task for task in plan.get("tasks", []) if task.get("milestone") == milestone_name
            ]
            if not milestone_tasks:
                lines.append(f'    {milestone_id}_empty["No tasks"]')

            for task in milestone_tasks:
                lines.append(f"    {self._task_node(task, node_ids[task['id']])}")
                rendered_tasks.add(task["id"])

            lines.append("  end")

        ungrouped_tasks = [
            task for task in plan.get("tasks", []) if task["id"] not in rendered_tasks
        ]
        if ungrouped_tasks:
            lines.append('  subgraph ungrouped["Ungrouped Tasks"]')
            for task in ungrouped_tasks:
                lines.append(f"    {self._task_node(task, node_ids[task['id']])}")
            lines.append("  end")

        external_dependencies = self._external_dependencies(plan.get("tasks", []), task_ids)
        for dependency_id in external_dependencies:
            dependency_node_id = self._node_id(dependency_id)
            node_ids[dependency_id] = dependency_node_id
            dependency_label = f"{self._escape_label(dependency_id)}<br/>Status: external"
            lines.append(
                f'  {dependency_node_id}["{dependency_label}"]'
            )
            lines.append(f"  class {dependency_node_id} status_external;")

        for task in plan.get("tasks", []):
            task_node_id = node_ids[task["id"]]
            for dependency_id in task.get("depends_on", []) or []:
                lines.append(f"  {node_ids[dependency_id]} --> {task_node_id}")

        lines.extend(
            [
                "  classDef status_pending fill:#fff7cc,stroke:#9a6b00,color:#1f1f1f;",
                "  classDef status_in_progress fill:#d9ecff,stroke:#1d5f9f,color:#1f1f1f;",
                "  classDef status_completed fill:#dff3df,stroke:#2f7d32,color:#1f1f1f;",
                "  classDef status_blocked fill:#ffe0df,stroke:#b3261e,color:#1f1f1f;",
                "  classDef status_skipped fill:#eeeeee,stroke:#777777,color:#1f1f1f;",
                "  classDef status_external fill:#f5f5f5,stroke:#999999,"
                "stroke-dasharray: 5 5,color:#1f1f1f;",
            ]
        )

        return "\n".join(lines) + "\n"

    def _task_node(self, task: dict[str, Any], node_id: str) -> str:
        """Render a task node with the original task id and status in the label."""
        status = task.get("status", "pending")
        label = "<br/>".join(
            [
                self._escape_label(task["id"]),
                self._escape_label(task.get("title", "Untitled task")),
                f"Status: {self._escape_label(status)}",
            ]
        )
        return f'{node_id}["{label}"]:::status_{self._class_name(status)}'

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _external_dependencies(
        self,
        tasks: list[dict[str, Any]],
        task_ids: set[str],
    ) -> list[str]:
        """Find dependencies that are referenced but not present as tasks."""
        dependencies: set[str] = set()
        for task in tasks:
            dependencies.update(task.get("depends_on", []) or [])
        return sorted(dependencies - task_ids)

    def _node_id(self, raw_id: str) -> str:
        """Convert source ids into Mermaid-safe node identifiers."""
        node_id = re.sub(r"[^0-9A-Za-z_]", "_", raw_id)
        if not node_id or node_id[0].isdigit():
            node_id = f"node_{node_id}"
        return node_id

    def _class_name(self, status: str) -> str:
        """Convert task status into a Mermaid class suffix."""
        return re.sub(r"[^0-9A-Za-z_]", "_", status or "pending")

    def _escape_label(self, value: Any) -> str:
        """Escape values for Mermaid labels."""
        text = str(value).replace("\n", " ")
        return (
            text.replace("&", "&amp;")
            .replace('"', "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
