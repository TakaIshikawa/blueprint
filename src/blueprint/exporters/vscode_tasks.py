"""VS Code tasks.json exporter for execution plans."""

import json
import shlex
from typing import Any

from blueprint.exporters.base import TargetExporter


class VSCodeTasksExporter(TargetExporter):
    """Export execution-plan tasks as VS Code shell tasks."""

    def get_format(self) -> str:
        """Get export format."""
        return "json"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".json"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution-plan tasks to VS Code tasks.json format."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        payload = self.render_payload(execution_plan)
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)
            f.write("\n")

        return output_path

    def render_payload(self, execution_plan: dict[str, Any]) -> dict[str, Any]:
        """Build the VS Code tasks.json payload for a validated execution plan."""
        labels_by_id = {
            task["id"]: self._task_label(task)
            for task in execution_plan.get("tasks", [])
        }
        return {
            "version": "2.0.0",
            "tasks": [
                self._task_payload(task, labels_by_id)
                for task in execution_plan.get("tasks", [])
            ],
        }

    def _task_payload(
        self,
        task: dict[str, Any],
        labels_by_id: dict[str, str],
    ) -> dict[str, Any]:
        """Build one VS Code shell task entry."""
        payload: dict[str, Any] = {
            "label": self._task_label(task),
            "type": "shell",
            "command": self._task_command(task),
            "problemMatcher": [],
        }
        dependencies = [
            labels_by_id[task_id]
            for task_id in task.get("depends_on", [])
            if task_id in labels_by_id
        ]
        if dependencies:
            payload["dependsOn"] = dependencies
        return payload

    def _task_label(self, task: dict[str, Any]) -> str:
        """Get the VS Code task label."""
        return f"{task['id']}: {task['title']}"

    def _task_command(self, task: dict[str, Any]) -> str:
        """Get the task command or a shell-safe placeholder."""
        metadata = task.get("metadata") or {}
        command = metadata.get("command")
        if isinstance(command, str) and command.strip():
            return command
        return f"echo {shlex.quote(task['title'])}"
