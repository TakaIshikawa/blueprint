"""Taskfile.yml exporter for execution plans."""

from __future__ import annotations

import re
import shlex
from typing import Any

import yaml

from blueprint.exporters.base import TargetExporter


class TaskfileExporter(TargetExporter):
    """Export execution-plan tasks as go-task Taskfile tasks."""

    def get_format(self) -> str:
        """Get export format."""
        return "yaml"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".yml"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution-plan tasks to Taskfile.yml format."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        payload = self.render_payload(execution_plan)
        with open(output_path, "w") as f:
            yaml.safe_dump(payload, f, sort_keys=False)

        return output_path

    def render_payload(self, execution_plan: dict[str, Any]) -> dict[str, Any]:
        """Build the Taskfile.yml payload for a validated execution plan."""
        plan_tasks = execution_plan.get("tasks", [])
        task_names_by_id = self.task_names_by_id(plan_tasks)
        task_payloads: dict[str, Any] = {
            "default": {
                "desc": "List available Blueprint task ids",
                "cmds": self._default_commands(plan_tasks, task_names_by_id),
            }
        }

        for task in plan_tasks:
            task_name = task_names_by_id[task["id"]]
            task_payloads[task_name] = self._task_payload(task, task_names_by_id)

        return {
            "version": "3",
            "tasks": task_payloads,
        }

    def task_names_by_id(self, tasks: list[dict[str, Any]]) -> dict[str, str]:
        """Return stable generated Taskfile task names keyed by execution task id."""
        names_by_id: dict[str, str] = {}
        used_names = {"default"}

        for task in tasks:
            milestone = self._slug(task.get("milestone") or "unassigned")
            task_slug = self._slug(task["id"])
            base_name = f"{milestone}:{task_slug}"
            task_name = self._unique_name(base_name, used_names)
            used_names.add(task_name)
            names_by_id[task["id"]] = task_name

        return names_by_id

    def _task_payload(
        self,
        task: dict[str, Any],
        task_names_by_id: dict[str, str],
    ) -> dict[str, Any]:
        """Build one Taskfile task entry."""
        payload: dict[str, Any] = {
            "desc": f"{task['id']}: {task['title']}",
            "cmds": self._task_commands(task),
        }
        dependencies = [
            task_names_by_id[task_id]
            for task_id in task.get("depends_on", [])
            if task_id in task_names_by_id
        ]
        if dependencies:
            payload["deps"] = dependencies
        return payload

    def _task_commands(self, task: dict[str, Any]) -> list[str]:
        """Build shell-safe placeholder commands for a task."""
        commands = [
            f"echo {shlex.quote('Task: ' + task['title'])}",
            f"echo {shlex.quote('Description: ' + task['description'])}",
        ]
        acceptance_criteria = task.get("acceptance_criteria", [])
        if acceptance_criteria:
            commands.append("echo 'Acceptance criteria:'")
            commands.extend(
                f"echo {shlex.quote('- ' + criterion)}"
                for criterion in acceptance_criteria
            )
        else:
            commands.append("echo 'Acceptance criteria: none specified'")
        return commands

    def _default_commands(
        self,
        tasks: list[dict[str, Any]],
        task_names_by_id: dict[str, str],
    ) -> list[str]:
        """Build default task commands that list available task ids."""
        if not tasks:
            return ["echo 'No Blueprint tasks available'"]

        commands = ["echo 'Available Blueprint task ids:'"]
        commands.extend(
            f"echo {shlex.quote(task['id'] + ' -> task ' + task_names_by_id[task['id']])}"
            for task in tasks
        )
        return commands

    def _slug(self, value: str) -> str:
        """Convert a task id or milestone to a Taskfile-safe name fragment."""
        slug = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip().lower()).strip("-_")
        return slug or "unassigned"

    def _unique_name(self, base_name: str, used_names: set[str]) -> str:
        """Return a unique task name, preserving the first generated name."""
        if base_name not in used_names:
            return base_name

        suffix = 2
        while f"{base_name}-{suffix}" in used_names:
            suffix += 1
        return f"{base_name}-{suffix}"
