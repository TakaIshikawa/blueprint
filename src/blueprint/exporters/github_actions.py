"""GitHub Actions workflow exporter for execution plans."""

import re
import shlex
from typing import Any

import yaml

from blueprint.exporters.base import TargetExporter


class GitHubActionsExporter(TargetExporter):
    """Export execution-plan commands as a GitHub Actions workflow."""

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
        """Export execution-plan commands to a GitHub Actions workflow."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        payload = self.render_payload(execution_plan, implementation_brief)
        with open(output_path, "w") as f:
            yaml.safe_dump(payload, f, sort_keys=False)

        return output_path

    def render_payload(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the GitHub Actions workflow payload."""
        jobs: dict[str, Any] = {}
        job_ids_by_task_id: dict[str, str] = {}

        for task in execution_plan.get("tasks", []):
            commands = self._task_commands(task)
            if not commands:
                continue

            job_id = self._unique_job_id(task["id"], set(jobs))
            job_ids_by_task_id[task["id"]] = job_id
            jobs[job_id] = self._task_job(task, commands)

        for task in execution_plan.get("tasks", []):
            job_id = job_ids_by_task_id.get(task["id"])
            if not job_id:
                continue
            needs = [
                job_ids_by_task_id[dependency_id]
                for dependency_id in task.get("depends_on", [])
                if dependency_id in job_ids_by_task_id
            ]
            if needs:
                jobs[job_id]["needs"] = needs[0] if len(needs) == 1 else needs

        validation_job = self._validation_job(
            execution_plan,
            list(job_ids_by_task_id.values()),
        )
        if validation_job:
            jobs["validation"] = validation_job

        if not jobs:
            jobs["test"] = self._basic_test_job()

        return {
            "name": self._workflow_name(execution_plan, implementation_brief),
            "on": {
                "push": {"branches": ["main"]},
                "pull_request": {},
                "workflow_dispatch": {},
            },
            "jobs": jobs,
        }

    def _task_job(
        self,
        task: dict[str, Any],
        commands: list[str],
    ) -> dict[str, Any]:
        """Build one task command job."""
        return {
            "name": task["title"],
            "runs-on": "ubuntu-latest",
            "steps": [
                {"name": "Checkout", "uses": "actions/checkout@v4"},
                *[
                    {
                        "name": self._step_name(index, len(commands), task["title"]),
                        "run": command,
                    }
                    for index, command in enumerate(commands, start=1)
                ],
            ],
        }

    def _validation_job(
        self,
        execution_plan: dict[str, Any],
        dependency_job_ids: list[str],
    ) -> dict[str, Any] | None:
        """Build the final validation job from the plan test strategy."""
        commands = self._test_strategy_commands(execution_plan.get("test_strategy"))
        if not commands:
            return None

        job: dict[str, Any] = {
            "name": "Validation",
            "runs-on": "ubuntu-latest",
            "steps": [
                {"name": "Checkout", "uses": "actions/checkout@v4"},
                *[
                    {"name": self._validation_step_name(index, len(commands)), "run": command}
                    for index, command in enumerate(commands, start=1)
                ],
            ],
        }
        if dependency_job_ids:
            job["needs"] = dependency_job_ids
        return job

    def _basic_test_job(self) -> dict[str, Any]:
        """Build a useful fallback job when a plan has no runnable commands."""
        return {
            "name": "Test",
            "runs-on": "ubuntu-latest",
            "steps": [
                {"name": "Checkout", "uses": "actions/checkout@v4"},
                {"name": "Run tests", "run": "pytest"},
            ],
        }

    def _task_commands(self, task: dict[str, Any]) -> list[str]:
        """Extract runnable shell commands from task metadata."""
        metadata = task.get("metadata") or {}
        commands: list[str] = []

        command = metadata.get("command")
        if isinstance(command, str) and command.strip():
            commands.append(command.strip())

        raw_commands = metadata.get("commands")
        if isinstance(raw_commands, str) and raw_commands.strip():
            commands.append(raw_commands.strip())
        elif isinstance(raw_commands, list):
            commands.extend(
                item.strip()
                for item in raw_commands
                if isinstance(item, str) and item.strip()
            )

        return commands

    def _test_strategy_commands(self, test_strategy: Any) -> list[str]:
        """Derive a validation command from the plan test strategy."""
        if not isinstance(test_strategy, str) or not test_strategy.strip():
            return []

        strategy = test_strategy.strip()
        command = self._strategy_to_command(strategy)
        if command:
            return [command]
        return [f"echo {shlex.quote('Test strategy: ' + strategy)}"]

    def _strategy_to_command(self, strategy: str) -> str | None:
        """Recognize common command-shaped test strategy text."""
        normalized = strategy.strip()
        lowered = normalized.lower()
        if lowered.startswith("run "):
            candidate = normalized[4:].strip()
        else:
            candidate = normalized

        command_like_prefixes = (
            "pytest",
            "poetry ",
            "python ",
            "uv ",
            "npm ",
            "pnpm ",
            "yarn ",
            "make ",
            "tox",
            "ruff ",
        )
        if lowered == "run tests":
            return "pytest"
        if candidate.lower().startswith(command_like_prefixes):
            return candidate
        return None

    def _workflow_name(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any] | None,
    ) -> str:
        """Build a stable workflow display name."""
        if implementation_brief and implementation_brief.get("title"):
            return f"Blueprint CI: {implementation_brief['title']}"
        return f"Blueprint CI: {execution_plan['id']}"

    def _unique_job_id(self, task_id: str, existing_job_ids: set[str]) -> str:
        """Convert a task id to a unique GitHub Actions job id."""
        base = re.sub(r"[^A-Za-z0-9_]+", "_", task_id).strip("_").lower()
        if not base:
            base = "task"
        if base[0].isdigit():
            base = f"task_{base}"

        job_id = base
        suffix = 2
        while job_id in existing_job_ids or job_id == "validation":
            job_id = f"{base}_{suffix}"
            suffix += 1
        return job_id

    def _step_name(self, index: int, command_count: int, task_title: str) -> str:
        """Build a concise step name."""
        if command_count == 1:
            return task_title
        return f"{task_title} ({index})"

    def _validation_step_name(self, index: int, command_count: int) -> str:
        """Build a validation step name."""
        if command_count == 1:
            return "Run validation"
        return f"Run validation ({index})"
