"""Relay exporter - JSON task graph format."""

import json
from typing import Any

from blueprint.exporters.base import TargetExporter
from blueprint.validation_commands import flatten_validation_commands


class RelayExporter(TargetExporter):
    """Export execution plans to Relay-compatible JSON task graphs."""

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
        """
        Export to Relay JSON format.

        Relay expects:
        - objective: Overall goal
        - target_repo: Repository to work in
        - milestones: Sequential phases
        - tasks: Task graph with dependencies
        - validation_commands: Commands to verify completion
        """
        self.ensure_output_dir(output_path)
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        relay_export = self.render_payload(execution_plan, implementation_brief)

        # Write to file
        with open(output_path, 'w') as f:
            json.dump(relay_export, f, indent=2)

        return output_path

    def render_payload(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the normalized Relay payload."""
        return {
            "schema_version": "blueprint.relay.v1",
            "objective": {
                "title": implementation_brief["title"],
                "problem": implementation_brief["problem_statement"],
                "mvp_goal": implementation_brief["mvp_goal"],
                "success_criteria": implementation_brief["definition_of_done"],
            },
            "target_repo": execution_plan.get("target_repo"),
            "project_type": execution_plan.get("project_type"),
            "milestones": [
                {
                    "id": f"m{i}",
                    "name": milestone["name"],
                    "description": milestone.get("description", ""),
                }
                for i, milestone in enumerate(execution_plan["milestones"], 1)
            ],
            "tasks": [
                {
                    "id": task["id"],
                    "milestone_id": self._get_milestone_id(task["milestone"], execution_plan["milestones"]),
                    "title": task["title"],
                    "description": task["description"],
                    "owner_type": task.get("owner_type", "agent"),
                    "depends_on": task.get("depends_on", []),
                    "files": task.get("files_or_modules", []),
                    "acceptance_criteria": task.get("acceptance_criteria", []),
                    "complexity": task.get("estimated_complexity", "medium"),
                    "estimated_hours": task.get("estimated_hours"),
                    "risk_level": task.get("risk_level"),
                    "test_command": task.get("test_command"),
                    "status": task.get("status", "pending"),
                }
                for task in execution_plan["tasks"]
            ],
            "validation": {
                "test_strategy": execution_plan.get("test_strategy", ""),
                "commands": self._extract_validation_commands(
                    execution_plan,
                    implementation_brief,
                    execution_plan.get("tasks", []),
                ),
            },
            "context": {
                "scope": implementation_brief.get("scope", []),
                "non_goals": implementation_brief.get("non_goals", []),
                "assumptions": implementation_brief.get("assumptions", []),
                "risks": implementation_brief.get("risks", []),
                "architecture_notes": implementation_brief.get("architecture_notes", ""),
            },
            "handoff_prompt": execution_plan.get("handoff_prompt", ""),
        }

    def _get_milestone_id(self, milestone_name: str, milestones: list[dict]) -> str:
        """Get milestone ID from name."""
        for i, milestone in enumerate(milestones, 1):
            if milestone["name"] == milestone_name:
                return f"m{i}"
        return "m1"  # Default

    def _extract_validation_commands(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]] | None = None,
    ) -> list[str]:
        """Extract validation commands from brief."""
        commands = []

        commands.extend(
            flatten_validation_commands(
                (plan.get("metadata") or {}).get("validation_commands")
            )
        )

        # Common commands based on project type
        if not commands and brief.get("product_surface"):
            surface = brief["product_surface"].lower()
            if "python" in surface or "library" in surface:
                commands.extend(["pytest", "black --check src/", "ruff check src/"])
            elif "cli" in surface:
                commands.extend(["<cli> --help", "pytest"])

        for task in tasks or []:
            if task.get("test_command"):
                commands.append(task["test_command"])

        return list(dict.fromkeys(commands))
