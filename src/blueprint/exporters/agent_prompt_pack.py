"""Agent prompt pack exporter for per-task autonomous coding handoffs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from blueprint.exporters.base import TargetExporter


SCHEMA_VERSION = "blueprint.agent_prompt_pack.v1"


class AgentPromptPackExporter(TargetExporter):
    """Export one Markdown implementation prompt per execution task."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ""

    def get_export_metadata(self) -> dict[str, Any]:
        """Return extra metadata for export records."""
        return {
            "artifact_type": "directory",
            "manifest_format": "json",
            "prompt_format": "markdown",
        }

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export tasks to a directory of agent-ready Markdown prompts."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        task_entries: dict[str, dict[str, Any]] = {}
        for task in execution_plan.get("tasks", []):
            filename = self._prompt_filename(task["id"])
            prompt_path = output_dir / filename
            prompt_path.write_text(
                self._prompt_content(execution_plan, implementation_brief, task)
            )
            task_entries[task["id"]] = {
                "title": task["title"],
                "prompt_path": filename,
                "dependencies": task.get("depends_on") or [],
            }

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "plan_id": execution_plan["id"],
            "implementation_brief_id": implementation_brief["id"],
            "project_title": implementation_brief["title"],
            "target_repo": execution_plan.get("target_repo"),
            "prompt_format": "markdown",
            "manifest_format": "json",
            "tasks": task_entries,
        }
        (output_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )

        return str(output_dir)

    def _prompt_content(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Build an autonomous coding agent prompt for a single task."""
        lines = [
            f"# Agent Task: {task['title']}",
            "",
            "## Operating Instructions",
            "- Work on an isolated branch for this task before making changes.",
            "- Keep the implementation scoped to this task and its acceptance criteria.",
            "- Do not remove or rewrite unrelated work.",
            "",
            "## Project Context",
            f"- Brief ID: `{brief['id']}`",
            f"- Plan ID: `{plan['id']}`",
            f"- Project: {brief['title']}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Product Surface: {brief.get('product_surface') or 'N/A'}",
            f"- Target User: {brief.get('target_user') or 'N/A'}",
            "",
            "### Problem",
            brief["problem_statement"],
            "",
            "### MVP Goal",
            brief["mvp_goal"],
        ]

        if brief.get("workflow_context"):
            lines.extend(["", "### Workflow Context", brief["workflow_context"]])
        if brief.get("architecture_notes"):
            lines.extend(["", "### Architecture Notes", brief["architecture_notes"]])
        if brief.get("data_requirements"):
            lines.extend(["", "### Data Requirements", brief["data_requirements"]])
        if brief.get("integration_points"):
            lines.extend(["", "### Integration Points"])
            lines.extend(self._bullet_lines(brief.get("integration_points")))

        lines.extend(
            [
                "",
                "## Task",
                f"- Task ID: `{task['id']}`",
                f"- Title: {task['title']}",
                f"- Milestone: {task.get('milestone') or 'N/A'}",
                f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
                f"- Dependencies: {self._inline_list(task.get('depends_on'))}",
                f"- Expected Files/Modules: {self._inline_list(task.get('files_or_modules'))}",
                "",
                "### Description",
                task["description"],
                "",
                "## Acceptance Criteria",
            ]
        )
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))

        lines.extend(["", "## Plan Validation"])
        suggested_test_command = self._suggested_test_command(plan)
        if suggested_test_command:
            lines.append(f"- Suggested Test Command: `{suggested_test_command}`")
        lines.append(f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}")
        lines.append(f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}")
        lines.append("- Definition of Done:")
        lines.extend(self._bullet_lines(brief.get("definition_of_done"), indent="  "))

        if plan.get("handoff_prompt"):
            lines.extend(["", "## Additional Handoff Context", plan["handoff_prompt"]])

        if brief.get("risks"):
            lines.extend(["", "## Risks"])
            lines.extend(self._bullet_lines(brief.get("risks")))

        if brief.get("assumptions"):
            lines.extend(["", "## Assumptions"])
            lines.extend(self._bullet_lines(brief.get("assumptions")))

        return "\n".join(lines) + "\n"

    def _prompt_filename(self, task_id: str) -> str:
        """Build a stable filesystem-safe prompt filename from a task ID."""
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", task_id).strip("-")
        return f"{slug or 'task'}.md"

    def _suggested_test_command(self, plan: dict[str, Any]) -> str | None:
        """Return a suggested test command from supported plan metadata keys."""
        metadata = plan.get("metadata") or {}
        for key in ("suggested_test_command", "test_command", "validation_command"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _inline_list(self, value: list[str] | None) -> str:
        """Render a short list field inline."""
        return ", ".join(value or []) or "None"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render list values as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]
