"""Markdown task bundle exporter for per-task execution handoffs."""

import re
from pathlib import Path
from typing import Any

from blueprint.exporters.base import TargetExporter


class TaskBundleExporter(TargetExporter):
    """Export an execution plan as README plus one Markdown file per task."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ""

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution-plan tasks to a Markdown bundle directory."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        bundle_dir = Path(output_path)
        bundle_dir.mkdir(parents=True, exist_ok=True)

        tasks = execution_plan.get("tasks", [])
        task_files = [
            (task, self._task_filename(index, task))
            for index, task in enumerate(tasks, start=1)
        ]

        (bundle_dir / "README.md").write_text(
            self._readme_content(execution_plan, implementation_brief, task_files)
        )
        for task, filename in task_files:
            (bundle_dir / filename).write_text(
                self._task_content(execution_plan, implementation_brief, task)
            )

        return str(bundle_dir)

    def _readme_content(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task_files: list[tuple[dict[str, Any], str]],
    ) -> str:
        """Build the bundle index file."""
        lines = [
            f"# Task Bundle: {plan['id']}",
            "",
            "## Plan Summary",
            f"- Plan ID: `{plan['id']}`",
            f"- Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repo: {plan.get('target_repo') or 'N/A'}",
            f"- Project Type: {plan.get('project_type') or 'N/A'}",
            f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}",
            "",
            "## Brief Summary",
            f"- Brief ID: `{brief['id']}`",
            f"- Title: {brief['title']}",
            f"- Domain: {brief.get('domain') or 'N/A'}",
            f"- Target User: {brief.get('target_user') or 'N/A'}",
            f"- MVP Goal: {brief.get('mvp_goal') or 'N/A'}",
            f"- Problem Statement: {brief.get('problem_statement') or 'N/A'}",
            "",
            "## Task Order",
        ]

        if task_files:
            lines.extend(
                f"{index}. [{task['id']} - {task['title']}]({filename})"
                for index, (task, filename) in enumerate(task_files, start=1)
            )
        else:
            lines.append("No tasks defined.")

        return "\n".join(lines) + "\n"

    def _task_content(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Build a per-task Markdown handoff file."""
        lines = [
            f"# {task['title']}",
            "",
            "## Task Metadata",
            f"- Task ID: `{task['id']}`",
            f"- Plan ID: `{plan['id']}`",
            f"- Status: {task.get('status') or 'pending'}",
            f"- Milestone: {task.get('milestone') or 'N/A'}",
            f"- Dependencies: {self._inline_list(task.get('depends_on'))}",
            f"- Files or Modules: {self._inline_list(task.get('files_or_modules'))}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            "",
            "## Description",
            task["description"],
            "",
            "## Acceptance Criteria",
        ]
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))
        lines.extend(
            [
                "",
                "## Validation Context",
                f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}",
                f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}",
                "- Definition of Done:",
            ]
        )
        lines.extend(self._bullet_lines(brief.get("definition_of_done"), indent="  "))

        return "\n".join(lines) + "\n"

    def _task_filename(self, index: int, task: dict[str, Any]) -> str:
        """Build a stable task filename from sequence and task ID."""
        return f"{index:03d}-{self._slug(task['id'])}.md"

    def _slug(self, value: str) -> str:
        """Normalize task IDs for filesystem-safe filenames."""
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
        return slug or "task"

    def _inline_list(self, value: list[str] | None) -> str:
        """Render a short list field inline."""
        return ", ".join(value or []) or "None"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render a list field as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]
