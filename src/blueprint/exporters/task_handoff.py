"""Focused Markdown and JSON exporter for a single execution task handoff."""

from __future__ import annotations

from typing import Any


class TaskHandoffExporter:
    """Render a single execution task with the context needed for handoff."""

    def render_markdown(
        self,
        task: dict[str, Any],
        dependency_tasks: list[dict[str, Any]],
        plan: dict[str, Any] | None,
        brief: dict[str, Any] | None,
    ) -> str:
        """Render a focused Markdown handoff for one execution task."""
        lines = [
            f"# {task['title']}",
            "",
            "## Task",
            f"- Task ID: `{task['id']}`",
            f"- Status: {task.get('status') or 'pending'}",
            f"- Plan ID: `{task.get('execution_plan_id') or 'N/A'}`",
            f"- Milestone: {task.get('milestone') or 'N/A'}",
            f"- Owner: {task.get('owner_type') or 'N/A'}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            f"- Estimated Complexity: {task.get('estimated_complexity') or 'N/A'}",
            "",
            "## Description",
            task["description"],
            "",
            "## Dependencies",
        ]

        lines.extend(self._dependency_lines(task.get("depends_on"), dependency_tasks))
        lines.extend(["", "## Files or Modules"])
        lines.extend(self._bullet_lines(task.get("files_or_modules")))
        lines.extend(["", "## Acceptance Criteria"])
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))
        lines.extend(
            [
                "",
                "## Implementation Brief Context",
                f"- Brief ID: `{self._brief_value(brief, 'id')}`",
                f"- Title: {self._brief_value(brief, 'title')}",
                f"- Status: {self._brief_value(brief, 'status')}",
                f"- Domain: {self._brief_value(brief, 'domain')}",
                f"- Target User: {self._brief_value(brief, 'target_user')}",
                f"- Workflow Context: {self._brief_value(brief, 'workflow_context')}",
                f"- Problem Statement: {self._brief_value(brief, 'problem_statement')}",
                f"- MVP Goal: {self._brief_value(brief, 'mvp_goal')}",
                f"- Architecture Notes: {self._brief_value(brief, 'architecture_notes')}",
                f"- Validation Plan: {self._brief_value(brief, 'validation_plan')}",
                "",
                "## Plan Test Strategy",
                self._plan_value(plan, "test_strategy"),
            ]
        )

        handoff_prompt = self._plan_value(plan, "handoff_prompt")
        if handoff_prompt != "N/A":
            lines.extend(["", "## Handoff Prompt", handoff_prompt])

        return "\n".join(lines) + "\n"

    def render_json(
        self,
        task: dict[str, Any],
        dependency_tasks: list[dict[str, Any]],
        plan: dict[str, Any] | None,
        brief: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Render a JSON-serializable handoff payload."""
        return {
            "task": task,
            "dependency_tasks": dependency_tasks,
            "plan": self._plan_summary(plan),
            "brief_summary": self._brief_summary(brief),
        }

    def _dependency_lines(
        self,
        dependency_ids: list[str] | None,
        dependency_tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render dependency IDs with the best available task status."""
        dependency_ids = dependency_ids or []
        if not dependency_ids:
            return ["No dependencies."]

        dependencies_by_id = {task["id"]: task for task in dependency_tasks}
        lines = ["| Task ID | Title | Status |", "| --- | --- | --- |"]
        for dependency_id in dependency_ids:
            dependency = dependencies_by_id.get(dependency_id)
            lines.append(
                "| "
                f"`{dependency_id}` | "
                f"{dependency.get('title') if dependency else 'N/A'} | "
                f"{dependency.get('status') if dependency else 'missing'} |"
            )
        return lines

    def _bullet_lines(self, value: list[str] | None) -> list[str]:
        """Render list fields as Markdown bullets."""
        items = value or []
        if not items:
            return ["- None"]
        return [f"- {item}" for item in items]

    def _brief_value(self, brief: dict[str, Any] | None, key: str) -> Any:
        """Return a brief field for display."""
        if not brief:
            return "N/A"
        return brief.get(key) or "N/A"

    def _plan_value(self, plan: dict[str, Any] | None, key: str) -> Any:
        """Return a plan field for display."""
        if not plan:
            return "N/A"
        return plan.get(key) or "N/A"

    def _plan_summary(self, plan: dict[str, Any] | None) -> dict[str, Any] | None:
        """Return focused plan fields for JSON output."""
        if not plan:
            return None
        return {
            "id": plan["id"],
            "implementation_brief_id": plan["implementation_brief_id"],
            "status": plan.get("status"),
            "target_engine": plan.get("target_engine"),
            "target_repo": plan.get("target_repo"),
            "project_type": plan.get("project_type"),
            "test_strategy": plan.get("test_strategy"),
            "handoff_prompt": plan.get("handoff_prompt"),
        }

    def _brief_summary(self, brief: dict[str, Any] | None) -> dict[str, Any] | None:
        """Return implementation brief summary fields for JSON output."""
        if not brief:
            return None
        return {
            "id": brief["id"],
            "title": brief["title"],
            "status": brief.get("status"),
            "domain": brief.get("domain"),
            "target_user": brief.get("target_user"),
            "workflow_context": brief.get("workflow_context"),
            "problem_statement": brief.get("problem_statement"),
            "mvp_goal": brief.get("mvp_goal"),
            "architecture_notes": brief.get("architecture_notes"),
            "validation_plan": brief.get("validation_plan"),
            "definition_of_done": brief.get("definition_of_done"),
        }
