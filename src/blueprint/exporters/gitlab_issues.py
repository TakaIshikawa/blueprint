"""GitLab issue JSON exporter for execution plans."""

from __future__ import annotations

import json
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class GitLabIssuesExporter(TargetExporter):
    """Export execution tasks as GitLab issue creation JSON."""

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
        """Write one GitLab issue object per execution task."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        issues = [
            self._issue(execution_plan, implementation_brief, task)
            for task in execution_plan.get("tasks", [])
        ]
        with open(output_path, "w") as f:
            json.dump(issues, f, indent=2, sort_keys=True)
            f.write("\n")

        return output_path

    def _issue(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, Any]:
        """Build one GitLab issue object."""
        dependency_ids = task.get("depends_on") or []
        issue = {
            "title": task["title"],
            "description": self._description(plan, brief, task),
            "labels": self._labels(plan, task),
            "milestone": task.get("milestone") or "Ungrouped",
            "weight": self._weight(task),
            "metadata": {
                "plan_id": plan["id"],
                "implementation_brief_id": brief["id"],
                "task_id": task["id"],
                "target_repo": plan.get("target_repo"),
                "suggested_engine": task.get("suggested_engine"),
                "files_or_modules": task.get("files_or_modules") or [],
                "acceptance_criteria": task.get("acceptance_criteria") or [],
                "depends_on": dependency_ids,
            },
        }

        due_date = self._due_date(task)
        if due_date:
            issue["due_date"] = due_date
            issue["metadata"]["due_date"] = due_date

        return issue

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task planning context into a GitLab Markdown description."""
        lines = [
            task["description"],
            "",
            "## Planning Context",
            f"- Plan ID: `{plan['id']}`",
            f"- Task ID: `{task['id']}`",
            f"- Implementation Brief: `{brief['id']}` - {brief['title']}",
            f"- Target Repo: {plan.get('target_repo') or 'N/A'}",
            f"- Milestone: {task.get('milestone') or 'Ungrouped'}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}",
            f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}",
            "",
            "## Files/Modules",
        ]
        lines.extend(self._bullet_lines(task.get("files_or_modules"), wrap_code=True))
        lines.extend(["", "## Acceptance Criteria"])
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))
        lines.extend(["", "## Dependencies"])
        lines.extend(self._dependency_lines(task.get("depends_on")))
        return "\n".join(lines).strip() + "\n"

    def _labels(self, plan: dict[str, Any], task: dict[str, Any]) -> list[str]:
        """Derive deterministic GitLab label names."""
        metadata = task.get("metadata") or {}
        raw_labels: list[Any] = [
            plan.get("target_engine"),
            task.get("suggested_engine"),
            task.get("owner_type"),
            task.get("milestone"),
        ]
        raw_labels.extend(metadata.get("gitlab_labels") or [])
        raw_labels.extend(metadata.get("labels") or [])
        raw_labels.extend(metadata.get("tags") or [])
        raw_labels.extend(metadata.get("components") or [])

        labels: list[str] = []
        for value in raw_labels:
            label = self._label(value)
            if label and label not in labels:
                labels.append(label)
        return labels

    def _weight(self, task: dict[str, Any]) -> int | None:
        """Resolve GitLab weight from explicit metadata or task complexity."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("gitlab_weight") or metadata.get("weight")
        if isinstance(explicit, int) and explicit >= 0:
            return explicit
        if isinstance(explicit, str) and explicit.isdigit():
            return int(explicit)
        return {
            "low": 1,
            "medium": 3,
            "high": 5,
        }.get(str(task.get("estimated_complexity") or "").lower())

    def _due_date(self, task: dict[str, Any]) -> str | None:
        """Return a GitLab due date from task or metadata fields."""
        metadata = task.get("metadata") or {}
        value = (
            task.get("due_date")
            or metadata.get("gitlab_due_date")
            or metadata.get("due_date")
        )
        if not isinstance(value, str) or not value.strip():
            return None
        return value.strip()

    def _dependency_lines(self, values: list[str] | None) -> list[str]:
        """Render dependency notes for GitLab issue descriptions."""
        items = values or []
        if not items:
            return ["- None"]
        return [f"- Blocked by Blueprint task `{item}`" for item in items]

    def _bullet_lines(
        self,
        values: list[str] | None,
        *,
        wrap_code: bool = False,
    ) -> list[str]:
        """Render a list as Markdown bullets."""
        items = values or []
        if not items:
            return ["- None"]
        if wrap_code:
            return [f"- `{item}`" for item in items]
        return [f"- {item}" for item in items]

    def _label(self, value: Any) -> str:
        """Normalize a value into a GitLab label name."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())
