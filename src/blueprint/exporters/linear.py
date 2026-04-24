"""Linear issue JSON exporter for execution plans."""

from __future__ import annotations

import json
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class LinearExporter(TargetExporter):
    """Export execution tasks as Linear-compatible issue JSON."""

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
        """Write one Linear issue object per execution task."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        payload = self._payload(execution_plan, implementation_brief)
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")

        return output_path

    def _payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the deterministic Linear import payload."""
        issues = [self._issue(plan, brief, task) for task in plan.get("tasks", [])]
        return {
            "schema_version": "blueprint.linear.v1",
            "exporter": "linear",
            "plan": {
                "id": plan["id"],
                "implementation_brief_id": plan["implementation_brief_id"],
                "target_engine": plan.get("target_engine"),
                "target_repo": plan.get("target_repo"),
                "project_type": plan.get("project_type"),
            },
            "issues": issues,
        }

    def _issue(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, Any]:
        """Build one Linear issue object."""
        external_id = self._external_id(plan["id"], task["id"])
        dependency_ids = task.get("depends_on") or []
        return {
            "externalId": external_id,
            "title": task["title"],
            "description": self._description(plan, brief, task, external_id),
            "teamKey": self._team_key(plan, task),
            "labels": self._labels(plan, task),
            "priority": self._priority(task),
            "estimate": self._estimate(task),
            "relations": self._relations(plan["id"], task["id"], dependency_ids),
            "metadata": {
                "planId": plan["id"],
                "taskId": task["id"],
                "milestone": task.get("milestone"),
                "suggestedEngine": task.get("suggested_engine"),
                "filesOrModules": task.get("files_or_modules") or [],
                "acceptanceCriteria": task.get("acceptance_criteria") or [],
                "dependsOn": dependency_ids,
            },
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
        external_id: str,
    ) -> str:
        """Render task planning context into a Linear Markdown description."""
        lines = [
            task["description"],
            "",
            "## Blueprint Metadata",
            f"- External ID: `{external_id}`",
            f"- Plan ID: `{plan['id']}`",
            f"- Task ID: `{task['id']}`",
            f"- Implementation Brief: `{brief['id']}` - {brief['title']}",
            f"- Milestone: {task.get('milestone') or 'Ungrouped'}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            "",
            "## Files/Modules",
        ]
        lines.extend(self._bullet_lines(task.get("files_or_modules")))
        lines.extend(["", "## Acceptance Criteria"])
        lines.extend(self._bullet_lines(task.get("acceptance_criteria")))
        lines.extend(["", "## Dependencies"])
        lines.extend(self._bullet_lines(task.get("depends_on"), wrap_code=True))
        lines.extend(["", "## Validation"])
        lines.append(f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}")
        lines.append(f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}")
        return "\n".join(lines).strip() + "\n"

    def _labels(self, plan: dict[str, Any], task: dict[str, Any]) -> list[str]:
        """Derive deterministic Linear label names."""
        metadata = task.get("metadata") or {}
        raw_labels: list[Any] = [
            plan.get("target_engine"),
            task.get("suggested_engine"),
            task.get("owner_type"),
            task.get("milestone"),
        ]
        raw_labels.extend(metadata.get("labels") or [])
        raw_labels.extend(metadata.get("tags") or [])
        raw_labels.extend(metadata.get("components") or [])

        labels: list[str] = []
        for value in raw_labels:
            label = self._label(value)
            if label and label not in labels:
                labels.append(label)
        return labels

    def _relations(
        self,
        plan_id: str,
        task_id: str,
        dependency_ids: list[str],
    ) -> list[dict[str, str]]:
        """Represent dependencies as Linear blocking relations."""
        return [
            {
                "type": "blocked_by",
                "externalId": self._external_id_for_task(plan_id, dependency_id),
                "taskId": dependency_id,
            }
            for dependency_id in dependency_ids
            if dependency_id != task_id
        ]

    def _team_key(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Resolve a Linear team key from metadata with a stable fallback."""
        task_metadata = task.get("metadata") or {}
        plan_metadata = plan.get("metadata") or {}
        value = (
            task_metadata.get("linear_team_key")
            or task_metadata.get("team_key")
            or plan_metadata.get("linear_team_key")
            or plan_metadata.get("team_key")
            or "BLUEPRINT"
        )
        normalized = re.sub(r"[^A-Za-z0-9]+", "", str(value).upper())
        return normalized or "BLUEPRINT"

    def _priority(self, task: dict[str, Any]) -> int:
        """Resolve Linear priority value."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("linear_priority") or metadata.get("priority")
        if isinstance(explicit, int) and 0 <= explicit <= 4:
            return explicit
        if isinstance(explicit, str):
            mapped = self._priority_value(explicit)
            if mapped is not None:
                return mapped

        return {
            "high": 2,
            "medium": 3,
            "low": 4,
        }.get(str(task.get("estimated_complexity") or "").lower(), 3)

    def _priority_value(self, value: str) -> int | None:
        """Map human priority names to Linear numeric values."""
        return {
            "urgent": 1,
            "highest": 1,
            "high": 2,
            "medium": 3,
            "normal": 3,
            "low": 4,
            "none": 0,
            "no priority": 0,
        }.get(value.strip().lower())

    def _estimate(self, task: dict[str, Any]) -> int | None:
        """Resolve Linear estimate points from explicit metadata or complexity."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("linear_estimate") or metadata.get("estimate")
        if isinstance(explicit, int) and explicit >= 0:
            return explicit
        if isinstance(explicit, str) and explicit.isdigit():
            return int(explicit)
        return {
            "low": 1,
            "medium": 3,
            "high": 5,
        }.get(str(task.get("estimated_complexity") or "").lower())

    def _external_id(self, plan_id: str, task_id: str) -> str:
        """Build a stable external ID for a Linear issue."""
        return self._external_id_for_task(plan_id, task_id)

    def _external_id_for_task(self, plan_id: str, task_id: str) -> str:
        """Build the stable task portion of an external ID."""
        return f"{plan_id}:task:{self._slug(task_id) or 'task'}"

    def _label(self, value: Any) -> str:
        """Normalize a value into a Linear label name."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _slug(self, value: str) -> str:
        """Normalize identifiers for stable external IDs."""
        return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-").lower()

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
