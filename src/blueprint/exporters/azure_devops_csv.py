"""Azure DevOps CSV work item exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class AzureDevOpsCsvExporter(TargetExporter):
    """Export execution tasks as Azure Boards CSV import rows."""

    FIELDNAMES = [
        "Work Item Type",
        "Title",
        "Description",
        "Acceptance Criteria",
        "Tags",
        "Area Path",
        "Iteration Path",
        "Parent",
        "Priority",
        "Depends On",
    ]

    def get_format(self) -> str:
        """Get export format."""
        return "csv"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".csv"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export one Azure Boards work item row per execution task."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        rows = self._rows(execution_plan, implementation_brief)
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        return output_path

    def _rows(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> list[dict[str, str]]:
        """Build deterministic Azure Boards work item rows in task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one Azure Boards CSV row for an execution task."""
        metadata = task.get("metadata") or {}
        milestone = task.get("milestone") or ""
        return {
            "Work Item Type": self._work_item_type(task),
            "Title": task["title"],
            "Description": self._description(plan, brief, task),
            "Acceptance Criteria": self._list_text(task.get("acceptance_criteria")),
            "Tags": self._tags(plan, brief, task),
            "Area Path": self._area_path(plan, task),
            "Iteration Path": self._iteration_path(plan, task),
            "Parent": self._string(metadata.get("azure_parent") or metadata.get("parent")),
            "Priority": self._priority(task),
            "Depends On": self._list_text(task.get("depends_on")),
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into the description field."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _tags(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Derive Azure Boards semicolon-delimited tags."""
        metadata = task.get("metadata") or {}
        raw_tags: list[Any] = [
            f"blueprint-plan:{plan['id']}",
            f"blueprint-brief:{brief['id']}",
            plan.get("target_engine"),
            task.get("suggested_engine"),
            task.get("owner_type"),
            task.get("milestone"),
        ]
        raw_tags.extend(metadata.get("labels") or [])
        raw_tags.extend(metadata.get("tags") or [])
        raw_tags.extend(metadata.get("components") or [])

        tags: list[str] = []
        for value in raw_tags:
            tag = self._tag(value)
            if tag and tag not in tags:
                tags.append(tag)
        return "; ".join(tags)

    def _area_path(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Resolve Azure Area Path from task metadata or plan context."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("azure_area_path") or metadata.get("area_path")
        if explicit:
            return str(explicit)
        return self._string(plan.get("target_repo") or plan.get("project_type"))

    def _iteration_path(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Map task milestone to Azure Iteration Path when present."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("azure_iteration_path") or metadata.get("iteration_path")
        if explicit:
            return str(explicit)
        milestone = task.get("milestone")
        return str(milestone) if milestone else ""

    def _priority(self, task: dict[str, Any]) -> str:
        """Resolve Azure priority from explicit metadata or task complexity."""
        metadata = task.get("metadata") or {}
        explicit = (
            metadata.get("azure_priority") or metadata.get("priority") or task.get("priority")
        )
        if explicit:
            return str(explicit)

        complexity = str(task.get("estimated_complexity") or "").lower()
        return {
            "high": "1",
            "medium": "2",
            "low": "3",
        }.get(complexity, "2")

    def _work_item_type(self, task: dict[str, Any]) -> str:
        """Resolve Azure work item type from task metadata."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("azure_work_item_type") or metadata.get("work_item_type")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()
        return "User Story"

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _list_text(self, values: list[str] | None) -> str:
        """Render list-like CSV fields as newline-delimited text."""
        return "\n".join(str(value) for value in values or [])

    def _tag(self, value: Any) -> str:
        """Normalize a value into an Azure Boards tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
