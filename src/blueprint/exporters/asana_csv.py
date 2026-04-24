"""Asana CSV task exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class AsanaCsvExporter(TargetExporter):
    """Export execution tasks as Asana CSV import rows."""

    FIELDNAMES = [
        "Name",
        "Notes",
        "Section/Column",
        "Assignee",
        "Due Date",
        "Tags",
        "Dependencies",
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
        """Export one Asana CSV row per execution task."""
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
        """Build deterministic Asana task rows in plan task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one Asana CSV row for an execution task."""
        return {
            "Name": task["title"],
            "Notes": self._notes(plan, brief, task),
            "Section/Column": self._string(task.get("milestone")),
            "Assignee": self._assignee(task),
            "Due Date": self._due_date(task),
            "Tags": self._tags(plan, brief, task),
            "Dependencies": self._dependency_text(task.get("depends_on")),
        }

    def _notes(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into Asana notes."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _assignee(self, task: dict[str, Any]) -> str:
        """Resolve an Asana assignee from explicit metadata or usable owner_type."""
        metadata = task.get("metadata") or {}
        explicit = (
            metadata.get("asana_assignee")
            or metadata.get("assignee")
            or metadata.get("owner")
            or task.get("assignee")
        )
        if explicit:
            return str(explicit)

        owner_type = task.get("owner_type")
        if isinstance(owner_type, str) and self._usable_assignee(owner_type):
            return owner_type.strip()
        return ""

    def _due_date(self, task: dict[str, Any]) -> str:
        """Resolve an Asana due date from task metadata."""
        metadata = task.get("metadata") or {}
        due_date = (
            metadata.get("asana_due_date")
            or metadata.get("due_date")
            or metadata.get("due_on")
            or task.get("due_date")
            or task.get("due_on")
        )
        return self._string(due_date)

    def _tags(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Derive Asana comma-delimited tags from plan, task, and metadata."""
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
        return ", ".join(tags)

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named notes section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _dependency_text(self, values: list[str] | None) -> str:
        """Render dependencies as a stable comma-separated field."""
        dependencies: list[str] = []
        for value in values or []:
            dependency = str(value).strip()
            if dependency and dependency not in dependencies:
                dependencies.append(dependency)
        return ", ".join(dependencies)

    def _usable_assignee(self, value: str) -> bool:
        """Return True when owner_type looks specific enough for Asana import."""
        normalized = value.strip().lower()
        return bool(normalized) and normalized not in {"agent", "ai", "human", "owner"}

    def _tag(self, value: Any) -> str:
        """Normalize a value into an Asana tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
