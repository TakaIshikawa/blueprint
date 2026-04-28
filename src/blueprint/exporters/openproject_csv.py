"""OpenProject CSV task exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class OpenProjectCsvExporter(TargetExporter):
    """Export execution tasks as OpenProject-oriented work package CSV rows."""

    FIELDNAMES = [
        "Subject",
        "Description",
        "Type",
        "Status",
        "Priority",
        "Assignee",
        "Start date",
        "Due date",
        "Estimated time",
        "Parent",
        "Predecessors",
        "Tags",
        "Milestone/Version",
        "Blueprint task ID",
    ]

    STATUS_MAP = {
        "pending": "New",
        "todo": "New",
        "to_do": "New",
        "in_progress": "In progress",
        "active": "In progress",
        "blocked": "On hold",
        "completed": "Closed",
        "complete": "Closed",
        "done": "Closed",
        "skipped": "Rejected",
        "canceled": "Rejected",
        "cancelled": "Rejected",
    }

    PRIORITY_MAP = {
        "urgent": "Immediate",
        "critical": "Immediate",
        "blocker": "Immediate",
        "highest": "Immediate",
        "high": "High",
        "major": "High",
        "medium": "Normal",
        "normal": "Normal",
        "moderate": "Normal",
        "low": "Low",
        "minor": "Low",
        "lowest": "Low",
    }

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
        """Export one OpenProject CSV row per execution task."""
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
        """Build deterministic OpenProject task rows in plan task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one OpenProject work package CSV row for an execution task."""
        return {
            "Subject": task["title"],
            "Description": self._description(plan, brief, task),
            "Type": self._type(task),
            "Status": self._status(task),
            "Priority": self._priority(task),
            "Assignee": self._assignee(task),
            "Start date": self._date_field(task, "start"),
            "Due date": self._date_field(task, "due"),
            "Estimated time": self._estimated_time(task),
            "Parent": self._parent(task),
            "Predecessors": self._dependencies(task.get("depends_on")),
            "Tags": self._tags(plan, brief, task),
            "Milestone/Version": self._string(task.get("milestone")),
            "Blueprint task ID": task["id"],
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into an OpenProject description."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Implementation Notes", self._implementation_notes(task)),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _type(self, task: dict[str, Any]) -> str:
        """Resolve an OpenProject work package type."""
        metadata = task.get("metadata") or {}
        return self._string(metadata.get("openproject_type") or metadata.get("type") or "Task")

    def _status(self, task: dict[str, Any]) -> str:
        """Map Blueprint task status to OpenProject-friendly status text."""
        status = str(task.get("status") or "pending").strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", "_", status).strip("_")
        return self.STATUS_MAP.get(normalized, self._title_text(status) or "New")

    def _priority(self, task: dict[str, Any]) -> str:
        """Infer OpenProject priority from explicit metadata, risk, or complexity."""
        metadata = task.get("metadata") or {}
        candidates = [
            metadata.get("openproject_priority"),
            metadata.get("priority"),
            task.get("priority"),
            metadata.get("risk"),
            metadata.get("risk_level"),
            metadata.get("risk_profile"),
            task.get("risk"),
            metadata.get("estimated_complexity"),
            metadata.get("complexity"),
            task.get("estimated_complexity"),
        ]
        for candidate in candidates:
            priority = self._priority_value(candidate)
            if priority:
                return priority
        return "Normal"

    def _priority_value(self, value: Any) -> str:
        """Normalize one priority-like value to an OpenProject priority."""
        if value is None:
            return ""
        normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
        if not normalized:
            return ""
        if normalized.startswith(("p0", "p_0")):
            return "Immediate"
        if normalized.startswith(("p1", "p_1")):
            return "High"
        if normalized.startswith(("p2", "p_2")):
            return "Normal"
        if normalized.startswith(("p3", "p_3")):
            return "Low"
        return self.PRIORITY_MAP.get(normalized, "")

    def _assignee(self, task: dict[str, Any]) -> str:
        """Resolve an OpenProject assignee from explicit metadata or usable owner_type."""
        metadata = task.get("metadata") or {}
        explicit = (
            metadata.get("openproject_assignee")
            or metadata.get("assignee")
            or metadata.get("owner")
        )
        if explicit:
            return self._string(explicit)

        owner = self._string(task.get("owner_type")).strip()
        return "" if owner.lower() in {"agent", "automation", "ai"} else owner

    def _date_field(self, task: dict[str, Any], kind: str) -> str:
        """Resolve start or due date metadata."""
        metadata = task.get("metadata") or {}
        if kind == "start":
            value = (
                metadata.get("openproject_start_date")
                or metadata.get("start_date")
                or metadata.get("starts_on")
                or task.get("start_date")
                or task.get("starts_on")
            )
        else:
            value = (
                metadata.get("openproject_due_date")
                or metadata.get("due_date")
                or metadata.get("deadline")
                or task.get("due_date")
                or task.get("deadline")
            )
        return self._string(value)

    def _estimated_time(self, task: dict[str, Any]) -> str:
        """Resolve an estimated time field from task metadata when available."""
        metadata = task.get("metadata") or {}
        estimate = (
            metadata.get("openproject_estimated_time")
            or metadata.get("estimated_time")
            or metadata.get("time_estimate")
            or task.get("estimated_time")
            or task.get("time_estimate")
        )
        return self._string(estimate)

    def _parent(self, task: dict[str, Any]) -> str:
        """Resolve a parent work package identifier when supplied."""
        metadata = task.get("metadata") or {}
        return self._string(
            metadata.get("openproject_parent")
            or metadata.get("parent")
            or metadata.get("parent_id")
            or task.get("parent")
            or task.get("parent_id")
        )

    def _dependencies(self, values: list[str] | None) -> str:
        """Render dependencies as stable comma-separated predecessor IDs."""
        dependencies: list[str] = []
        for value in values or []:
            dependency = str(value).strip()
            if dependency and dependency not in dependencies:
                dependencies.append(dependency)
        return ", ".join(dependencies)

    def _tags(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Derive comma-delimited tags from plan, task, and metadata."""
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

    def _implementation_notes(self, task: dict[str, Any]) -> list[str]:
        """Return implementation notes supplied on task metadata."""
        metadata = task.get("metadata") or {}
        raw_notes = (
            metadata.get("implementation_notes")
            or metadata.get("implementation_note")
            or metadata.get("notes")
            or task.get("implementation_notes")
            or task.get("notes")
        )
        if raw_notes is None:
            return []
        if isinstance(raw_notes, list):
            return [self._string(note) for note in raw_notes if self._string(note)]
        return [self._string(raw_notes)]

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named description section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _tag(self, value: Any) -> str:
        """Normalize a value into an OpenProject tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _title_text(self, value: str) -> str:
        """Convert an underscore-like value into title text."""
        return " ".join(part.capitalize() for part in re.split(r"[^a-z0-9]+", value) if part)

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
