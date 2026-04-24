"""ClickUp CSV task exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class ClickUpCsvExporter(TargetExporter):
    """Export execution tasks as ClickUp-oriented CSV import rows."""

    FIELDNAMES = [
        "Task Name",
        "Task Description",
        "Status",
        "Priority",
        "Assignee",
        "Due Date",
        "Tags",
        "Dependencies",
        "List Name",
        "Task ID",
        "Acceptance Criteria",
        "Files",
    ]

    STATUS_MAP = {
        "pending": "To Do",
        "in_progress": "In Progress",
        "completed": "Complete",
        "blocked": "Blocked",
        "skipped": "Canceled",
    }

    PRIORITY_MAP = {
        "urgent": "Urgent",
        "critical": "Urgent",
        "blocker": "Urgent",
        "highest": "Urgent",
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
        """Export one ClickUp CSV row per execution task."""
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
        """Build deterministic ClickUp task rows in plan task order."""
        tasks_by_id = {task["id"]: task for task in plan.get("tasks", [])}
        return [self._task_row(plan, brief, task, tasks_by_id) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> dict[str, str]:
        """Build one ClickUp CSV row for an execution task."""
        return {
            "Task Name": task["title"],
            "Task Description": self._description(plan, brief, task),
            "Status": self._status(task),
            "Priority": self._priority(task),
            "Assignee": self._assignee(task),
            "Due Date": self._due_date(task),
            "Tags": self._tags(plan, brief, task),
            "Dependencies": self._dependencies(task, tasks_by_id),
            "List Name": self._string(task.get("milestone")),
            "Task ID": task["id"],
            "Acceptance Criteria": self._list_text(task.get("acceptance_criteria")),
            "Files": self._list_text(task.get("files_or_modules")),
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into a ClickUp description."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _status(self, task: dict[str, Any]) -> str:
        """Map Blueprint task status to ClickUp-friendly status text."""
        status = str(task.get("status") or "pending").strip().lower()
        return self.STATUS_MAP.get(status, self._title_text(status) or "To Do")

    def _priority(self, task: dict[str, Any]) -> str:
        """Infer ClickUp priority from explicit metadata, risk, or complexity."""
        metadata = task.get("metadata") or {}
        candidates = [
            metadata.get("clickup_priority"),
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
        """Normalize one priority-like value to a ClickUp priority."""
        if value is None:
            return ""
        normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
        if not normalized:
            return ""
        if normalized.startswith(("p0", "p_0")):
            return "Urgent"
        if normalized.startswith(("p1", "p_1")):
            return "High"
        if normalized.startswith(("p2", "p_2")):
            return "Normal"
        if normalized.startswith(("p3", "p_3")):
            return "Low"
        return self.PRIORITY_MAP.get(normalized, "")

    def _assignee(self, task: dict[str, Any]) -> str:
        """Resolve a ClickUp assignee from explicit metadata or usable owner_type."""
        metadata = task.get("metadata") or {}
        explicit = (
            metadata.get("clickup_assignee")
            or metadata.get("assignee")
            or metadata.get("owner")
            or task.get("assignee")
        )
        if explicit:
            return ", ".join(self._list(explicit))

        owner_type = task.get("owner_type")
        if isinstance(owner_type, str) and self._usable_assignee(owner_type):
            return owner_type.strip()
        return ""

    def _due_date(self, task: dict[str, Any]) -> str:
        """Resolve a ClickUp due date from task metadata."""
        metadata = task.get("metadata") or {}
        due_date = (
            metadata.get("clickup_due_date")
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
        """Derive pipe-delimited tags from plan, task, and metadata."""
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
        return " | ".join(tags)

    def _dependencies(
        self,
        task: dict[str, Any],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> str:
        """Render dependencies as readable title plus task ID lines."""
        dependencies: list[str] = []
        for dependency_id in task.get("depends_on") or []:
            dependency_id = str(dependency_id).strip()
            if not dependency_id:
                continue
            dependency_task = tasks_by_id.get(dependency_id)
            if dependency_task:
                dependency = f"{dependency_task['title']} ({dependency_id})"
            else:
                dependency = dependency_id
            if dependency not in dependencies:
                dependencies.append(dependency)
        return "\n".join(dependencies)

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named description section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _list_text(self, values: list[str] | None) -> str:
        """Render list-like CSV fields as newline-delimited text."""
        return "\n".join(str(value) for value in values or [])

    def _list(self, value: Any) -> list[str]:
        """Normalize strings and lists into a clean string list."""
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            values = value
        else:
            values = [value]
        return [str(item).strip() for item in values if str(item).strip()]

    def _tag(self, value: Any) -> str:
        """Normalize a value into a ClickUp tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _title_text(self, value: str) -> str:
        """Convert an underscore status to title text."""
        if not value:
            return ""
        return value.replace("_", " ").title()

    def _usable_assignee(self, value: str) -> bool:
        """Return True when owner_type looks specific enough for ClickUp import."""
        normalized = value.strip().lower()
        return bool(normalized) and normalized not in {"agent", "ai", "human", "owner"}

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
