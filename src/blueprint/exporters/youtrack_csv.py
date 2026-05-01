"""YouTrack CSV issue exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class YouTrackCsvExporter(TargetExporter):
    """Export execution tasks as YouTrack-compatible CSV import rows."""

    FIELDNAMES = [
        "Summary",
        "Description",
        "Project",
        "Issue Id",
        "Type",
        "Priority",
        "State",
        "Assignee",
        "Tags",
        "Subsystem",
        "Estimation",
        "Depends On",
        "External ID",
    ]

    STATUS_MAP = {
        "pending": "Submitted",
        "todo": "Submitted",
        "to_do": "Submitted",
        "new": "Submitted",
        "open": "Open",
        "active": "In Progress",
        "in_progress": "In Progress",
        "blocked": "Open",
        "complete": "Fixed",
        "completed": "Fixed",
        "done": "Fixed",
        "fixed": "Fixed",
        "skipped": "Won't fix",
        "canceled": "Won't fix",
        "cancelled": "Won't fix",
    }

    PRIORITY_MAP = {
        "urgent": "Critical",
        "critical": "Critical",
        "blocker": "Critical",
        "highest": "Critical",
        "high": "Major",
        "major": "Major",
        "medium": "Normal",
        "normal": "Normal",
        "moderate": "Normal",
        "low": "Minor",
        "minor": "Minor",
        "lowest": "Minor",
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
        """Export one YouTrack CSV issue row per execution task."""
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
        """Build deterministic YouTrack issue rows in plan task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one YouTrack CSV row for an execution task."""
        metadata = task.get("metadata") or {}
        return {
            "Summary": task["title"],
            "Description": self._description(plan, brief, task),
            "Project": self._project(plan, task),
            "Issue Id": self._string(
                metadata.get("youtrack_issue_id") or metadata.get("issue_id")
            ),
            "Type": self._issue_type(task),
            "Priority": self._priority(task),
            "State": self._state(task),
            "Assignee": self._assignee(task),
            "Tags": self._tags(plan, brief, task),
            "Subsystem": self._subsystem(task),
            "Estimation": self._estimation(task),
            "Depends On": self._dependencies(task.get("depends_on")),
            "External ID": task["id"],
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into a YouTrack description."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Dependencies", task.get("depends_on")),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _project(self, plan: dict[str, Any], task: dict[str, Any]) -> str:
        """Resolve the YouTrack project field from metadata or plan context."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("youtrack_project") or metadata.get("project")
        if explicit:
            return str(explicit)
        return self._string(plan.get("target_repo") or plan.get("project_type"))

    def _issue_type(self, task: dict[str, Any]) -> str:
        """Resolve the YouTrack issue type for a task row."""
        metadata = task.get("metadata") or {}
        explicit = (
            metadata.get("youtrack_type") or metadata.get("issue_type") or metadata.get("type")
        )
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()
        return "Task"

    def _priority(self, task: dict[str, Any]) -> str:
        """Infer YouTrack priority from explicit metadata, risk, or complexity."""
        metadata = task.get("metadata") or {}
        candidates = [
            metadata.get("youtrack_priority"),
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
        """Normalize one priority-like value to a YouTrack priority."""
        if value is None:
            return ""
        normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
        if not normalized:
            return ""
        if normalized.startswith(("p0", "p_0", "p1", "p_1")):
            return "Critical"
        if normalized.startswith(("p2", "p_2")):
            return "Normal"
        if normalized.startswith(("p3", "p_3")):
            return "Minor"
        return self.PRIORITY_MAP.get(normalized, "")

    def _state(self, task: dict[str, Any]) -> str:
        """Map Blueprint task status to a YouTrack state."""
        metadata = task.get("metadata") or {}
        explicit = metadata.get("youtrack_state") or metadata.get("state")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()

        status = str(task.get("status") or "pending").strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", "_", status).strip("_")
        return self.STATUS_MAP.get(normalized, "Submitted")

    def _assignee(self, task: dict[str, Any]) -> str:
        """Resolve the YouTrack assignee field from task metadata."""
        metadata = task.get("metadata") or {}
        assignee = (
            metadata.get("youtrack_assignee")
            or metadata.get("assignee")
            or metadata.get("owner")
            or task.get("assignee")
        )
        return self._string(assignee)

    def _tags(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Derive comma-delimited YouTrack tags from plan, task, and metadata."""
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

    def _subsystem(self, task: dict[str, Any]) -> str:
        """Resolve the YouTrack subsystem field from task metadata or milestone."""
        metadata = task.get("metadata") or {}
        subsystem = (
            metadata.get("youtrack_subsystem")
            or metadata.get("subsystem")
            or metadata.get("component")
            or task.get("subsystem")
            or task.get("milestone")
        )
        return self._string(subsystem)

    def _estimation(self, task: dict[str, Any]) -> str:
        """Resolve the YouTrack estimation field from task metadata."""
        metadata = task.get("metadata") or {}
        estimation = (
            metadata.get("youtrack_estimation")
            or metadata.get("estimation")
            or metadata.get("estimate")
            or metadata.get("time_estimate")
            or task.get("estimation")
            or task.get("estimate")
            or task.get("time_estimate")
        )
        return self._string(estimation)

    def _dependencies(self, values: list[str] | None) -> str:
        """Render dependencies as stable comma-separated issue IDs."""
        dependencies: list[str] = []
        for value in values or []:
            dependency = str(value).strip()
            if dependency and dependency not in dependencies:
                dependencies.append(dependency)
        return ", ".join(dependencies)

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named description section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _tag(self, value: Any) -> str:
        """Normalize a value into a YouTrack tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
