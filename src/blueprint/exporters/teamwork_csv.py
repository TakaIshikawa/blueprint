"""Teamwork CSV task exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class TeamworkCsvExporter(TargetExporter):
    """Export execution tasks as Teamwork-compatible CSV import rows."""

    FIELDNAMES = [
        "Task Name",
        "Description",
        "Task List",
        "Priority",
        "Progress",
        "Tags",
        "Predecessors",
        "Estimated Time",
        "External ID",
    ]

    STATUS_PROGRESS = {
        "pending": "0",
        "todo": "0",
        "to_do": "0",
        "in_progress": "50",
        "active": "50",
        "blocked": "0",
        "completed": "100",
        "complete": "100",
        "done": "100",
        "skipped": "0",
        "canceled": "0",
        "cancelled": "0",
    }

    PRIORITY_MAP = {
        "urgent": "High",
        "critical": "High",
        "blocker": "High",
        "highest": "High",
        "high": "High",
        "major": "High",
        "medium": "Medium",
        "normal": "Medium",
        "moderate": "Medium",
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
        """Export one Teamwork CSV row per execution task."""
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
        """Build deterministic Teamwork task rows in plan task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one Teamwork CSV row for an execution task."""
        return {
            "Task Name": task["title"],
            "Description": self._description(plan, brief, task),
            "Task List": self._string(task.get("milestone")),
            "Priority": self._priority(task),
            "Progress": self._progress(task),
            "Tags": self._tags(plan, brief, task),
            "Predecessors": self._dependencies(task.get("depends_on")),
            "Estimated Time": self._estimated_time(task),
            "External ID": task["id"],
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task details and planning context into a Teamwork description."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _progress(self, task: dict[str, Any]) -> str:
        """Map Blueprint task status to a Teamwork progress percentage."""
        status = str(task.get("status") or "pending").strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", "_", status).strip("_")
        return self.STATUS_PROGRESS.get(normalized, "0")

    def _priority(self, task: dict[str, Any]) -> str:
        """Infer Teamwork priority from explicit metadata, risk, or complexity."""
        metadata = task.get("metadata") or {}
        candidates = [
            metadata.get("teamwork_priority"),
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
        return ""

    def _priority_value(self, value: Any) -> str:
        """Normalize one priority-like value to a Teamwork priority."""
        if value is None:
            return ""
        normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
        if not normalized:
            return ""
        if normalized.startswith(("p0", "p_0", "p1", "p_1")):
            return "High"
        if normalized.startswith(("p2", "p_2")):
            return "Medium"
        if normalized.startswith(("p3", "p_3")):
            return "Low"
        return self.PRIORITY_MAP.get(normalized, "")

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

    def _dependencies(self, values: list[str] | None) -> str:
        """Render dependencies as stable comma-separated predecessor IDs."""
        dependencies: list[str] = []
        for value in values or []:
            dependency = str(value).strip()
            if dependency and dependency not in dependencies:
                dependencies.append(dependency)
        return ", ".join(dependencies)

    def _estimated_time(self, task: dict[str, Any]) -> str:
        """Resolve an estimated time field from task metadata when available."""
        metadata = task.get("metadata") or {}
        estimate = (
            metadata.get("teamwork_estimated_time")
            or metadata.get("estimated_time")
            or metadata.get("time_estimate")
            or task.get("estimated_time")
            or task.get("time_estimate")
        )
        return self._string(estimate)

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact named description section."""
        items = values or []
        if not items:
            return ""
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _tag(self, value: Any) -> str:
        """Normalize a value into a Teamwork tag."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
