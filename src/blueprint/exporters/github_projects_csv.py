"""GitHub Projects CSV task exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class GitHubProjectsCsvExporter(TargetExporter):
    """Export execution tasks as GitHub Projects or spreadsheet-ready CSV rows."""

    FIELDNAMES = [
        "Title",
        "Body",
        "Status",
        "Milestone",
        "Labels",
        "Assignees",
        "Repository",
        "Task ID",
        "Dependencies",
        "Acceptance Criteria",
        "Files",
        "Estimate",
        "Suggested Engine",
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
        """Export one GitHub Projects CSV row per execution task."""
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
        """Build deterministic task rows in plan task order."""
        return [self._task_row(plan, brief, task) for task in plan.get("tasks", [])]

    def _task_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> dict[str, str]:
        """Build one GitHub Projects CSV row for an execution task."""
        return {
            "Title": task["title"],
            "Body": self._body(plan, brief, task),
            "Status": self._string(task.get("status") or "pending"),
            "Milestone": self._string(task.get("milestone")),
            "Labels": self._labels(task),
            "Assignees": self._assignees(task),
            "Repository": self._string(plan.get("target_repo")),
            "Task ID": task["id"],
            "Dependencies": self._list_text(task.get("depends_on")),
            "Acceptance Criteria": self._list_text(task.get("acceptance_criteria")),
            "Files": self._list_text(task.get("files_or_modules")),
            "Estimate": self._estimate(task),
            "Suggested Engine": self._string(task.get("suggested_engine")),
        }

    def _body(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
    ) -> str:
        """Render task context into a compact Markdown body field."""
        sections = [
            task["description"],
            f"Plan: {plan['id']}",
            f"Implementation brief: {brief['id']} - {brief['title']}",
            self._section("Task ID", [task["id"]]),
            self._section("Dependencies", task.get("depends_on") or ["None"]),
            self._section("Acceptance Criteria", task.get("acceptance_criteria")),
            self._section("Files/Modules", task.get("files_or_modules")),
        ]
        return "\n\n".join(section for section in sections if section).strip()

    def _labels(self, task: dict[str, Any]) -> str:
        """Derive comma-delimited labels from task metadata and planning fields."""
        metadata = task.get("metadata") or {}
        raw_labels: list[Any] = [
            self._prefixed("milestone", task.get("milestone")),
            self._prefixed("engine", task.get("suggested_engine")),
            self._prefixed(
                "complexity",
                metadata.get("complexity") or task.get("estimated_complexity"),
            ),
            self._prefixed(
                "risk",
                metadata.get("risk")
                or metadata.get("risk_level")
                or metadata.get("risk_profile"),
            ),
        ]
        raw_labels.extend(metadata.get("labels") or [])
        raw_labels.extend(metadata.get("tags") or [])
        raw_labels.extend(metadata.get("components") or [])
        raw_labels.extend(
            self._prefixed("risk", value) for value in self._list(metadata.get("risks"))
        )

        labels: list[str] = []
        for value in raw_labels:
            label = self._label(value)
            if label and label not in labels:
                labels.append(label)
        return ", ".join(labels)

    def _assignees(self, task: dict[str, Any]) -> str:
        """Resolve assignees only from explicit task metadata or task fields."""
        metadata = task.get("metadata") or {}
        raw_assignees = (
            metadata.get("assignees")
            or metadata.get("assignee")
            or task.get("assignees")
            or task.get("assignee")
        )
        return ", ".join(self._list(raw_assignees))

    def _estimate(self, task: dict[str, Any]) -> str:
        """Resolve an estimate from explicit metadata with complexity fallback."""
        metadata = task.get("metadata") or {}
        estimate = (
            metadata.get("estimate")
            or metadata.get("github_estimate")
            or metadata.get("story_points")
            or metadata.get("points")
            or task.get("estimate")
            or task.get("estimated_complexity")
        )
        return self._string(estimate)

    def _section(self, title: str, values: list[str] | None) -> str:
        """Render a compact Markdown section."""
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

    def _prefixed(self, prefix: str, value: Any) -> str:
        """Build a label with a stable metadata prefix."""
        if value is None or not str(value).strip():
            return ""
        return f"{prefix}:{value}"

    def _label(self, value: Any) -> str:
        """Normalize a value into a GitHub label string."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"\s+", " ", value.strip())

    def _string(self, value: Any) -> str:
        """Return a string only for populated values."""
        if value is None:
            return ""
        return str(value)
