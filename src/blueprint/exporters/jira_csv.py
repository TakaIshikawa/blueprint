"""Jira CSV issue exporter for execution plans."""

from __future__ import annotations

import csv
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


class JiraCsvExporter(TargetExporter):
    """Export execution plans as Jira-import-ready CSV issue rows."""

    FIELDNAMES = [
        "Summary",
        "Description",
        "Issue Type",
        "Labels",
        "Epic Name",
        "Parent",
        "Priority",
        "External ID",
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
        """Export milestones as epics and tasks as child Jira issues."""
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
        """Build deterministic Jira rows from milestone order then task order."""
        milestone_rows: list[dict[str, str]] = []
        milestone_parent_ids: dict[str, str] = {}

        for index, milestone in enumerate(plan.get("milestones", []), start=1):
            name = self._milestone_name(milestone, index)
            external_id = self._external_id(plan["id"], "epic", self._slug(name), index)
            milestone_parent_ids[name] = external_id
            milestone_rows.append(self._milestone_row(plan, brief, milestone, index, external_id))

        task_rows = [
            self._task_row(plan, task, index, milestone_parent_ids)
            for index, task in enumerate(plan.get("tasks", []), start=1)
        ]
        return milestone_rows + task_rows

    def _milestone_row(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        milestone: dict[str, Any],
        index: int,
        external_id: str,
    ) -> dict[str, str]:
        """Build a Jira Epic row for one execution milestone."""
        name = self._milestone_name(milestone, index)
        description = self._join_sections(
            [
                f"Plan: {plan['id']}",
                f"Implementation brief: {brief['id']} - {brief['title']}",
                milestone.get("description") or "No milestone description provided.",
            ]
        )
        return {
            "Summary": name,
            "Description": description,
            "Issue Type": "Epic",
            "Labels": self._labels(plan, milestone, extra=["milestone"]),
            "Epic Name": name,
            "Parent": "",
            "Priority": self._priority(milestone),
            "External ID": external_id,
        }

    def _task_row(
        self,
        plan: dict[str, Any],
        task: dict[str, Any],
        index: int,
        milestone_parent_ids: dict[str, str],
    ) -> dict[str, str]:
        """Build a Jira child issue row for one execution task."""
        milestone = task.get("milestone") or ""
        parent = milestone_parent_ids.get(milestone, milestone)
        return {
            "Summary": task["title"],
            "Description": self._task_description(task),
            "Issue Type": self._task_issue_type(task),
            "Labels": self._labels(plan, task),
            "Epic Name": "",
            "Parent": parent,
            "Priority": self._priority(task),
            "External ID": self._external_id(plan["id"], "task", task["id"], index),
        }

    def _task_description(self, task: dict[str, Any]) -> str:
        """Render task details into a Jira text description."""
        sections = [
            task["description"],
            self._list_section("Dependencies", task.get("depends_on")),
            self._list_section("Files/Modules", task.get("files_or_modules")),
            self._list_section("Acceptance Criteria", task.get("acceptance_criteria")),
        ]
        return self._join_sections(sections)

    def _labels(
        self,
        plan: dict[str, Any],
        item: dict[str, Any],
        *,
        extra: list[str] | None = None,
    ) -> str:
        """Derive Jira labels from plan, assignment, engine, and metadata."""
        metadata = item.get("metadata") or {}
        raw_labels: list[Any] = [
            plan.get("target_engine"),
            item.get("suggested_engine"),
            item.get("owner_type"),
            *(extra or []),
        ]
        raw_labels.extend(metadata.get("labels") or [])
        raw_labels.extend(metadata.get("tags") or [])
        raw_labels.extend(metadata.get("components") or [])

        labels: list[str] = []
        for value in raw_labels:
            label = self._label(value)
            if label and label not in labels:
                labels.append(label)
        return ", ".join(labels)

    def _priority(self, item: dict[str, Any]) -> str:
        """Resolve Jira priority from explicit metadata or task complexity."""
        metadata = item.get("metadata") or {}
        explicit = item.get("priority") or metadata.get("priority") or metadata.get("jira_priority")
        if explicit:
            return str(explicit)

        complexity = str(item.get("estimated_complexity") or "").lower()
        return {
            "high": "High",
            "medium": "Medium",
            "low": "Low",
        }.get(complexity, "Medium")

    def _task_issue_type(self, task: dict[str, Any]) -> str:
        """Resolve the Jira issue type for a task row."""
        metadata = task.get("metadata") or {}
        issue_type = metadata.get("jira_issue_type") or metadata.get("issue_type")
        if isinstance(issue_type, str) and issue_type.strip():
            return issue_type.strip()
        return "Story"

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Return the display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _list_section(self, title: str, values: list[str] | None) -> str:
        """Render a named list section for Jira descriptions."""
        items = values or []
        if not items:
            return f"{title}:\n- None"
        return "\n".join([f"{title}:"] + [f"- {item}" for item in items])

    def _join_sections(self, sections: list[str]) -> str:
        """Join non-empty description sections."""
        return "\n\n".join(section for section in sections if section).strip()

    def _external_id(self, plan_id: str, row_type: str, value: str, index: int) -> str:
        """Build a stable Jira External ID value."""
        return f"{plan_id}:{row_type}:{index:03d}:{self._slug(value) or row_type}"

    def _label(self, value: Any) -> str:
        """Normalize a value into a Jira-friendly label."""
        if not isinstance(value, str) or not value.strip():
            return ""
        return re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-").lower()

    def _slug(self, value: str) -> str:
        """Normalize identifiers for stable external IDs."""
        return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-").lower()
