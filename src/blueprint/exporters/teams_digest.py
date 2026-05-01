"""Microsoft Teams message-card JSON digest exporter for execution plans."""

from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict
from typing import Any

from blueprint.exporters.base import TargetExporter


SCHEMA_VERSION = "blueprint.teams_digest.v1"


class TeamsDigestExporter(TargetExporter):
    """Export compact execution-plan status as a Teams-compatible JSON payload."""

    STATUS_ORDER = ["pending", "in_progress", "completed", "blocked", "skipped"]
    MAX_SECTION_TASKS = 8
    MAX_TASK_TITLE_LENGTH = 96
    MAX_REASON_LENGTH = 140

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
        """Export an execution plan Teams digest to JSON."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        payload = self.render_payload(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")

        return output_path

    def render_payload(self, plan: dict[str, Any], brief: dict[str, Any]) -> dict[str, Any]:
        """Render a Teams message payload for a validated plan and brief."""
        tasks = plan.get("tasks", [])
        blocked_tasks = [task for task in tasks if task.get("status") == "blocked"]
        high_risk_tasks = self._high_risk_tasks(tasks)

        return {
            "schema_version": SCHEMA_VERSION,
            "summary": self._summary_text(plan, brief, tasks),
            "title": f"Execution Plan Digest: {self._plain_text(brief['title'])}",
            "type": "message",
            "attachments": [
                {
                    "content": {
                        "@context": "https://schema.org/extensions",
                        "@type": "MessageCard",
                        "summary": self._summary_text(plan, brief, tasks),
                        "themeColor": self._theme_color(blocked_tasks, high_risk_tasks),
                        "title": f"Execution Plan Digest: {self._plain_text(brief['title'])}",
                        "sections": self._sections(plan, brief, tasks),
                    },
                    "contentType": "application/vnd.microsoft.card.message",
                }
            ],
        }

    def _sections(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build deterministic MessageCard sections."""
        sections = [
            {
                "activityTitle": self._escape_markdown(brief["title"]),
                "facts": [
                    {"name": "Plan", "value": self._escape_markdown(plan["id"])},
                    {"name": "Brief", "value": self._escape_markdown(brief["id"])},
                    {"name": "Status", "value": self._status_count_text(tasks)},
                    {"name": "Summary", "value": self._summary_text(plan, brief, tasks)},
                ],
                "markdown": True,
            }
        ]

        blocked_tasks = [task for task in tasks if task.get("status") == "blocked"]
        sections.append(
            {
                "activityTitle": "Blocked Tasks",
                "facts": self._task_facts(
                    blocked_tasks,
                    include_blocked_reason=True,
                    empty="No blocked tasks.",
                ),
                "markdown": True,
            }
        )

        high_risk_tasks = self._high_risk_tasks(tasks)
        sections.append(
            {
                "activityTitle": "High-Risk Tasks",
                "facts": self._task_facts(
                    high_risk_tasks,
                    include_priority=True,
                    empty="No high-risk tasks.",
                ),
                "markdown": True,
            }
        )

        sections.extend(self._milestone_sections(plan, tasks))
        return sections

    def _milestone_sections(
        self,
        plan: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Render milestone or task-group sections in plan order."""
        grouped = self._group_by_milestone(tasks)
        if not grouped:
            return [
                {
                    "activityTitle": "Task Groups",
                    "facts": [{"name": "Tasks", "value": "No tasks in this plan."}],
                    "markdown": True,
                }
            ]

        milestone_order = self._milestone_order(plan)
        ordered_names = [name for name in milestone_order if name in grouped] + [
            name for name in grouped if name not in milestone_order
        ]

        sections = []
        for milestone in ordered_names:
            milestone_tasks = grouped[milestone]
            counts = Counter(task.get("status") or "pending" for task in milestone_tasks)
            sections.append(
                {
                    "activityTitle": self._escape_markdown(milestone),
                    "facts": [
                        {
                            "name": "Progress",
                            "value": (
                                f"{counts.get('completed', 0)}/{len(milestone_tasks)} "
                                f"completed, {counts.get('blocked', 0)} blocked"
                            ),
                        },
                        *self._task_facts(milestone_tasks, empty="No tasks."),
                    ],
                    "markdown": True,
                }
            )
        return sections

    def _task_facts(
        self,
        tasks: list[dict[str, Any]],
        *,
        include_blocked_reason: bool = False,
        include_priority: bool = False,
        empty: str,
    ) -> list[dict[str, str]]:
        """Render compact Teams facts for tasks."""
        if not tasks:
            return [{"name": "Tasks", "value": empty}]

        facts = []
        visible_tasks = tasks[: self.MAX_SECTION_TASKS]
        for task in visible_tasks:
            value_parts = [
                f"status: {task.get('status') or 'pending'}",
                f"owner: {self._owner_text(task)}",
                f"deps: {self._dependency_text(task)}",
            ]
            priority = self._priority_text(task)
            if include_priority and priority:
                value_parts.append(f"risk: {self._escape_markdown(priority)}")
            if include_blocked_reason:
                value_parts.append(
                    "blocked: "
                    + self._escape_markdown(
                        self._compact_text(self._blocked_reason(task), self.MAX_REASON_LENGTH)
                    )
                )
            facts.append(
                {
                    "name": self._escape_markdown(task["id"]),
                    "value": (
                        self._escape_markdown(
                            self._compact_text(task["title"], self.MAX_TASK_TITLE_LENGTH)
                        )
                        + " | "
                        + " | ".join(value_parts)
                    ),
                }
            )

        hidden_count = len(tasks) - len(visible_tasks)
        if hidden_count > 0:
            facts.append({"name": "More", "value": f"{hidden_count} additional tasks hidden."})
        return facts

    def _status_count_text(self, tasks: list[dict[str, Any]]) -> str:
        """Render status counts in a stable compact string."""
        counts = Counter(task.get("status") or "pending" for task in tasks)
        return " | ".join(f"{status}: {counts.get(status, 0)}" for status in self.STATUS_ORDER)

    def _summary_text(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> str:
        """Render top-level Teams notification summary."""
        milestone_count = len(plan.get("milestones", [])) or len(self._group_by_milestone(tasks))
        return (
            f"{brief['title']} has {len(tasks)} tasks across {milestone_count} milestones "
            f"for plan {plan['id']}."
        )

    def _high_risk_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return incomplete tasks with high priority or high risk markers."""
        return [
            task
            for task in tasks
            if task.get("status") not in {"completed", "skipped"} and self._is_high_risk(task)
        ]

    def _is_high_risk(self, task: dict[str, Any]) -> bool:
        """Return whether a task should appear in the high-risk section."""
        metadata = task.get("metadata") or {}
        values = [
            task.get("priority"),
            task.get("risk_level"),
            metadata.get("priority"),
            metadata.get("teams_priority"),
            metadata.get("risk_level"),
        ]
        high_values = {"p0", "p1", "urgent", "critical", "high", "highest", "blocker"}
        return any(str(value).strip().lower() in high_values for value in values if value)

    def _group_by_milestone(
        self,
        tasks: list[dict[str, Any]],
    ) -> "OrderedDict[str, list[dict[str, Any]]]":
        """Group tasks by milestone while preserving first-seen task order."""
        grouped: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
        for task in tasks:
            milestone = task.get("milestone") or "Ungrouped"
            grouped.setdefault(milestone, []).append(task)
        return grouped

    def _milestone_order(self, plan: dict[str, Any]) -> list[str]:
        """Return milestone names from the plan in declared order."""
        names = []
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            if isinstance(milestone, dict):
                names.append(
                    milestone.get("name") or milestone.get("title") or f"Milestone {index}"
                )
            elif isinstance(milestone, str):
                names.append(milestone)
        return names

    def _owner_text(self, task: dict[str, Any]) -> str:
        """Render a Teams owner value as plain text."""
        metadata = task.get("metadata") or {}
        owner = (
            metadata.get("teams_owner")
            or metadata.get("owner")
            or metadata.get("assignee")
            or task.get("owner_type")
            or "unassigned"
        )
        return self._escape_markdown(str(owner).strip() or "unassigned")

    def _dependency_text(self, task: dict[str, Any]) -> str:
        """Render task dependency context."""
        dependencies = task.get("depends_on") or []
        if dependencies:
            return self._escape_markdown(", ".join(dependencies))
        return "none"

    def _priority_text(self, task: dict[str, Any]) -> str:
        """Return the first explicit priority-like value for display."""
        metadata = task.get("metadata") or {}
        for value in (
            task.get("priority"),
            metadata.get("teams_priority"),
            metadata.get("priority"),
            task.get("risk_level"),
            metadata.get("risk_level"),
        ):
            if value:
                return str(value)
        return ""

    def _blocked_reason(self, task: dict[str, Any]) -> str:
        """Get the best available blocked reason for a task."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason") or "No reason provided"

    def _theme_color(
        self,
        blocked_tasks: list[dict[str, Any]],
        high_risk_tasks: list[dict[str, Any]],
    ) -> str:
        """Pick a simple status color for Teams clients that honor MessageCard colors."""
        if blocked_tasks:
            return "D13438"
        if high_risk_tasks:
            return "FFB900"
        return "107C10"

    def _plain_text(self, value: Any) -> str:
        """Normalize text without Markdown escaping."""
        return " ".join(str(value).split())

    def _compact_text(self, value: Any, max_length: int) -> str:
        """Render a single-line value with deterministic truncation."""
        text = self._plain_text(value)
        if len(text) <= max_length:
            return text
        return text[: max_length - 3].rstrip() + "..."

    def _escape_markdown(self, value: Any) -> str:
        """Escape Markdown-sensitive characters used by Teams message cards."""
        text = self._plain_text(value)
        return re.sub(r"([\\`*_{}\[\]()#+.!|<>])", r"\\\1", text)
