"""Trello board JSON exporter for execution plans."""

from __future__ import annotations

import json
import re
from typing import Any

from blueprint.exporters.base import TargetExporter


SCHEMA_VERSION = "blueprint.trello.v1"


class TrelloJsonExporter(TargetExporter):
    """Export execution tasks as a Trello-compatible board JSON payload."""

    STATUS_LISTS = ["pending", "in_progress", "blocked", "completed", "skipped"]
    LABEL_COLORS = {
        "suggested_engine": "blue",
        "owner_type": "green",
        "complexity": "orange",
    }

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
        """Write a Trello board JSON artifact."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        with open(output_path, "w") as f:
            json.dump(self.render_payload(plan, brief), f, indent=2, sort_keys=True)
            f.write("\n")

        return output_path

    def render_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the deterministic Trello board payload."""
        tasks = sorted(plan.get("tasks", []), key=lambda task: task["id"])
        tasks_by_id = {task["id"]: task for task in tasks}
        lists = self._lists(plan, tasks)
        list_ids_by_name = {list_item["name"]: list_item["id"] for list_item in lists}
        labels = self._labels(tasks)
        label_ids_by_name = {label["name"]: label["id"] for label in labels}
        cards = [
            self._card(plan, brief, task, tasks_by_id, list_ids_by_name, label_ids_by_name)
            for task in tasks
        ]
        card_ids_by_list = {
            list_item["id"]: [card["id"] for card in cards if card["idList"] == list_item["id"]]
            for list_item in lists
        }
        for list_item in lists:
            list_item["cards"] = card_ids_by_list[list_item["id"]]

        return {
            "schema_version": SCHEMA_VERSION,
            "exporter": "trello-json",
            "board": {
                "id": self._stable_id("board", plan["id"]),
                "name": f"{brief['title']} ({plan['id']})",
                "desc": self._board_description(plan, brief, len(tasks)),
                "metadata": {
                    "planId": plan["id"],
                    "implementationBriefId": brief["id"],
                    "targetRepo": plan.get("target_repo"),
                    "projectType": plan.get("project_type"),
                },
            },
            "lists": lists,
            "labels": labels,
            "cards": cards,
        }

    def _lists(self, plan: dict[str, Any], tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build board lists from milestones, falling back to workflow statuses."""
        milestone_names = [
            str(milestone.get("name")).strip()
            for milestone in plan.get("milestones", [])
            if isinstance(milestone, dict) and str(milestone.get("name", "")).strip()
        ]
        task_milestones = [
            str(task.get("milestone")).strip()
            for task in tasks
            if str(task.get("milestone") or "").strip()
        ]
        names = self._dedupe(milestone_names + task_milestones)
        grouping = "milestone"
        if names:
            if any(not str(task.get("milestone") or "").strip() for task in tasks):
                names.append("Ungrouped")
        else:
            names = self.STATUS_LISTS.copy()
            grouping = "status"

        return [
            {
                "id": self._stable_id("list", name),
                "name": name,
                "source": grouping,
                "pos": index,
                "cards": [],
            }
            for index, name in enumerate(names, 1)
        ]

    def _card(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
        tasks_by_id: dict[str, dict[str, Any]],
        list_ids_by_name: dict[str, str],
        label_ids_by_name: dict[str, str],
    ) -> dict[str, Any]:
        """Build one Trello card for an execution task."""
        list_name = self._list_name_for_task(task, list_ids_by_name)
        labels = self._task_label_names(task)
        task_id = task["id"]
        return {
            "id": self._stable_id("card", plan["id"], task_id),
            "name": f"{task_id}: {task['title']}",
            "desc": self._description(plan, brief, task, tasks_by_id),
            "idList": list_ids_by_name[list_name],
            "idLabels": [label_ids_by_name[label] for label in labels if label in label_ids_by_name],
            "labels": labels,
            "checklists": [
                {
                    "id": self._stable_id("checklist", plan["id"], task_id, "acceptance"),
                    "name": "Acceptance Criteria",
                    "items": [
                        {"name": criterion, "checked": False}
                        for criterion in task.get("acceptance_criteria") or []
                    ],
                }
            ],
            "metadata": {
                "planId": plan["id"],
                "taskId": task_id,
                "milestone": task.get("milestone"),
                "status": task.get("status") or "pending",
                "suggestedEngine": task.get("suggested_engine"),
                "ownerType": task.get("owner_type"),
                "estimatedComplexity": task.get("estimated_complexity"),
                "dependsOn": task.get("depends_on") or [],
                "filesOrModules": task.get("files_or_modules") or [],
            },
        }

    def _description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task: dict[str, Any],
        tasks_by_id: dict[str, dict[str, Any]],
    ) -> str:
        """Render task context into a Trello Markdown card description."""
        lines = [
            task["description"],
            "",
            "## Blueprint Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Task ID: `{task['id']}`",
            f"- Implementation Brief: `{brief['id']}` - {brief['title']}",
            f"- Milestone: {task.get('milestone') or 'Ungrouped'}",
            f"- Status: {task.get('status') or 'pending'}",
            f"- Suggested Engine: {task.get('suggested_engine') or 'N/A'}",
            f"- Owner Type: {task.get('owner_type') or 'N/A'}",
            f"- Complexity: {task.get('estimated_complexity') or 'N/A'}",
            "",
            "## Files/Modules",
        ]
        lines.extend(self._bullet_lines(task.get("files_or_modules")))
        lines.extend(["", "## Dependencies"])
        dependencies = task.get("depends_on") or []
        if dependencies:
            for dependency_id in dependencies:
                dependency = tasks_by_id.get(dependency_id)
                if dependency:
                    lines.append(f"- `{dependency_id}` - {dependency['title']}")
                else:
                    lines.append(f"- `{dependency_id}` - missing from plan")
        else:
            lines.append("- None")
        lines.extend(["", "## Validation"])
        lines.append(f"- Test Strategy: {plan.get('test_strategy') or 'N/A'}")
        lines.append(f"- Brief Validation Plan: {brief.get('validation_plan') or 'N/A'}")
        return "\n".join(lines).strip() + "\n"

    def _labels(self, tasks: list[dict[str, Any]]) -> list[dict[str, str]]:
        """Build board labels from task execution metadata."""
        label_names: list[tuple[str, str]] = []
        for task in tasks:
            for field, label in self._task_label_pairs(task):
                if (field, label) not in label_names:
                    label_names.append((field, label))

        return [
            {
                "id": self._stable_id("label", label),
                "name": label,
                "color": self.LABEL_COLORS[field],
            }
            for field, label in label_names
        ]

    def _task_label_names(self, task: dict[str, Any]) -> list[str]:
        """Return normalized labels for one task."""
        return [label for _, label in self._task_label_pairs(task)]

    def _task_label_pairs(self, task: dict[str, Any]) -> list[tuple[str, str]]:
        """Return label source fields and names for one task."""
        pairs: list[tuple[str, str]] = []
        values = {
            "suggested_engine": task.get("suggested_engine"),
            "owner_type": task.get("owner_type"),
            "complexity": task.get("estimated_complexity"),
        }
        for field, value in values.items():
            if isinstance(value, str) and value.strip():
                pairs.append((field, f"{field}: {value.strip()}"))
        return pairs

    def _list_name_for_task(
        self,
        task: dict[str, Any],
        list_ids_by_name: dict[str, str],
    ) -> str:
        """Resolve the list name for one task."""
        milestone = str(task.get("milestone") or "").strip()
        if milestone and milestone in list_ids_by_name:
            return milestone
        if "Ungrouped" in list_ids_by_name:
            return "Ungrouped"
        return str(task.get("status") or "pending")

    def _board_description(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        task_count: int,
    ) -> str:
        """Render board-level metadata for Trello import context."""
        return "\n".join(
            [
                f"Blueprint execution plan `{plan['id']}` for `{brief['id']}`.",
                f"Target repository: {plan.get('target_repo') or 'N/A'}",
                f"Plan status: {plan.get('status') or 'N/A'}",
                f"Total tasks: {task_count}",
            ]
        )

    def _bullet_lines(self, values: list[str] | None) -> list[str]:
        """Render values as Markdown bullets."""
        items = values or []
        if not items:
            return ["- None"]
        return [f"- {item}" for item in items]

    def _dedupe(self, values: list[str]) -> list[str]:
        """Return non-empty strings in first-seen order."""
        deduped: list[str] = []
        for value in values:
            if value and value not in deduped:
                deduped.append(value)
        return deduped

    def _stable_id(self, *parts: str) -> str:
        """Build a deterministic Trello-style identifier for import references."""
        raw = "-".join(str(part) for part in parts if str(part))
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("-").lower()
        return slug or "trello-item"
