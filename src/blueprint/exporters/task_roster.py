"""Task assignment roster exporter for execution plans."""

from __future__ import annotations

from typing import Any


UNASSIGNED = "unassigned"


class TaskRosterExporter:
    """Render execution tasks grouped for owner and engine handoff planning."""

    def render_json(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Render a JSON-serializable task assignment roster."""
        groups = [
            {
                "owner_type": owner_type,
                "suggested_engine": suggested_engine,
                "tasks": tasks,
            }
            for (owner_type, suggested_engine), tasks in self._group_tasks(plan).items()
        ]
        return {
            "plan_id": plan["id"],
            "groups": groups,
        }

    def render_markdown(self, plan: dict[str, Any]) -> str:
        """Render a readable Markdown task assignment roster."""
        lines = [
            f"# Task Assignment Roster: {plan['id']}",
            "",
            f"- Plan ID: `{plan['id']}`",
            f"- Status: {plan.get('status') or 'N/A'}",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            "",
        ]

        groups = self._group_tasks(plan)
        if not groups:
            lines.append("No tasks defined.")
            return "\n".join(lines) + "\n"

        for (owner_type, suggested_engine), tasks in groups.items():
            lines.extend(
                [
                    f"## Owner: {owner_type} / Engine: {suggested_engine}",
                    "",
                ]
            )
            for task in tasks:
                lines.extend(
                    [
                        f"### {task['id']} - {task['title']}",
                        f"- Status: {task.get('status') or 'pending'}",
                        f"- Dependencies: {self._inline_list(task.get('dependencies'))}",
                        f"- Files or Modules: {self._inline_list(task.get('files_or_modules'))}",
                        "- Acceptance Criteria:",
                    ]
                )
                lines.extend(self._bullet_lines(task.get("acceptance_criteria"), indent="  "))
                lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _group_tasks(
        self,
        plan: dict[str, Any],
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        """Return task summaries grouped and sorted by owner and engine."""
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for task in plan.get("tasks", []):
            owner_type = task.get("owner_type") or UNASSIGNED
            suggested_engine = task.get("suggested_engine") or UNASSIGNED
            grouped.setdefault((owner_type, suggested_engine), []).append(self._task_summary(task))

        return {
            key: sorted(tasks, key=lambda task: task["id"])
            for key, tasks in sorted(grouped.items(), key=lambda item: item[0])
        }

    def _task_summary(self, task: dict[str, Any]) -> dict[str, Any]:
        """Return focused task fields for the roster."""
        return {
            "id": task["id"],
            "title": task["title"],
            "status": task.get("status") or "pending",
            "dependencies": task.get("depends_on") or [],
            "files_or_modules": task.get("files_or_modules") or [],
            "acceptance_criteria": task.get("acceptance_criteria") or [],
        }

    def _inline_list(self, value: list[str] | None) -> str:
        """Render a short list field inline."""
        return ", ".join(value or []) or "none"

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render a list field as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]
