"""File impact map exporter for execution plans."""

from __future__ import annotations

from typing import Any

from blueprint.exporters.base import TargetExporter


UNASSIGNED_SECTION = "unassigned"


class FileImpactMapExporter(TargetExporter):
    """Export tasks grouped by the files or modules they declare."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an execution plan file impact map to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render_markdown(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render_payload(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Render a JSON-serializable file impact map."""
        tasks = plan.get("tasks", [])
        grouped: dict[str, list[dict[str, Any]]] = {}
        unassigned_tasks: list[dict[str, Any]] = []

        for task in sorted(tasks, key=lambda item: item["id"]):
            files_or_modules = task.get("files_or_modules") or []
            task_summary = self._task_summary(task)
            if not files_or_modules:
                unassigned_tasks.append(task_summary)
                continue

            for file_or_module in sorted(set(files_or_modules)):
                grouped.setdefault(file_or_module, []).append(task_summary)

        sections = [
            {
                "file_or_module": file_or_module,
                "task_count": len(section_tasks),
                "tasks": section_tasks,
            }
            for file_or_module, section_tasks in sorted(grouped.items())
        ]

        return {
            "plan_id": plan["id"],
            "implementation_brief_id": plan["implementation_brief_id"],
            "total_tasks": len(tasks),
            "impacted_files_or_modules": len(sections),
            "sections": sections,
            "unassigned": {
                "file_or_module": UNASSIGNED_SECTION,
                "task_count": len(unassigned_tasks),
                "tasks": unassigned_tasks,
            },
        }

    def render_markdown(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render a readable Markdown file impact map."""
        payload = self.render_payload(plan)
        lines = [
            f"# File Impact Map: {plan['id']}",
            "",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Title: {brief['title']}",
            f"- Plan Status: {plan.get('status') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Tasks: {payload['total_tasks']}",
            f"- Impacted Files or Modules: {payload['impacted_files_or_modules']}",
            f"- Unassigned Tasks: {payload['unassigned']['task_count']}",
            "",
            "## Files and Modules",
            "",
        ]

        if not payload["sections"]:
            lines.extend(["No declared file or module impacts.", ""])
        else:
            for section in payload["sections"]:
                lines.extend(self._section_lines(section))
                lines.append("")

        lines.extend(["## Unassigned", ""])
        unassigned = payload["unassigned"]
        if not unassigned["tasks"]:
            lines.extend(["No unassigned tasks.", ""])
        else:
            lines.extend(
                [
                    f"_Tasks without files_or_modules: {unassigned['task_count']}_",
                    "",
                ]
            )
            for task in unassigned["tasks"]:
                lines.extend(self._task_lines(task))
                lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _section_lines(self, section: dict[str, Any]) -> list[str]:
        """Render one file or module section."""
        lines = [
            f"### `{section['file_or_module']}`",
            "",
            f"_Tasks: {section['task_count']}_",
            "",
        ]
        for task in section["tasks"]:
            lines.extend(self._task_lines(task))
            lines.append("")
        return lines[:-1]

    def _task_lines(self, task: dict[str, Any]) -> list[str]:
        """Render one task impact summary."""
        return [
            f"- `{task['id']}` {task['title']}",
            f"  - Status: {task['status']}",
            f"  - Milestone: {task['milestone']}",
            f"  - Dependency Count: {task['dependency_count']}",
            f"  - Suggested Engine: {task['suggested_engine']}",
        ]

    def _task_summary(self, task: dict[str, Any]) -> dict[str, Any]:
        """Return focused task fields for file impact planning."""
        return {
            "id": task["id"],
            "title": task["title"],
            "status": task.get("status") or "pending",
            "milestone": task.get("milestone") or "Ungrouped",
            "dependency_count": len(task.get("depends_on") or []),
            "suggested_engine": task.get("suggested_engine") or "N/A",
        }
