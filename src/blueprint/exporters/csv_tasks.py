"""CSV task exporter for spreadsheet and tracker workflows."""

import csv
from typing import Any

from blueprint.exporters.base import TargetExporter


class CsvTasksExporter(TargetExporter):
    """Export execution-plan tasks as one CSV row per task."""

    FIELDNAMES = [
        "plan_id",
        "task_id",
        "title",
        "milestone",
        "status",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "estimated_complexity",
        "acceptance_criteria",
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
        """Export execution-plan tasks to CSV."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            for task in execution_plan["tasks"]:
                writer.writerow(self._task_row(execution_plan["id"], task))

        return output_path

    def _task_row(self, plan_id: str, task: dict[str, Any]) -> dict[str, str]:
        """Build a flat CSV row for a validated execution task."""
        return {
            "plan_id": plan_id,
            "task_id": task["id"],
            "title": task["title"],
            "milestone": task.get("milestone") or "",
            "status": task.get("status") or "",
            "suggested_engine": task.get("suggested_engine") or "",
            "depends_on": self._join_list(task.get("depends_on")),
            "files_or_modules": self._join_list(task.get("files_or_modules")),
            "estimated_complexity": task.get("estimated_complexity") or "",
            "acceptance_criteria": self._join_list(task.get("acceptance_criteria")),
        }

    def _join_list(self, value: list[str] | None) -> str:
        """Join list fields into spreadsheet-friendly cell values."""
        return "; ".join(value or [])
