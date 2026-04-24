"""Dependency wave schedule exporter for autonomous agents."""

from __future__ import annotations

import json
from typing import Any

from blueprint.audits.execution_waves import analyze_execution_waves
from blueprint.exporters.base import TargetExporter


SCHEMA_VERSION = "blueprint.wave_schedule.v1"


class WaveScheduleExporter(TargetExporter):
    """Export execution-plan tasks grouped by dependency-ready waves."""

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
        """Export dependency wave schedule JSON."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        payload = self._payload(execution_plan)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=False)
            f.write("\n")

        return output_path

    def _payload(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Build the schedule payload from existing wave analysis."""
        wave_result = analyze_execution_waves(plan)
        tasks_by_id = {task["id"]: task for task in plan.get("tasks", [])}
        waves = []

        for wave in wave_result.waves:
            wave_tasks = [
                self._task_payload(
                    task=tasks_by_id[task_id],
                    wave_number=wave.wave_number,
                )
                for task_id in wave.task_ids
            ]
            waves.append(
                {
                    "wave_number": wave.wave_number,
                    "task_ids": wave.task_ids,
                    "tasks": wave_tasks,
                }
            )

        return {
            "schema_version": SCHEMA_VERSION,
            "plan_id": plan["id"],
            "total_waves": len(waves),
            "task_count": wave_result.task_count,
            "waves": waves,
        }

    def _task_payload(self, task: dict[str, Any], wave_number: int) -> dict[str, Any]:
        """Build one task entry for a wave."""
        status = task.get("status") or "pending"
        return {
            "id": task["id"],
            "title": task["title"],
            "wave_number": wave_number,
            "suggested_engine": task.get("suggested_engine"),
            "owner_type": task.get("owner_type"),
            "files_or_modules": task.get("files_or_modules") or [],
            "dependencies": task.get("depends_on") or [],
            "status": status,
            "status_metadata": {
                "blocked": status == "blocked",
                "skipped": status == "skipped",
                "blocked_reason": self._blocked_reason(task),
            },
        }

    def _blocked_reason(self, task: dict[str, Any]) -> str | None:
        """Return blocked reason from first-class field or metadata."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason")
