"""Portable archive exporter for execution plans."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.plan_graph import PlanGraphExporter
from blueprint.exporters.status_report import StatusReportExporter


class ArchiveExporter:
    """Export one execution plan and related records as a portable zip archive."""

    SCHEMA_VERSION = "1"

    def get_format(self) -> str:
        """Get export format."""
        return "zip"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".zip"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
        *,
        source_brief: dict[str, Any] | None = None,
    ) -> str:
        """Export a complete plan archive to a zip file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        files = self._build_files(
            execution_plan,
            implementation_brief,
            source_brief=source_brief,
        )
        manifest = self._build_manifest(
            execution_plan,
            implementation_brief,
            included_files=sorted([*files.keys(), "manifest.json"]),
        )
        files["manifest.json"] = self._json_bytes(manifest)

        with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as archive:
            for archive_name in sorted(files):
                archive.writestr(archive_name, files[archive_name])

        return output_path

    def _build_files(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        *,
        source_brief: dict[str, Any] | None,
    ) -> dict[str, bytes]:
        """Render archive member files."""
        files = {
            "plan.json": self._json_bytes(execution_plan),
            "implementation_brief.json": self._json_bytes(implementation_brief),
            "graph.dot": PlanGraphExporter().render(execution_plan, "dot").encode(),
            "graph.json": PlanGraphExporter().render(execution_plan, "json").encode(),
        }
        if source_brief is not None:
            files["source_brief.json"] = self._json_bytes(source_brief)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            tasks_path = temp_path / "tasks.csv"
            CsvTasksExporter().export(
                execution_plan,
                implementation_brief,
                str(tasks_path),
            )
            files["tasks.csv"] = tasks_path.read_bytes()

            status_path = temp_path / "status_report.md"
            StatusReportExporter().export(
                execution_plan,
                implementation_brief,
                str(status_path),
            )
            files["status_report.md"] = status_path.read_bytes()

        return files

    def _build_manifest(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        *,
        included_files: list[str],
    ) -> dict[str, Any]:
        """Build archive manifest metadata."""
        return {
            "schema_version": self.SCHEMA_VERSION,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "plan_id": execution_plan["id"],
            "brief_id": implementation_brief["id"],
            "task_count": len(execution_plan.get("tasks", [])),
            "included_files": included_files,
        }

    def _json_bytes(self, payload: dict[str, Any]) -> bytes:
        """Serialize a JSON archive member with stable formatting."""
        return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
