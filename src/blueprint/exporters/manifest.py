"""Export manifest builder for rendered plan artifacts."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from blueprint.store import Store


class ExportManifestExporter:
    """Build a machine-readable inventory of exports for one execution plan."""

    def build(
        self,
        store: Store,
        plan_id: str,
        *,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Build a manifest for export records associated with a plan."""
        timestamp = generated_at or datetime.now(UTC)
        records = store.list_export_records(plan_id=plan_id, limit=None)

        return {
            "generated_at": self._format_timestamp(timestamp),
            "plan_id": plan_id,
            "exports": [
                self._export_entry(record)
                for record in sorted(records, key=self._sort_key)
            ],
        }

    def _export_entry(self, record: dict[str, Any]) -> dict[str, Any]:
        """Build one manifest export entry from a stored export record."""
        path = self._resolve_path(record["output_path"])
        exists = path.exists()
        entry: dict[str, Any] = {
            "checksum": None,
            "exists": exists,
            "export_record_id": record["id"],
            "exported_at": record.get("exported_at"),
            "format": record["export_format"],
            "path": str(path),
            "size_bytes": None,
            "target_engine": record["target_engine"],
        }

        if exists and path.is_file():
            entry["size_bytes"] = path.stat().st_size
            entry["checksum"] = self._sha256(path)

        return entry

    def _resolve_path(self, output_path: str) -> Path:
        """Resolve an export record path to an absolute filesystem path."""
        return Path(output_path).expanduser().resolve()

    def _sha256(self, path: Path) -> str:
        """Compute the SHA-256 checksum for a file."""
        digest = hashlib.sha256()
        with path.open("rb") as artifact:
            for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _format_timestamp(self, value: datetime) -> str:
        """Format timestamps as ISO-8601 UTC strings."""
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def _sort_key(self, record: dict[str, Any]) -> tuple[str, str]:
        """Sort records deterministically for stable manifest output."""
        return (record.get("exported_at") or "", record["id"])
