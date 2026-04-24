"""Normalized source brief exporter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from blueprint.domain import SourceBrief


class SourceBriefExporter:
    """Render normalized source briefs for external review."""

    FIELDS = (
        "id",
        "title",
        "domain",
        "summary",
        "source_project",
        "source_entity_type",
        "source_id",
        "source_links",
        "source_payload",
        "created_at",
        "updated_at",
    )

    def export(
        self,
        source_brief: dict[str, Any],
        output_path: str,
        *,
        output_format: str = "markdown",
    ) -> str:
        """Export a source brief to a file."""
        rendered = self.render(source_brief, output_format=output_format)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(rendered)
        return output_path

    def render(
        self,
        source_brief: dict[str, Any],
        *,
        output_format: str = "markdown",
    ) -> str:
        """Render a source brief in the requested format."""
        if output_format == "markdown":
            return self.render_markdown(source_brief)
        if output_format == "json":
            return self.render_json(source_brief)
        raise ValueError(f"Unsupported source brief export format: {output_format}")

    def render_json(self, source_brief: dict[str, Any]) -> str:
        """Render a deterministic pretty JSON payload."""
        return json.dumps(self.build_payload(source_brief), indent=2, sort_keys=True) + "\n"

    def render_markdown(self, source_brief: dict[str, Any]) -> str:
        """Render deterministic Markdown for external review."""
        payload = self.build_payload(source_brief)
        lines = [
            f"# Source Brief: {payload['title']}",
            "",
            "## Metadata",
            f"- Source Brief ID: `{payload['id']}`",
            f"- Domain: {payload.get('domain') or 'N/A'}",
            "",
            "## Source Identity",
            f"- Source Project: {payload['source_project']}",
            f"- Source Entity Type: {payload['source_entity_type']}",
            f"- Source ID: `{payload['source_id']}`",
            "",
            "## Summary",
            payload["summary"],
            "",
            "## Source Links",
        ]
        lines.extend(self._mapping_lines(payload.get("source_links")))
        lines.extend(["", "## Source Payload"])
        lines.extend(self._payload_lines(payload.get("source_payload")))
        return "\n".join(lines).rstrip() + "\n"

    def build_payload(self, source_brief: dict[str, Any]) -> dict[str, Any]:
        """Build the canonical normalized payload used by every export format."""
        validated = SourceBrief.model_validate(source_brief).model_dump(mode="json")
        return {field: validated.get(field) for field in self.FIELDS}

    def _mapping_lines(self, value: dict[str, Any] | None) -> list[str]:
        """Render mapping values in sorted key order."""
        items = value or {}
        if not items:
            return ["- None"]
        return [f"- {key}: {self._compact_json(items[key])}" for key in sorted(items)]

    def _payload_lines(self, value: dict[str, Any] | None) -> list[str]:
        """Render top-level source payload fields compactly in sorted order."""
        payload = value or {}
        if not payload:
            return ["- None"]
        return [
            f"- {key}: {self._compact_json(payload[key], max_length=240)}"
            for key in sorted(payload)
        ]

    def _compact_json(self, value: Any, *, max_length: int = 160) -> str:
        """Serialize a value deterministically for compact Markdown display."""
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        if len(rendered) <= max_length:
            return rendered
        return f"{rendered[: max_length - 3]}..."
