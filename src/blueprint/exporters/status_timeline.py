"""Markdown and JSON status history timeline exporter."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class StatusTimelineExporter:
    """Export status history events for one entity."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        entity_id: str,
        events: list[dict[str, Any]],
        output_path: str,
    ) -> str:
        """Write a Markdown status timeline and return the output path."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render_markdown(entity_id, events))
        return str(path)

    def render_markdown(self, entity_id: str, events: list[dict[str, Any]]) -> str:
        """Render status events as a chronological Markdown timeline."""
        ordered_events = self._ordered_events(events)
        lines = [
            f"# Status Timeline: `{entity_id}`",
            "",
            f"- Entity ID: `{entity_id}`",
            f"- Event Count: {len(ordered_events)}",
            "",
            "## Events",
            "",
        ]

        if not ordered_events:
            lines.append(f"- No status history events found for `{entity_id}`.")
            return "\n".join(lines) + "\n"

        for index, event in enumerate(ordered_events, 1):
            timestamp = event.get("created_at") or "N/A"
            entity_type = event.get("entity_type") or "N/A"
            old_status = event.get("old_status") or "N/A"
            new_status = event.get("new_status") or "N/A"
            reason = event.get("reason") or "N/A"
            lines.extend(
                [
                    f"{index}. `{timestamp}`",
                    f"   - Entity Type: {entity_type}",
                    f"   - Status: `{old_status}` -> `{new_status}`",
                    f"   - Reason: {reason}",
                ]
            )

        return "\n".join(lines) + "\n"

    def render_json(self, entity_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Render status events as a JSON-serializable payload."""
        ordered_events = self._ordered_events(events)
        return {
            "entity_id": entity_id,
            "event_count": len(ordered_events),
            "events": ordered_events,
        }

    def _ordered_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return events ordered by creation time without mutating caller data."""
        return sorted(
            (dict(event) for event in events),
            key=lambda event: event.get("created_at") or "",
        )
