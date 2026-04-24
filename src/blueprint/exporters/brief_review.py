"""Markdown review packet exporter for implementation briefs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from blueprint.domain import ImplementationBrief, SourceBrief


class BriefReviewPacketExporter:
    """Render implementation briefs as compact human review packets."""

    OPTIONAL_REVIEW_FIELDS = (
        ("target_user", "Who is the primary target user?"),
        ("buyer", "Who is the buyer or decision-maker?"),
        ("workflow_context", "What workflow context should planning preserve?"),
        ("product_surface", "What product surface is in scope?"),
        ("architecture_notes", "Are there architecture constraints or preferences?"),
        ("data_requirements", "What data requirements must the plan account for?"),
        ("integration_points", "Which integration points should be considered?"),
    )

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        implementation_brief: dict[str, Any],
        output_path: str,
        *,
        source_brief: dict[str, Any] | None = None,
        include_source: bool = False,
    ) -> str:
        """Export a review packet to a Markdown file."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(
            self.render(
                implementation_brief,
                source_brief=source_brief,
                include_source=include_source,
            )
        )
        return output_path

    def render(
        self,
        implementation_brief: dict[str, Any],
        *,
        source_brief: dict[str, Any] | None = None,
        include_source: bool = False,
    ) -> str:
        """Render a deterministic Markdown review packet."""
        brief = ImplementationBrief.model_validate(implementation_brief).model_dump(
            mode="python"
        )
        source = self._validate_source(source_brief) if source_brief else None

        lines = [
            f"# Review Packet: {brief['title']}",
            "",
            "## Traceability",
            f"- Implementation Brief ID: `{brief['id']}`",
            f"- Source Brief ID: `{brief['source_brief_id']}`",
            f"- Status: {brief.get('status') or 'N/A'}",
            f"- Domain: {brief.get('domain') or 'N/A'}",
        ]

        if include_source:
            lines.extend(["", "## Source Metadata"])
            if source:
                lines.extend(
                    [
                        f"- Source Brief ID: `{source['id']}`",
                        f"- Source Project: {source['source_project']}",
                        f"- Source Entity Type: {source['source_entity_type']}",
                        f"- Source ID: `{source['source_id']}`",
                        "- Source Links:",
                    ]
                )
                lines.extend(self._mapping_lines(source.get("source_links"), indent="  "))
                lines.extend(
                    [
                        "- Source Payload Summary:",
                        f"  - {self._source_payload_summary(source.get('source_payload'))}",
                    ]
                )
            else:
                lines.append("- Source brief not loaded.")

        lines.extend(
            [
                "",
                "## Normalized Summary",
                f"- Problem: {brief['problem_statement']}",
                f"- MVP Goal: {brief['mvp_goal']}",
                f"- Target Users: {brief.get('target_user') or 'N/A'}",
                f"- Buyer: {brief.get('buyer') or 'N/A'}",
                f"- Workflow Context: {brief.get('workflow_context') or 'N/A'}",
                f"- Product Surface: {brief.get('product_surface') or 'N/A'}",
                "",
                "## Scope",
            ]
        )
        lines.extend(self._bullet_lines(brief.get("scope")))
        lines.extend(["", "## Non-Goals"])
        lines.extend(self._bullet_lines(brief.get("non_goals")))
        lines.extend(["", "## Assumptions"])
        lines.extend(self._bullet_lines(brief.get("assumptions")))
        lines.extend(["", "## Planning Notes"])
        lines.extend(
            [
                f"- Architecture Notes: {brief.get('architecture_notes') or 'N/A'}",
                f"- Data Requirements: {brief.get('data_requirements') or 'N/A'}",
                "- Integration Points:",
            ]
        )
        lines.extend(self._bullet_lines(brief.get("integration_points"), indent="  "))
        lines.extend(["", "## Risks"])
        lines.extend(self._bullet_lines(brief.get("risks")))
        lines.extend(["", "## Validation Plan", brief["validation_plan"]])
        lines.extend(["", "## Definition of Done"])
        lines.extend(self._bullet_lines(brief.get("definition_of_done")))
        lines.extend(["", "## Review Questions"])
        lines.extend(self._review_question_lines(brief))

        return "\n".join(lines).rstrip() + "\n"

    def _validate_source(self, source_brief: dict[str, Any]) -> dict[str, Any]:
        """Validate source brief data before rendering."""
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")

    def _review_question_lines(self, brief: dict[str, Any]) -> list[str]:
        """Infer review questions for optional planning fields that are absent."""
        questions = [
            question
            for field, question in self.OPTIONAL_REVIEW_FIELDS
            if self._is_missing(brief.get(field))
        ]
        if not questions:
            return ["- None."]
        return [f"- {question}" for question in questions]

    def _is_missing(self, value: Any) -> bool:
        """Return whether a value should prompt a review question."""
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, list | dict):
            return len(value) == 0
        return False

    def _bullet_lines(self, value: list[str] | None, indent: str = "") -> list[str]:
        """Render list values as Markdown bullets."""
        items = value or []
        if not items:
            return [f"{indent}- None"]
        return [f"{indent}- {item}" for item in items]

    def _mapping_lines(self, value: dict[str, Any] | None, indent: str = "") -> list[str]:
        """Render mapping values in sorted key order."""
        items = value or {}
        if not items:
            return [f"{indent}- None"]
        return [
            f"{indent}- {key}: {self._compact_json(items[key])}"
            for key in sorted(items)
        ]

    def _source_payload_summary(self, value: dict[str, Any] | None) -> str:
        """Build a concise deterministic summary of the source payload."""
        payload = value or {}
        if not payload:
            return "No source payload fields."

        keys = sorted(payload)
        visible = keys[:5]
        field_summary = ", ".join(
            f"{key}={self._compact_json(payload[key], max_length=80)}" for key in visible
        )
        remaining = len(keys) - len(visible)
        if remaining > 0:
            field_summary = f"{field_summary}; +{remaining} more field(s)"
        return field_summary

    def _compact_json(self, value: Any, *, max_length: int = 120) -> str:
        """Serialize a value deterministically for compact Markdown display."""
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        if len(rendered) <= max_length:
            return rendered
        return f"{rendered[: max_length - 3]}..."
