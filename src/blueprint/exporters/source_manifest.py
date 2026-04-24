"""Markdown source brief manifest exporter."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from blueprint.audits.source_duplicates import (
    DEFAULT_THRESHOLD as SOURCE_DUPLICATE_THRESHOLD,
    SourceDuplicateGroup,
    find_duplicate_source_brief_groups,
)
from blueprint.audits.source_similarity import (
    DEFAULT_THRESHOLD as SOURCE_SIMILARITY_THRESHOLD,
    find_similar_source_briefs,
)
from blueprint.domain import SourceBrief


class SourceManifestExporter:
    """Render a Markdown inventory of normalized source briefs."""

    SUMMARY_EXCERPT_LENGTH = 220
    SIMILARITY_LIMIT = 3

    def export(
        self,
        source_briefs: list[dict[str, Any]],
        output_path: str,
        *,
        source_project: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Export a source brief manifest to a Markdown file."""
        rendered = self.render_markdown(
            source_briefs,
            source_project=source_project,
            limit=limit,
        )
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(rendered)
        return output_path

    def render_markdown(
        self,
        source_briefs: list[dict[str, Any]],
        *,
        source_project: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Render a deterministic Markdown manifest grouped by project and domain."""
        briefs = self._validated_briefs(source_briefs)
        duplicate_hints = self._duplicate_hints(briefs, source_project=source_project)
        similarity_hints = self._similarity_hints(briefs)

        lines = [
            "# Source Brief Manifest",
            "",
            "## Selection",
            f"- Source Project: {source_project or 'All'}",
            f"- Limit: {limit if limit is not None else 'All'}",
            f"- Briefs: {len(briefs)}",
            "",
        ]

        if not briefs:
            lines.extend(
                [
                    "## No Matching Source Briefs",
                    "No source briefs matched the selected filters.",
                ]
            )
            return "\n".join(lines).rstrip() + "\n"

        grouped = self._group_briefs(briefs)
        for project in sorted(grouped):
            project_count = sum(len(domain_briefs) for domain_briefs in grouped[project].values())
            lines.extend([f"## Source Project: {project}", f"_Briefs: {project_count}_", ""])
            for domain in sorted(grouped[project]):
                domain_briefs = grouped[project][domain]
                lines.extend([f"### Domain: {domain}", ""])
                for brief in domain_briefs:
                    lines.extend(
                        self._brief_lines(
                            brief,
                            duplicate_hints=duplicate_hints.get(brief["id"], []),
                            similarity_hints=similarity_hints.get(brief["id"], []),
                        )
                    )
                    lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _validated_briefs(self, source_briefs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            SourceBrief.model_validate(source_brief).model_dump(mode="json")
            for source_brief in source_briefs
        ]

    def _group_briefs(
        self,
        source_briefs: list[dict[str, Any]],
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for brief in source_briefs:
            grouped[brief["source_project"] or "Unspecified"][
                brief.get("domain") or "Unspecified"
            ].append(brief)

        for domain_groups in grouped.values():
            for briefs in domain_groups.values():
                briefs.sort(key=lambda brief: (brief.get("created_at") or "", brief["id"]), reverse=True)
        return grouped

    def _brief_lines(
        self,
        brief: dict[str, Any],
        *,
        duplicate_hints: list[str],
        similarity_hints: list[str],
    ) -> list[str]:
        source_identity = (
            f"{brief['source_project']}/{brief['source_entity_type']}/{brief['source_id']}"
        )
        lines = [
            f"#### {brief['title']}",
            f"- Source Brief ID: `{brief['id']}`",
            f"- Source Identity: `{source_identity}`",
            f"- Created: {brief.get('created_at') or 'N/A'}",
            f"- Updated: {brief.get('updated_at') or 'N/A'}",
            f"- Summary: {self._excerpt(brief['summary'])}",
            "- Source Links:",
        ]
        lines.extend(self._source_link_lines(brief.get("source_links")))

        hints = duplicate_hints + similarity_hints
        if hints:
            lines.append("- Duplicate/Similarity Hints:")
            lines.extend(f"  - {hint}" for hint in hints)

        return lines

    def _source_link_lines(self, source_links: dict[str, Any] | None) -> list[str]:
        links = source_links or {}
        if not links:
            return ["  - None"]
        return [
            f"  - {key}: {self._compact_json(links[key])}"
            for key in sorted(links)
        ]

    def _duplicate_hints(
        self,
        source_briefs: list[dict[str, Any]],
        *,
        source_project: str | None,
    ) -> dict[str, list[str]]:
        report = find_duplicate_source_brief_groups(
            source_briefs,
            threshold=SOURCE_DUPLICATE_THRESHOLD,
            limit=max(len(source_briefs), 1),
            source_project=source_project,
        )
        hints: dict[str, list[str]] = defaultdict(list)
        for group in report.groups:
            group_hints = self._group_hints(group)
            for brief in group.briefs:
                hints[brief.id].extend(group_hints)
        return hints

    def _group_hints(self, group: SourceDuplicateGroup) -> list[str]:
        duplicate_ids = ", ".join(brief.id for brief in group.briefs)
        evidence = "; ".join(
            f"{pair.left_id}<->{pair.right_id} {pair.score:.4f} "
            f"({', '.join(pair.matched_fields)})"
            for pair in group.pairs
        )
        return [
            (
                f"Duplicate group canonical `{group.canonical_id}` "
                f"score {group.score:.4f}: {duplicate_ids}"
            ),
            f"Duplicate evidence: {evidence}",
        ]

    def _similarity_hints(
        self,
        source_briefs: list[dict[str, Any]],
    ) -> dict[str, list[str]]:
        hints: dict[str, list[str]] = defaultdict(list)
        for brief in source_briefs:
            matches = find_similar_source_briefs(
                brief,
                source_briefs,
                threshold=SOURCE_SIMILARITY_THRESHOLD,
                limit=self.SIMILARITY_LIMIT,
            )
            if not matches:
                continue
            rendered = ", ".join(
                f"`{match.id}` {match.score:.4f} ({', '.join(match.matched_fields)})"
                for match in matches
            )
            hints[brief["id"]].append(f"Similar briefs: {rendered}")
        return hints

    def _excerpt(self, value: str) -> str:
        normalized = " ".join(value.split())
        if len(normalized) <= self.SUMMARY_EXCERPT_LENGTH:
            return normalized
        return f"{normalized[: self.SUMMARY_EXCERPT_LENGTH - 3]}..."

    def _compact_json(self, value: Any, *, max_length: int = 180) -> str:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        if len(rendered) <= max_length:
            return rendered
        return f"{rendered[: max_length - 3]}..."
