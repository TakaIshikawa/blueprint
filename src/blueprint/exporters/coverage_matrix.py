"""Markdown coverage matrix exporter for implementation briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint.exporters.base import TargetExporter


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "can",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "may",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_COVERED_THRESHOLD = 0.6
_PARTIAL_THRESHOLD = 0.34


class CoverageMatrixExporter(TargetExporter):
    """Export brief-to-task traceability as deterministic Markdown tables."""

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
        """Export an execution plan coverage matrix to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render coverage grouped by brief section."""
        lines = [
            f"# Coverage Matrix: {brief['title']}",
            "",
            "## Plan Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Total Tasks: {len(plan.get('tasks', []))}",
            "",
        ]

        sections = [
            ("Scope Coverage", _list_of_strings(brief.get("scope"))),
            ("Risk Coverage", _list_of_strings(brief.get("risks"))),
            ("Validation Coverage", [str(brief.get("validation_plan") or "").strip()]),
            (
                "Definition of Done Coverage",
                _list_of_strings(brief.get("definition_of_done")),
            ),
        ]
        for heading, items in sections:
            lines.extend(self._section_lines(heading, items, plan.get("tasks", [])))
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _section_lines(
        self,
        heading: str,
        items: list[str],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        lines = [
            f"## {heading}",
            "",
            "| Item | Status | Matching Tasks |",
            "| --- | --- | --- |",
        ]
        if not items:
            lines.append("| None | uncovered | none |")
            return lines

        for item in items:
            status, matching_task_ids = self._coverage_for_item(item, tasks)
            lines.append(
                "| "
                f"{_escape_table_cell(item)} | "
                f"{status} | "
                f"{self._matching_task_cell(matching_task_ids)} |"
            )
        return lines

    def _coverage_for_item(
        self,
        item: str,
        tasks: list[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        covered = False
        matching_task_ids: list[str] = []

        for task in tasks:
            score = _task_match_score(item, task)
            if score < _PARTIAL_THRESHOLD:
                continue
            matching_task_ids.append(str(task["id"]))
            if score >= _COVERED_THRESHOLD:
                covered = True

        if covered:
            return "covered", matching_task_ids
        if matching_task_ids:
            return "partial", matching_task_ids
        return "uncovered", []

    def _matching_task_cell(self, task_ids: list[str]) -> str:
        if not task_ids:
            return "none"
        return ", ".join(f"`{_escape_table_cell(task_id)}`" for task_id in task_ids)


def _task_match_score(item: str, task: dict[str, Any]) -> float:
    scores = [_text_match_score(item, text) for text in _task_texts(task)]
    return max(scores, default=0.0)


def _text_match_score(item: str, text: str) -> float:
    item_phrase = _normalized_phrase(item)
    text_phrase = _normalized_phrase(text)
    if item_phrase and item_phrase in text_phrase:
        return 1.0

    item_tokens = _normalized_tokens(item)
    if not item_tokens:
        return 0.0

    text_tokens = _normalized_tokens(text)
    if not text_tokens:
        return 0.0
    return len(item_tokens & text_tokens) / len(item_tokens)


def _task_texts(task: dict[str, Any]) -> list[str]:
    texts = [
        str(task.get("title") or ""),
        str(task.get("description") or ""),
    ]
    texts.extend(_list_of_strings(task.get("acceptance_criteria")))
    texts.extend(_list_of_strings(task.get("files_or_modules")))
    texts.extend(_metadata_texts(task.get("metadata")))
    return [text for text in texts if text.strip()]


def _metadata_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        texts: list[str] = []
        for key in sorted(value):
            texts.append(str(key))
            texts.extend(_metadata_texts(value[key]))
        return texts
    if isinstance(value, list):
        texts = []
        for item in value:
            texts.extend(_metadata_texts(item))
        return texts
    return [str(value)]


def _normalized_phrase(value: str) -> str:
    return " ".join(_normalized_token_list(value))


def _normalized_tokens(value: str) -> set[str]:
    return set(_normalized_token_list(value))


def _normalized_token_list(value: str) -> list[str]:
    tokens: list[str] = []
    for raw_token in _TOKEN_RE.findall(value.lower()):
        token = _normalize_token(raw_token)
        if token and token not in _STOPWORDS:
            tokens.append(token)
    return tokens


def _normalize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def _list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _escape_table_cell(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
