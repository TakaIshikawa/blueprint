"""Markdown risk register exporter for implementation briefs."""

from __future__ import annotations

import json
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
_MIN_TOKEN_COVERAGE = 0.6


class RiskRegisterExporter(TargetExporter):
    """Export brief risks and task mitigation links as a Markdown register."""

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
        """Export an implementation risk register to Markdown."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        content = self.render(plan, brief)
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def render(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        """Render one markdown register row per implementation brief risk."""
        risks = _list_of_strings(brief.get("risks"))
        lines = [
            f"# Risk Register: {brief['title']}",
            "",
            "## Plan Metadata",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief: `{brief['id']}`",
            f"- Target Engine: {plan.get('target_engine') or 'N/A'}",
            f"- Target Repository: {plan.get('target_repo') or 'N/A'}",
            f"- Total Risks: {len(risks)}",
            "",
            "## Register",
            "",
            (
                "| Risk ID | Source Risk | Affected Milestones/Tasks | Mitigation Evidence | "
                "Owner/Suggested Engine | Status |"
            ),
            "| --- | --- | --- | --- | --- | --- |",
        ]

        if not risks:
            lines.append("| none | No implementation risks listed | none | none | N/A | accepted |")
            return "\n".join(lines).rstrip() + "\n"

        for index, risk in enumerate(risks, 1):
            risk_id = risk_identifier(index)
            matching_tasks = [
                task for task in plan.get("tasks", []) if risk_matches_task(risk, risk_id, task)
            ]
            row = [
                f"`{risk_id}`",
                _escape_table_cell(risk),
                _escape_table_cell(_affected_tasks(matching_tasks)),
                _escape_table_cell(_mitigation_evidence(matching_tasks, brief)),
                _escape_table_cell(_owners_and_engines(matching_tasks, plan)),
                _escape_table_cell(_status(matching_tasks)),
            ]
            lines.append("| " + " | ".join(row) + " |")

        return "\n".join(lines).rstrip() + "\n"


def risk_identifier(index: int) -> str:
    """Return the stable register identifier for a one-based risk index."""
    return f"RISK-{index:03d}"


def risk_matches_task(risk: str, risk_id: str, task: dict[str, Any]) -> bool:
    """Return True when task content or metadata references a risk."""
    for text in _task_reference_texts(task):
        if _risk_matches_text(risk, text) or _risk_id_matches_text(risk_id, text):
            return True
    return False


def _affected_tasks(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        return "none"
    values = []
    for task in tasks:
        milestone = task.get("milestone") or "Ungrouped"
        values.append(f"{milestone}: `{task['id']}`")
    return ", ".join(values)


def _mitigation_evidence(tasks: list[dict[str, Any]], brief: dict[str, Any]) -> str:
    evidence: list[str] = []
    for task in tasks:
        evidence.extend(_list_of_strings(task.get("acceptance_criteria")))
        metadata = task.get("metadata") or {}
        if isinstance(metadata, dict):
            evidence.extend(_metadata_mitigation_texts(metadata.get("mitigation")))

    if not evidence:
        validation_plan = str(brief.get("validation_plan") or "").strip()
        if validation_plan:
            evidence.append(f"Validation plan: {validation_plan}")

    return "; ".join(dict.fromkeys(item for item in evidence if item.strip())) or "none"


def _owners_and_engines(tasks: list[dict[str, Any]], plan: dict[str, Any]) -> str:
    if not tasks:
        return plan.get("target_engine") or "Unassigned"

    owners: list[str] = []
    for task in tasks:
        owner = task.get("owner_type") or "unassigned"
        engine = task.get("suggested_engine") or plan.get("target_engine") or "unassigned"
        owners.append(f"{owner}: {engine}")
    return ", ".join(dict.fromkeys(owners))


def _status(tasks: list[dict[str, Any]]) -> str:
    if not tasks:
        return "uncovered"
    statuses = {task.get("status") or "pending" for task in tasks}
    if statuses == {"completed"}:
        return "mitigated"
    if "blocked" in statuses:
        return "blocked"
    return "tracked"


def _task_reference_texts(task: dict[str, Any]) -> list[str]:
    texts = [
        str(task.get("title") or ""),
        str(task.get("description") or ""),
    ]
    texts.extend(_list_of_strings(task.get("acceptance_criteria")))
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


def _metadata_mitigation_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, dict):
        return [json.dumps(value, sort_keys=True)]
    return [str(value)]


def _risk_matches_text(risk: str, text: str) -> bool:
    risk_phrase = _normalized_phrase(risk)
    text_phrase = _normalized_phrase(text)
    if risk_phrase and risk_phrase in text_phrase:
        return True

    risk_tokens = _normalized_tokens(risk)
    if not risk_tokens:
        return False

    text_tokens = _normalized_tokens(text)
    if not text_tokens:
        return False
    return len(risk_tokens & text_tokens) / len(risk_tokens) >= _MIN_TOKEN_COVERAGE


def _risk_id_matches_text(risk_id: str, text: str) -> bool:
    normalized_risk_id = risk_id.lower()
    compact_risk_id = normalized_risk_id.replace("-", "")
    normalized_text = text.lower()
    compact_text = re.sub(r"[^a-z0-9]+", "", normalized_text)
    return normalized_risk_id in normalized_text or compact_risk_id in compact_text


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


def _escape_table_cell(value: Any) -> str:
    """Escape Markdown table delimiters inside a cell."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
