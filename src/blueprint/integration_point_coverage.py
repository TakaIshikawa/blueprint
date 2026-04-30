"""Map implementation brief integration points to execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True, slots=True)
class IntegrationPointCoverage:
    """Coverage status for one implementation brief integration point."""

    integration_point: str
    covered: bool
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_reason: str | None = None
    suggested_task_title: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "integration_point": self.integration_point,
            "covered": self.covered,
            "task_ids": list(self.task_ids),
            "evidence": list(self.evidence),
            "missing_reason": self.missing_reason,
            "suggested_task_title": self.suggested_task_title,
        }


def analyze_integration_point_coverage(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[IntegrationPointCoverage, ...]:
    """Return task coverage for each implementation brief integration point."""
    brief_payload = _brief_payload(implementation_brief)
    plan_payload = _plan_payload(execution_plan)
    task_contexts = [
        _task_context(task, index)
        for index, task in enumerate(_task_payloads(plan_payload.get("tasks")), start=1)
    ]

    return tuple(
        _coverage_record(integration_point, task_contexts)
        for integration_point in _strings(brief_payload.get("integration_points"))
    )


def integration_point_coverage_to_dict(
    coverage: tuple[IntegrationPointCoverage, ...] | list[IntegrationPointCoverage],
) -> list[dict[str, Any]]:
    """Serialize integration point coverage records to dictionaries."""
    return [record.to_dict() for record in coverage]


integration_point_coverage_to_dict.__test__ = False


def _coverage_record(
    integration_point: str,
    task_contexts: list[dict[str, Any]],
) -> IntegrationPointCoverage:
    matching_tasks: list[dict[str, Any]] = []
    evidence: list[str] = []

    for task_context in task_contexts:
        task_evidence = _task_evidence(integration_point, task_context)
        if not task_evidence:
            continue
        matching_tasks.append(task_context)
        evidence.extend(f"{task_context['task_id']} {item}" for item in task_evidence)

    if matching_tasks:
        return IntegrationPointCoverage(
            integration_point=integration_point,
            covered=True,
            task_ids=tuple(task["task_id"] for task in matching_tasks),
            evidence=tuple(_dedupe(evidence)),
        )

    return IntegrationPointCoverage(
        integration_point=integration_point,
        covered=False,
        missing_reason="No execution task mentions this integration point.",
        suggested_task_title=f"Cover {integration_point} integration point",
    )


def _task_context(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    fields = _task_text_fields(task)
    aggregate_text = " ".join(text for _, text in fields)
    return {
        "task_id": _task_id(task, index),
        "fields": fields,
        "normalized": _normalized(aggregate_text),
        "tokens": set(_token_keys(aggregate_text)),
    }


def _task_evidence(
    integration_point: str,
    task_context: Mapping[str, Any],
) -> list[str]:
    point_normalized = _normalized(integration_point)
    point_tokens = _significant_token_keys(integration_point)
    if not point_normalized or not _matches_task(point_normalized, point_tokens, task_context):
        return []

    evidence: list[str] = []
    for field_name, text in task_context["fields"]:
        field_normalized = _normalized(text)
        field_tokens = set(_token_keys(text))
        if point_normalized in field_normalized or (
            point_tokens and bool(point_tokens & field_tokens)
        ):
            evidence.append(f"{field_name}: {_snippet(text)}")
        if len(evidence) == 3:
            break

    if not evidence:
        evidence.append("task context: matched integration point tokens")
    return evidence


def _matches_task(
    point_normalized: str,
    point_tokens: set[str],
    task_context: Mapping[str, Any],
) -> bool:
    task_normalized = task_context["normalized"]
    if point_normalized in task_normalized:
        return True
    task_tokens = task_context["tokens"]
    if not point_tokens:
        return False
    if point_tokens <= task_tokens:
        return True

    overlap_count = len(point_tokens & task_tokens)
    return len(point_tokens) >= 3 and overlap_count >= len(point_tokens) - 1


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))

    for path in _strings(task.get("files_or_modules")):
        fields.append(("files_or_modules", path))

    for criterion in _strings(task.get("acceptance_criteria")):
        fields.append(("acceptance_criteria", criterion))

    for key, value in _metadata_texts(task.get("metadata")):
        fields.append((f"metadata.{key}", value))

    if test_command := _optional_text(task.get("test_command")):
        fields.append(("test_command", test_command))

    return fields


def _metadata_texts(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _optional_text(key)
            if not key_text:
                continue
            path = f"{prefix}.{key_text}" if prefix else key_text
            texts.extend(_metadata_texts(value[key], path))
        return texts

    if isinstance(value, (list, tuple, set)):
        texts = []
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items, start=1):
            path = f"{prefix}.{index}" if prefix else str(index)
            texts.extend(_metadata_texts(item, path))
        return texts

    text = _optional_text(value)
    return [(prefix or "value", text)] if text else []


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _normalized(value: str) -> str:
    return " ".join(_token_keys(value))


def _significant_token_keys(value: str) -> set[str]:
    return {
        token
        for token in _token_keys(value)
        if token not in _STOP_WORDS
    }


def _token_keys(value: str) -> list[str]:
    return [_token_key(token) for token in _TOKEN_RE.findall(value.lower())]


def _token_key(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return f"{token[:-3]}y"
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


def _snippet(value: str) -> str:
    return value if len(value) <= 120 else f"{value[:117].rstrip()}..."


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "IntegrationPointCoverage",
    "analyze_integration_point_coverage",
    "integration_point_coverage_to_dict",
]
