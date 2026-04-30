"""Detect execution plan tasks that appear to drift from brief scope."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


Severity = Literal["high", "medium"]
DriftType = Literal["non_goal_conflict", "potential_scope_drift"]

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
    "do",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "no",
    "not",
    "of",
    "on",
    "or",
    "that",
    "the",
    "then",
    "to",
    "with",
    "without",
}


@dataclass(frozen=True)
class PlanScopeDrift:
    """A deterministic scope-drift signal for one execution task."""

    task_id: str
    title: str
    drift_type: DriftType
    matched_phrase: str | None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    severity: Severity = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "drift_type": self.drift_type,
            "matched_phrase": self.matched_phrase,
            "evidence": list(self.evidence),
            "severity": self.severity,
        }


def detect_plan_scope_drift(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    plan: Mapping[str, Any] | ExecutionPlan,
) -> list[PlanScopeDrift]:
    """Return tasks that conflict with non-goals or lack declared scope evidence."""
    brief = _validated_brief_payload(implementation_brief)
    plan_payload = _validated_plan_payload(plan)
    non_goals = _string_list(brief.get("non_goals"))
    scope_evidence = _dedupe(
        [*_string_list(brief.get("scope")), *_string_list(brief.get("definition_of_done"))]
    )
    drifts: list[PlanScopeDrift] = []

    for index, task in enumerate(_list_of_task_payloads(plan_payload.get("tasks")), start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title")) or task_id
        task_evidence = _task_evidence(task)
        task_text = " ".join(task_evidence)

        non_goal_match = _first_matching_phrase(task_text, non_goals)
        if non_goal_match is not None:
            drifts.append(
                PlanScopeDrift(
                    task_id=task_id,
                    title=title,
                    drift_type="non_goal_conflict",
                    matched_phrase=non_goal_match,
                    evidence=_matching_task_evidence(task_evidence, non_goal_match),
                    severity="high",
                )
            )
            continue

        scope_match = _first_matching_phrase(task_text, scope_evidence)
        if scope_match is None:
            drifts.append(
                PlanScopeDrift(
                    task_id=task_id,
                    title=title,
                    drift_type="potential_scope_drift",
                    matched_phrase=None,
                    evidence=tuple(task_evidence[:3]),
                    severity="medium",
                )
            )

    return drifts


def summarize_plan_scope_drift(
    drifts: list[PlanScopeDrift] | tuple[PlanScopeDrift, ...],
) -> dict[str, int]:
    """Return drift counts by severity."""
    summary = {"high": 0, "medium": 0, "total": len(drifts)}
    for drift in drifts:
        summary[drift.severity] += 1
    return summary


def plan_scope_drift_to_dicts(
    drifts: list[PlanScopeDrift] | tuple[PlanScopeDrift, ...],
) -> list[dict[str, Any]]:
    """Serialize drift records to dictionaries."""
    return [drift.to_dict() for drift in drifts]


def _validated_brief_payload(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
) -> dict[str, Any]:
    if hasattr(implementation_brief, "model_dump"):
        return implementation_brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(implementation_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(implementation_brief)


def _validated_plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _list_of_task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for field_name in ("title", "description"):
        value = _text(task.get(field_name))
        if value:
            evidence.append(f"{field_name}: {value}")
    for item in _string_list(task.get("files_or_modules")):
        evidence.append(f"files_or_modules: {item}")
    for item in _string_list(task.get("acceptance_criteria")):
        evidence.append(f"acceptance_criteria: {item}")
    return evidence


def _first_matching_phrase(text: str, phrases: list[str]) -> str | None:
    for phrase in phrases:
        if _matches_phrase(text, phrase):
            return phrase
    return None


def _matches_phrase(text: str, phrase: str) -> bool:
    normalized_text = _normalized_phrase(text)
    normalized_phrase = _normalized_phrase(phrase)
    if not normalized_text or not normalized_phrase:
        return False
    if _phrase_boundary_search(normalized_text, normalized_phrase):
        return True

    phrase_tokens = _tokens(phrase)
    if not phrase_tokens:
        return False
    text_tokens = _tokens(text)
    overlap = phrase_tokens & text_tokens
    if len(phrase_tokens) <= 2:
        return len(overlap) == len(phrase_tokens)
    return len(overlap) >= min(3, len(phrase_tokens))


def _matching_task_evidence(task_evidence: list[str], phrase: str) -> tuple[str, ...]:
    matched = [item for item in task_evidence if _matches_phrase(item, phrase)]
    return tuple((matched or task_evidence)[:3])


def _phrase_boundary_search(text: str, phrase: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text) is not None


def _tokens(value: str) -> set[str]:
    return {
        _stem(token)
        for token in _TOKEN_RE.findall(value.lower())
        if token not in _STOPWORDS and len(token) > 1
    }


def _stem(token: str) -> str:
    for suffix in ("ing", "ed", "es", "s"):
        if len(token) > len(suffix) + 2 and token.endswith(suffix):
            return token[: -len(suffix)]
    return token


def _normalized_phrase(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    return ""


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


__all__ = [
    "PlanScopeDrift",
    "detect_plan_scope_drift",
    "plan_scope_drift_to_dicts",
    "summarize_plan_scope_drift",
]
