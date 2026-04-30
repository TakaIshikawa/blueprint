"""Map implementation assumptions to execution plan validation coverage."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


AssumptionCoverageStatus = Literal["verified", "partially_covered", "unverified"]
EvidenceStrength = Literal["strong", "supporting"]

_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOP_WORDS = {
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
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_MIN_TOKEN_COVERAGE = 0.6


@dataclass(frozen=True, slots=True)
class AssumptionTaskEvidence:
    """Task evidence that mentions or validates an assumption."""

    task_id: str
    strength: EvidenceStrength
    fields: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "strength": self.strength,
            "fields": list(self.fields),
        }


@dataclass(frozen=True, slots=True)
class AssumptionCoverageEntry:
    """Coverage status for one implementation assumption."""

    assumption_id: str
    assumption: str
    status: AssumptionCoverageStatus
    matched_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[AssumptionTaskEvidence, ...] = field(default_factory=tuple)
    suggested_acceptance_criterion: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "assumption_id": self.assumption_id,
            "assumption": self.assumption,
            "status": self.status,
            "matched_task_ids": list(self.matched_task_ids),
            "evidence": [item.to_dict() for item in self.evidence],
            "suggested_acceptance_criterion": self.suggested_acceptance_criterion,
        }


@dataclass(frozen=True, slots=True)
class AssumptionCoverageReport:
    """Assumption coverage report for an implementation brief and execution plan."""

    brief_id: str | None
    plan_id: str | None
    assumptions: tuple[AssumptionCoverageEntry, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "assumptions": [item.to_dict() for item in self.assumptions],
            "summary": dict(self.summary),
        }


def analyze_assumption_coverage(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> AssumptionCoverageReport:
    """Map each implementation assumption to execution tasks that cover it."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))

    entries = tuple(
        _coverage_entry(assumption, index, tasks)
        for index, assumption in enumerate(_strings(brief.get("assumptions")), start=1)
    )
    return AssumptionCoverageReport(
        brief_id=_optional_text(brief.get("id")),
        plan_id=_optional_text(plan.get("id")),
        assumptions=entries,
        summary=_summary(entries),
    )


def assumption_coverage_report_to_dict(
    report: AssumptionCoverageReport,
) -> dict[str, Any]:
    """Serialize an assumption coverage report to a dictionary."""
    return report.to_dict()


assumption_coverage_report_to_dict.__test__ = False


def _coverage_entry(
    assumption: str,
    index: int,
    tasks: list[dict[str, Any]],
) -> AssumptionCoverageEntry:
    evidence = tuple(
        item
        for task_index, task in enumerate(tasks, start=1)
        if (item := _task_evidence(assumption, task, task_index)) is not None
    )
    matched_task_ids = tuple(item.task_id for item in evidence)

    if any(item.strength == "strong" for item in evidence):
        status: AssumptionCoverageStatus = "verified"
        suggestion = None
    elif evidence:
        status = "partially_covered"
        suggestion = None
    else:
        status = "unverified"
        suggestion = _suggested_acceptance_criterion(assumption)

    return AssumptionCoverageEntry(
        assumption_id=f"assumption-{index}",
        assumption=assumption,
        status=status,
        matched_task_ids=matched_task_ids,
        evidence=evidence,
        suggested_acceptance_criterion=suggestion,
    )


def _task_evidence(
    assumption: str,
    task: Mapping[str, Any],
    index: int,
) -> AssumptionTaskEvidence | None:
    strong_fields: list[str] = []
    supporting_fields: list[str] = []

    if any(_matches_assumption(assumption, text) for text in _strings(task.get("acceptance_criteria"))):
        strong_fields.append("acceptance_criteria")

    test_command = _optional_text(task.get("test_command"))
    if test_command and _matches_assumption(assumption, test_command):
        strong_fields.append("test_command")

    for field_name in ("title", "description"):
        text = _optional_text(task.get(field_name))
        if text and _matches_assumption(assumption, text):
            supporting_fields.append(field_name)

    if any(_matches_assumption(assumption, text) for text in _strings(task.get("files_or_modules"))):
        supporting_fields.append("files_or_modules")

    if any(_matches_assumption(assumption, text) for text in _metadata_texts(task.get("metadata"))):
        supporting_fields.append("metadata")

    fields = tuple(_dedupe([*strong_fields, *supporting_fields]))
    if not fields:
        return None
    return AssumptionTaskEvidence(
        task_id=_task_id(task, index),
        strength="strong" if strong_fields else "supporting",
        fields=fields,
    )


def _summary(entries: tuple[AssumptionCoverageEntry, ...]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for entry in entries:
        status_counts[entry.status] = status_counts.get(entry.status, 0) + 1
    return {
        "assumption_count": len(entries),
        "status_counts": dict(sorted(status_counts.items())),
        "unverified_count": status_counts.get("unverified", 0),
    }


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _optional_text(item))]
    return []


def _metadata_texts(value: Any) -> list[str]:
    texts: list[str] = []
    if not isinstance(value, Mapping):
        return texts

    for item in value.values():
        texts.extend(_nested_texts(item))
    return texts


def _nested_texts(value: Any) -> list[str]:
    text = _optional_text(value)
    if text:
        return [text]
    if isinstance(value, Mapping):
        texts: list[str] = []
        for item in value.values():
            texts.extend(_nested_texts(item))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts = []
        for item in value:
            texts.extend(_nested_texts(item))
        return texts
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _matches_assumption(assumption: str, text: str) -> bool:
    assumption_phrase = _normalized_phrase(assumption)
    text_phrase = _normalized_phrase(text)
    if not assumption_phrase or not text_phrase:
        return False
    if assumption_phrase in text_phrase:
        return True

    assumption_tokens = set(_normalized_tokens(assumption))
    text_tokens = set(_normalized_tokens(text))
    if not assumption_tokens or not text_tokens:
        return False

    return len(assumption_tokens & text_tokens) / len(assumption_tokens) >= _MIN_TOKEN_COVERAGE


def _normalized_phrase(value: str) -> str:
    return " ".join(_normalized_tokens(value))


def _normalized_tokens(value: str) -> list[str]:
    value = _CAMEL_BOUNDARY_RE.sub(" ", value)
    tokens: list[str] = []
    for raw_token in _TOKEN_RE.findall(value.lower()):
        token = _normalize_token(raw_token)
        if token and token not in _STOP_WORDS:
            tokens.append(token)
    return tokens


def _normalize_token(token: str) -> str:
    if token.endswith("able") and len(token) > 5:
        return token[:-4]
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def _suggested_acceptance_criterion(assumption: str) -> str:
    return f"Acceptance criteria verifies this assumption: {assumption}"


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


__all__ = [
    "AssumptionCoverageEntry",
    "AssumptionCoverageReport",
    "AssumptionCoverageStatus",
    "AssumptionTaskEvidence",
    "analyze_assumption_coverage",
    "assumption_coverage_report_to_dict",
]
