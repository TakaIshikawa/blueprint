"""Track implementation-brief assumptions against execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


AssumptionCoverageStatus = Literal[
    "validated",
    "unvalidated",
    "unowned",
    "contradicted",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_VALIDATION_RE = re.compile(
    r"\b(?:acceptance|assert|check|cover|covered|e2e|integration|prove|pytest|"
    r"regression|smoke|test|tested|validate|validates|validation|verify|verifies)\b",
    re.IGNORECASE,
)
_CONTRADICTION_RE = re.compile(
    r"\b(?:block|blocked|contradict|disable|exclude|ignore|not supported|out of scope|"
    r"remove|skip|without|won't|will not|do not|don't|dont|must not|should not|never)\b",
    re.IGNORECASE,
)
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "their",
    "this",
    "to",
    "with",
}
_BOILERPLATE_TERMS = {
    "assume",
    "assumes",
    "assumption",
    "available",
    "build",
    "create",
    "existing",
    "feature",
    "implement",
    "new",
    "plan",
    "support",
    "task",
    "update",
    "use",
    "user",
    "work",
}
_STATUS_ORDER: dict[AssumptionCoverageStatus, int] = {
    "contradicted": 0,
    "unowned": 1,
    "unvalidated": 2,
    "validated": 3,
}


@dataclass(frozen=True, slots=True)
class PlanAssumptionCoverageItem:
    """Coverage details for one brief assumption."""

    assumption_id: str
    assumption: str
    matched_task_ids: tuple[str, ...] = field(default_factory=tuple)
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)
    status: AssumptionCoverageStatus = "unowned"
    follow_up_task: str = ""
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "assumption_id": self.assumption_id,
            "assumption": self.assumption,
            "matched_task_ids": list(self.matched_task_ids),
            "validation_evidence": list(self.validation_evidence),
            "status": self.status,
            "follow_up_task": self.follow_up_task,
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class PlanAssumptionTrackerReport:
    """Plan-level assumption coverage report."""

    brief_id: str | None = None
    plan_id: str | None = None
    coverage: tuple[PlanAssumptionCoverageItem, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "coverage": [item.to_dict() for item in self.coverage],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return coverage records as plain dictionaries."""
        return [item.to_dict() for item in self.coverage]

    def to_markdown(self) -> str:
        """Render assumption coverage as deterministic Markdown."""
        title = "# Plan Assumption Tracker"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.coverage:
            lines.extend(["", "No brief assumptions were provided."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Assumption | Status | Tasks | Validation Evidence | Follow-up |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for item in self.coverage:
            lines.append(
                "| "
                f"{_markdown_cell(item.assumption)} | "
                f"{item.status} | "
                f"{_markdown_cell(', '.join(item.matched_task_ids) or 'None')} | "
                f"{_markdown_cell('; '.join(item.validation_evidence) or 'None')} | "
                f"{_markdown_cell(item.follow_up_task or 'None')} |"
            )
        return "\n".join(lines)


def build_plan_assumption_tracker(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> PlanAssumptionTrackerReport:
    """Map brief assumptions to owning tasks and validation evidence."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    assumptions = _strings(brief.get("assumptions"))
    tasks = _task_payloads(plan.get("tasks"))
    coverage = [
        _coverage_item(assumption, index, tasks)
        for index, assumption in enumerate(assumptions, start=1)
    ]
    coverage.sort(key=lambda item: (_STATUS_ORDER[item.status], item.assumption_id))
    result = tuple(coverage)

    return PlanAssumptionTrackerReport(
        brief_id=_optional_text(brief.get("id")),
        plan_id=_optional_text(plan.get("id")),
        coverage=result,
        summary={
            "assumption_count": len(assumptions),
            "validated_count": sum(1 for item in result if item.status == "validated"),
            "unvalidated_count": sum(1 for item in result if item.status == "unvalidated"),
            "unowned_count": sum(1 for item in result if item.status == "unowned"),
            "contradicted_count": sum(1 for item in result if item.status == "contradicted"),
            "follow_up_count": sum(1 for item in result if item.follow_up_task),
        },
    )


def plan_assumption_tracker_to_dict(
    report: PlanAssumptionTrackerReport,
) -> dict[str, Any]:
    """Serialize an assumption tracker report to a plain dictionary."""
    return report.to_dict()


plan_assumption_tracker_to_dict.__test__ = False


def plan_assumption_tracker_to_markdown(report: PlanAssumptionTrackerReport) -> str:
    """Render an assumption tracker report as Markdown."""
    return report.to_markdown()


plan_assumption_tracker_to_markdown.__test__ = False


def _coverage_item(
    assumption: str,
    index: int,
    tasks: list[dict[str, Any]],
) -> PlanAssumptionCoverageItem:
    assumption_id = f"assumption-{index}"
    tokens = _significant_tokens(assumption)
    matches = [_task_match(tokens, task, task_index) for task_index, task in enumerate(tasks, start=1)]
    matches = [match for match in matches if match is not None]
    matched_task_ids = tuple(_dedupe(match[0] for match in matches))
    matched_terms = tuple(_dedupe(term for match in matches for term in match[1]))
    evidence = tuple(_dedupe(item for match in matches for item in match[2]))
    has_contradiction = any(match[3] for match in matches)
    status = _coverage_status(matched_task_ids, evidence, has_contradiction)

    return PlanAssumptionCoverageItem(
        assumption_id=assumption_id,
        assumption=assumption,
        matched_task_ids=matched_task_ids,
        validation_evidence=evidence,
        status=status,
        follow_up_task=_follow_up_task(assumption, status, matched_task_ids),
        matched_terms=matched_terms,
    )


def _task_match(
    assumption_tokens: set[str],
    task: Mapping[str, Any],
    index: int,
) -> tuple[str, tuple[str, ...], tuple[str, ...], bool] | None:
    if not assumption_tokens:
        return None

    task_id = _task_id(task, index)
    overlaps: set[str] = set()
    validation_evidence: list[str] = []
    contradiction = False

    for field_name, text in _task_text_fields(task):
        field_tokens = _significant_tokens(text)
        overlap = assumption_tokens & field_tokens
        if not _is_meaningful_overlap(assumption_tokens, overlap):
            continue
        overlaps.update(overlap)
        if _is_validation_field(field_name) and _VALIDATION_RE.search(text):
            validation_evidence.append(f"{field_name}: {text}")
        if _is_validation_field(field_name) and _CONTRADICTION_RE.search(text):
            contradiction = True
            validation_evidence.append(f"{field_name}: {text}")

    if not _is_meaningful_overlap(assumption_tokens, overlaps):
        return None
    return task_id, tuple(sorted(overlaps)), tuple(_dedupe(validation_evidence)), contradiction


def _coverage_status(
    matched_task_ids: tuple[str, ...],
    validation_evidence: tuple[str, ...],
    has_contradiction: bool,
) -> AssumptionCoverageStatus:
    if has_contradiction:
        return "contradicted"
    if not matched_task_ids:
        return "unowned"
    if not validation_evidence:
        return "unvalidated"
    return "validated"


def _follow_up_task(
    assumption: str,
    status: AssumptionCoverageStatus,
    matched_task_ids: tuple[str, ...],
) -> str:
    snippet = _snippet(assumption, 100).rstrip(".")
    if status == "validated":
        return ""
    if status == "unowned":
        return f"Add a task to own and validate assumption: {snippet}."
    if status == "unvalidated":
        return (
            f"Add acceptance criteria or a test command to tasks {', '.join(matched_task_ids)} "
            f"that validates assumption: {snippet}."
        )
    return (
        f"Resolve contradictory acceptance criteria in tasks {', '.join(matched_task_ids)} "
        f"or update the brief assumption: {snippet}."
    )


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(brief) if isinstance(brief, Mapping) else {}


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description", "milestone", "risk_level"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        fields.append((f"acceptance_criteria[{index}]", text))
    if text := _optional_text(task.get("test_command")):
        fields.append(("test_command", text))
    for index, text in enumerate(_strings(task.get("files_or_modules"))):
        fields.append((f"files_or_modules[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        fields.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(_metadata_tags(task.get("metadata")))):
        fields.append((f"metadata.tags[{index}]", text))
    return fields


def _metadata_tags(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return None
    return value.get("tags") or value.get("labels")


def _is_validation_field(field_name: str) -> bool:
    return field_name.startswith("acceptance_criteria") or field_name == "test_command"


def _is_meaningful_overlap(assumption_tokens: set[str], overlap: set[str]) -> bool:
    if not overlap:
        return False
    if len(assumption_tokens) == 1:
        token = next(iter(assumption_tokens))
        return token in overlap and len(token) >= 6
    if len(assumption_tokens) <= 3:
        return len(overlap) >= 2
    return len(overlap) >= 2 and len(overlap) / len(assumption_tokens) >= 0.4


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _significant_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(value)
        if token not in _STOP_WORDS
        and token not in _BOILERPLATE_TERMS
        and not token.isdigit()
        and len(token) > 1
    }


def _tokens(value: str) -> list[str]:
    return [_stem(token) for token in _TOKEN_RE.findall(value.casefold())]


def _stem(token: str) -> str:
    for suffix in ("ing", "ies", "ied", "ed", "es", "s", "ers"):
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            if suffix in {"ies", "ied"}:
                return f"{token[:-3]}y"
            return token[: -len(suffix)]
    return token


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _snippet(value: str, limit: int = 140) -> str:
    text = _text(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "AssumptionCoverageStatus",
    "PlanAssumptionCoverageItem",
    "PlanAssumptionTrackerReport",
    "build_plan_assumption_tracker",
    "plan_assumption_tracker_to_dict",
    "plan_assumption_tracker_to_markdown",
]
