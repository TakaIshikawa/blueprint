"""Plan API conditional request readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ConditionalRequestSignal = Literal[
    "if_match_precondition",
    "if_unmodified_since_validation",
    "precondition_failed_responses",
    "conditional_mutations",
    "optimistic_locking",
    "lost_update_prevention",
    "if_range_requests",
    "conditional_idempotency",
]
ConditionalRequestSafeguard = Literal[
    "precondition_validation_tests",
    "412_response_tests",
    "optimistic_lock_implementation",
    "concurrent_modification_tests",
    "conditional_request_documentation",
]
ConditionalRequestReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ConditionalRequestReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[ConditionalRequestSignal, ...] = (
    "if_match_precondition",
    "if_unmodified_since_validation",
    "precondition_failed_responses",
    "conditional_mutations",
    "optimistic_locking",
    "lost_update_prevention",
    "if_range_requests",
    "conditional_idempotency",
)
_SAFEGUARD_ORDER: tuple[ConditionalRequestSafeguard, ...] = (
    "precondition_validation_tests",
    "412_response_tests",
    "optimistic_lock_implementation",
    "concurrent_modification_tests",
    "conditional_request_documentation",
)
_SIGNAL_PATTERNS: dict[ConditionalRequestSignal, re.Pattern[str]] = {
    "if_match_precondition": re.compile(
        r"\b(?:if-match|if match|etag precondition|require etag|conditional put|conditional post|conditional patch)\b",
        re.I,
    ),
    "if_unmodified_since_validation": re.compile(
        r"\b(?:if-unmodified-since|if unmodified since|last modified validation|timestamp precondition)\b",
        re.I,
    ),
    "precondition_failed_responses": re.compile(
        r"\b(?:412 precondition failed|412 response|precondition failed|etag mismatch|version mismatch)\b",
        re.I,
    ),
    "conditional_mutations": re.compile(
        r"\b(?:conditional update|conditional delete|conditional write|conditional modification|safe mutation)\b",
        re.I,
    ),
    "optimistic_locking": re.compile(
        r"\b(?:optimistic lock|optimistic concurrency|version check|etag validation|compare-and-swap)\b",
        re.I,
    ),
    "lost_update_prevention": re.compile(
        r"\b(?:lost update|prevent lost update|concurrent write|write conflict|overwrite protection)\b",
        re.I,
    ),
    "if_range_requests": re.compile(
        r"\b(?:if-range|if range|conditional range|partial request|range validation)\b",
        re.I,
    ),
    "conditional_idempotency": re.compile(
        r"\b(?:conditional idempotency|idempotent conditional|safe retry|conditional retry)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ConditionalRequestSignal, re.Pattern[str]] = {
    "if_match_precondition": re.compile(r"if[_-]?match|precondition|conditional", re.I),
    "if_unmodified_since_validation": re.compile(r"if[_-]?unmodified|validation|timestamp", re.I),
    "precondition_failed_responses": re.compile(r"412|precondition|failed", re.I),
    "conditional_mutations": re.compile(r"conditional|mutation|update", re.I),
    "optimistic_locking": re.compile(r"optimistic|lock|version", re.I),
    "lost_update_prevention": re.compile(r"lost[_-]?update|conflict|prevention", re.I),
    "if_range_requests": re.compile(r"if[_-]?range|partial|range", re.I),
    "conditional_idempotency": re.compile(r"idempotency|idempotent|retry", re.I),
}
_SAFEGUARD_PATTERNS: dict[ConditionalRequestSafeguard, re.Pattern[str]] = {
    "precondition_validation_tests": re.compile(
        r"\b(?:(?:precondition|if-match|if-unmodified).{0,80}(?:validat|test|verif|check).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:precondition|if-match|if-unmodified).{0,80}(?:validat|test|verif|check)|"
        r"precondition tests?|validation tests?)\b",
        re.I,
    ),
    "412_response_tests": re.compile(
        r"\b(?:(?:412|precondition failed).{0,80}(?:test|verif|check|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:412|precondition failed).{0,80}(?:test|verif|check)|"
        r"412 tests?|precondition failed tests?|mismatch tests?)\b",
        re.I,
    ),
    "optimistic_lock_implementation": re.compile(
        r"\b(?:optimistic lock.{0,80}(?:implement|code|logic|handler|function|method)|"
        r"(?:implement|code|logic|handler|function|method).{0,80}optimistic lock|"
        r"version check implementation|compare-and-swap implementation)\b",
        re.I,
    ),
    "concurrent_modification_tests": re.compile(
        r"\b(?:(?:concurrent|race|conflict|lost update).{0,80}(?:test|verif|check|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:concurrent|race|conflict|lost update)|"
        r"concurrent modification tests?|write conflict tests?|overwrite tests?)\b",
        re.I,
    ),
    "conditional_request_documentation": re.compile(
        r"\b(?:conditional request.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}conditional request|"
        r"if-match.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}if-match|"
        r"precondition.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?))\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:conditional request|if-match|precondition)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[ConditionalRequestSafeguard, str] = {
    "precondition_validation_tests": "Add tests verifying If-Match and If-Unmodified-Since precondition validation logic.",
    "412_response_tests": "Add tests for 412 Precondition Failed responses when ETag or timestamp mismatches occur.",
    "optimistic_lock_implementation": "Ensure optimistic locking is implemented correctly with version checking and compare-and-swap semantics.",
    "concurrent_modification_tests": "Add tests for concurrent modification scenarios, lost update prevention, and write conflict handling.",
    "conditional_request_documentation": "Document conditional request patterns, If-Match/If-Unmodified-Since usage, 412 response handling, and client retry strategies.",
}


@dataclass(frozen=True, slots=True)
class TaskApiConditionalRequestReadinessFinding:
    """API conditional request readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ConditionalRequestSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ConditionalRequestSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ConditionalRequestSafeguard, ...] = field(default_factory=tuple)
    readiness: ConditionalRequestReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskApiConditionalRequestReadinessPlan:
    """Plan-level API conditional request readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiConditionalRequestReadinessFinding, ...] = field(default_factory=tuple)
    conditional_request_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiConditionalRequestReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "conditional_request_task_ids": list(self.conditional_request_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }


def build_task_api_conditional_request_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiConditionalRequestReadinessPlan:
    """Build a conditional request readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    conditional_request_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in conditional_request_ids
    )
    return TaskApiConditionalRequestReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        conditional_request_task_ids=conditional_request_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, conditional_request_ids, not_applicable_ids),
    )


def derive_task_api_conditional_request_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiConditionalRequestReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_conditional_request_readiness_plan(plan)


def generate_task_api_conditional_request_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiConditionalRequestReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_conditional_request_readiness_plan(plan)


def extract_task_api_conditional_request_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiConditionalRequestReadinessFinding, ...]:
    """Return conditional request readiness findings for applicable tasks."""
    return build_task_api_conditional_request_readiness_plan(plan).findings


def summarize_task_api_conditional_request_readiness(
    result: TaskApiConditionalRequestReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for conditional request readiness review."""
    if isinstance(result, TaskApiConditionalRequestReadinessPlan):
        return dict(result.summary)
    return build_task_api_conditional_request_readiness_plan(result).summary


def _load_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(plan, str):
        return None, []
    if isinstance(plan, ExecutionPlan):
        payload = dict(plan.model_dump(mode="python"))
        return payload.get("id"), payload.get("tasks", [])
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return payload.get("id"), payload.get("tasks", [])
    if isinstance(plan, Mapping):
        try:
            payload = dict(ExecutionPlan.model_validate(plan).model_dump(mode="python"))
            return payload.get("id"), payload.get("tasks", [])
        except (TypeError, ValueError, ValidationError):
            return plan.get("id"), plan.get("tasks", [])
    return None, []


def _analyze_task(task: Mapping[str, Any]) -> TaskApiConditionalRequestReadinessFinding | None:
    task_id = task.get("id", "")
    title = task.get("title", "")

    if _NO_IMPACT_RE.search(_searchable_task_text(task)):
        return None

    detected_signals = _detect_signals(task)
    if not detected_signals:
        return None

    present_safeguards = _detect_safeguards(task)
    missing_safeguards = tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if safeguard not in present_safeguards
    )
    readiness = _compute_readiness(detected_signals, present_safeguards, missing_safeguards)
    evidence = _collect_evidence(task, detected_signals)
    actionable_remediations = tuple(
        _REMEDIATIONS[safeguard] for safeguard in missing_safeguards
    )

    return TaskApiConditionalRequestReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[ConditionalRequestSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    detected: list[ConditionalRequestSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[ConditionalRequestSafeguard, ...]:
    searchable = _searchable_task_text(task)
    detected: list[ConditionalRequestSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[ConditionalRequestSignal, ...],
    present: tuple[ConditionalRequestSafeguard, ...],
    missing: tuple[ConditionalRequestSafeguard, ...],
) -> ConditionalRequestReadiness:
    if not signals:
        return "weak"
    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[ConditionalRequestSignal, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    searchable = _searchable_task_text(task)

    for signal in signals[:3]:
        pattern = _SIGNAL_PATTERNS[signal]
        match = pattern.search(searchable)
        if match:
            start = max(0, match.start() - 40)
            end = min(len(searchable), match.end() + 40)
            snippet = _clean_text(searchable[start:end])
            if len(snippet) > 100:
                snippet = f"{snippet[:97]}..."
            evidence.append(f"{signal}: ...{snippet}...")

    return tuple(evidence)


def _summary(
    findings: tuple[TaskApiConditionalRequestReadinessFinding, ...],
    conditional_request_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "conditional_request_task_count": len(conditional_request_ids),
        "not_applicable_task_count": len(not_applicable_ids),
        "readiness_counts": {
            readiness: sum(1 for finding in findings if finding.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "overall_readiness": _overall_readiness(findings),
    }


def _overall_readiness(findings: tuple[TaskApiConditionalRequestReadinessFinding, ...]) -> ConditionalRequestReadiness:
    if not findings:
        return "weak"
    readiness_values = [_READINESS_ORDER[finding.readiness] for finding in findings]
    avg = sum(readiness_values) / len(readiness_values)
    if avg >= 1.5:
        return "strong"
    if avg >= 0.5:
        return "partial"
    return "weak"


def _searchable_task_text(task: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field in ("title", "body", "description", "prompt", "acceptance_criteria", "definition_of_done"):
        value = task.get(field)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
    return " ".join(parts)


def _task_paths(task: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for field in ("expected_files", "output_files", "related_files"):
        value = task.get(field)
        if isinstance(value, str):
            paths.append(value)
        elif isinstance(value, (list, tuple)):
            paths.extend(str(item) for item in value if item)
    return paths


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "ConditionalRequestReadiness",
    "ConditionalRequestSafeguard",
    "ConditionalRequestSignal",
    "TaskApiConditionalRequestReadinessFinding",
    "TaskApiConditionalRequestReadinessPlan",
    "build_task_api_conditional_request_readiness_plan",
    "derive_task_api_conditional_request_readiness_plan",
    "extract_task_api_conditional_request_readiness_findings",
    "generate_task_api_conditional_request_readiness_plan",
    "summarize_task_api_conditional_request_readiness",
]
