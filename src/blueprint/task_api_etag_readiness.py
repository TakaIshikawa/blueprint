"""Plan API ETag readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ETagSignal = Literal[
    "etag_generation",
    "etag_header_inclusion",
    "if_none_match_validation",
    "not_modified_responses",
    "last_modified_headers",
    "cache_key_computation",
    "entity_versioning",
    "concurrent_update_detection",
]
ETagSafeguard = Literal[
    "etag_generation_tests",
    "cache_validation_tests",
    "weak_vs_strong_etag_handling",
    "concurrent_update_tests",
    "etag_documentation",
]
ETagReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ETagReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[ETagSignal, ...] = (
    "etag_generation",
    "etag_header_inclusion",
    "if_none_match_validation",
    "not_modified_responses",
    "last_modified_headers",
    "cache_key_computation",
    "entity_versioning",
    "concurrent_update_detection",
)
_SAFEGUARD_ORDER: tuple[ETagSafeguard, ...] = (
    "etag_generation_tests",
    "cache_validation_tests",
    "weak_vs_strong_etag_handling",
    "concurrent_update_tests",
    "etag_documentation",
)
_SIGNAL_PATTERNS: dict[ETagSignal, re.Pattern[str]] = {
    "etag_generation": re.compile(
        r"\b(?:etag generation|e-tag generation|generate etag|compute etag|strong etag|weak etag)\b",
        re.I,
    ),
    "etag_header_inclusion": re.compile(
        r"\b(?:etag header|e-tag header|include etag|return etag|etag response)\b",
        re.I,
    ),
    "if_none_match_validation": re.compile(
        r"\b(?:if-none-match|if none match|etag validation|validate etag|conditional get)\b",
        re.I,
    ),
    "not_modified_responses": re.compile(
        r"\b(?:304 not modified|304 response|not modified|cache hit|unchanged resource)\b",
        re.I,
    ),
    "last_modified_headers": re.compile(
        r"\b(?:last-modified|last modified header|modification time|timestamp header)\b",
        re.I,
    ),
    "cache_key_computation": re.compile(
        r"\b(?:cache key|caching key|hash algorithm|md5|sha|checksum|content hash)\b",
        re.I,
    ),
    "entity_versioning": re.compile(
        r"\b(?:entity version|resource version|version number|versioning|revision)\b",
        re.I,
    ),
    "concurrent_update_detection": re.compile(
        r"\b(?:concurrent update|concurrent modification|race condition|lost update|optimistic lock)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ETagSignal, re.Pattern[str]] = {
    "etag_generation": re.compile(r"etag|e-?tag|hash|cache", re.I),
    "etag_header_inclusion": re.compile(r"etag|e-?tag|header|response", re.I),
    "if_none_match_validation": re.compile(r"validation|validate|conditional", re.I),
    "not_modified_responses": re.compile(r"304|cache|not[_-]?modified", re.I),
    "last_modified_headers": re.compile(r"last[_-]?modified|timestamp", re.I),
    "cache_key_computation": re.compile(r"cache|hash|key", re.I),
    "entity_versioning": re.compile(r"version|revision", re.I),
    "concurrent_update_detection": re.compile(r"concurrent|conflict|lock", re.I),
}
_SAFEGUARD_PATTERNS: dict[ETagSafeguard, re.Pattern[str]] = {
    "etag_generation_tests": re.compile(
        r"\b(?:(?:etag|e-tag).{0,80}(?:generat|build|comput).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:etag|e-tag).{0,80}(?:generat|build|comput)|"
        r"etag tests?|hash tests?)\b",
        re.I,
    ),
    "cache_validation_tests": re.compile(
        r"\b(?:(?:cache|304|not modified).{0,80}(?:validat|test|verif|check).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache|304|not modified).{0,80}(?:validat|test|verif|check)|"
        r"if-none-match tests?|validation tests?)\b",
        re.I,
    ),
    "weak_vs_strong_etag_handling": re.compile(
        r"\b(?:weak etag|strong etag|w/|etag type|etag strength|weak vs strong|"
        r"etag comparison|semantic equivalence|exact match)\b",
        re.I,
    ),
    "concurrent_update_tests": re.compile(
        r"\b(?:(?:concurrent|race|conflict).{0,80}(?:test|verif|check|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:concurrent|race|conflict)|"
        r"optimistic lock tests?|lost update tests?|version conflict tests?)\b",
        re.I,
    ),
    "etag_documentation": re.compile(
        r"\b(?:etag.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}etag|"
        r"cache.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}cache)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:etag|e-tag|cache)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[ETagSafeguard, str] = {
    "etag_generation_tests": "Add tests verifying ETag generation logic, hash computation, and strong vs weak ETag formatting.",
    "cache_validation_tests": "Add tests for If-None-Match validation, 304 Not Modified responses, and cache hit scenarios.",
    "weak_vs_strong_etag_handling": "Ensure weak and strong ETag comparison semantics are implemented correctly per RFC 7232.",
    "concurrent_update_tests": "Add tests for concurrent update detection, optimistic locking, and version conflict handling.",
    "etag_documentation": "Document ETag generation strategy, cache validation logic, weak vs strong semantics, and client usage examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiETagReadinessFinding:
    """API ETag readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ETagSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ETagSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ETagSafeguard, ...] = field(default_factory=tuple)
    readiness: ETagReadiness = "partial"
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
class TaskApiETagReadinessPlan:
    """Plan-level API ETag readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiETagReadinessFinding, ...] = field(default_factory=tuple)
    etag_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiETagReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "etag_task_ids": list(self.etag_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }


def build_task_api_etag_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiETagReadinessPlan:
    """Build an ETag readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    etag_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in etag_ids
    )
    return TaskApiETagReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        etag_task_ids=etag_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, etag_ids, not_applicable_ids),
    )


def derive_task_api_etag_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiETagReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_etag_readiness_plan(plan)


def generate_task_api_etag_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiETagReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_etag_readiness_plan(plan)


def extract_task_api_etag_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiETagReadinessFinding, ...]:
    """Return ETag readiness findings for applicable tasks."""
    return build_task_api_etag_readiness_plan(plan).findings


def summarize_task_api_etag_readiness(
    result: TaskApiETagReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for ETag readiness review."""
    if isinstance(result, TaskApiETagReadinessPlan):
        return dict(result.summary)
    return build_task_api_etag_readiness_plan(result).summary


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


def _analyze_task(task: Mapping[str, Any]) -> TaskApiETagReadinessFinding | None:
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

    return TaskApiETagReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[ETagSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    detected: list[ETagSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[ETagSafeguard, ...]:
    searchable = _searchable_task_text(task)
    detected: list[ETagSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[ETagSignal, ...],
    present: tuple[ETagSafeguard, ...],
    missing: tuple[ETagSafeguard, ...],
) -> ETagReadiness:
    if not signals:
        return "weak"
    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[ETagSignal, ...],
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
    findings: tuple[TaskApiETagReadinessFinding, ...],
    etag_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "etag_task_count": len(etag_ids),
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


def _overall_readiness(findings: tuple[TaskApiETagReadinessFinding, ...]) -> ETagReadiness:
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
    "ETagReadiness",
    "ETagSafeguard",
    "ETagSignal",
    "TaskApiETagReadinessFinding",
    "TaskApiETagReadinessPlan",
    "build_task_api_etag_readiness_plan",
    "derive_task_api_etag_readiness_plan",
    "extract_task_api_etag_readiness_findings",
    "generate_task_api_etag_readiness_plan",
    "summarize_task_api_etag_readiness",
]
