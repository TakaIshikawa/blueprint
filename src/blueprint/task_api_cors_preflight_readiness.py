"""Plan API CORS preflight readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CORSPreflightSignal = Literal[
    "options_handling",
    "request_method_validation",
    "request_headers_validation",
    "allow_methods_response",
    "allow_headers_response",
    "max_age_caching",
    "custom_header_support",
]
CORSPreflightSafeguard = Literal[
    "preflight_request_tests",
    "method_validation_tests",
    "header_validation_tests",
    "cache_behavior_tests",
    "preflight_documentation",
]
CORSPreflightReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CORSPreflightReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[CORSPreflightSignal, ...] = (
    "options_handling",
    "request_method_validation",
    "request_headers_validation",
    "allow_methods_response",
    "allow_headers_response",
    "max_age_caching",
    "custom_header_support",
)
_SAFEGUARD_ORDER: tuple[CORSPreflightSafeguard, ...] = (
    "preflight_request_tests",
    "method_validation_tests",
    "header_validation_tests",
    "cache_behavior_tests",
    "preflight_documentation",
)
_SIGNAL_PATTERNS: dict[CORSPreflightSignal, re.Pattern[str]] = {
    "options_handling": re.compile(
        r"\b(?:options request|options method|options endpoint|http options|handle options)\b",
        re.I,
    ),
    "request_method_validation": re.compile(
        r"\b(?:access-control-request-method|request method|validate method|method validation)\b",
        re.I,
    ),
    "request_headers_validation": re.compile(
        r"\b(?:access-control-request-headers|request headers?|validate headers?|header validation)\b",
        re.I,
    ),
    "allow_methods_response": re.compile(
        r"\b(?:access-control-allow-methods|allow(?:ed)? methods?|get|post|put|patch|delete)\b",
        re.I,
    ),
    "allow_headers_response": re.compile(
        r"\b(?:access-control-allow-headers|allow(?:ed)? headers?|content-type|authorization|x-api-key)\b",
        re.I,
    ),
    "max_age_caching": re.compile(
        r"\b(?:access-control-max-age|max-age|preflight cache|cache duration|3600|86400)\b",
        re.I,
    ),
    "custom_header_support": re.compile(
        r"\b(?:custom headers?|x-(?:api-key|custom|auth)|non-standard headers?)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CORSPreflightSignal, re.Pattern[str]] = {
    "options_handling": re.compile(r"options|preflight|cors", re.I),
    "request_method_validation": re.compile(r"method|validate|request", re.I),
    "request_headers_validation": re.compile(r"header|validate|request", re.I),
    "allow_methods_response": re.compile(r"allow|method|response", re.I),
    "allow_headers_response": re.compile(r"allow|header|response", re.I),
    "max_age_caching": re.compile(r"max[_-]?age|cache|ttl", re.I),
    "custom_header_support": re.compile(r"custom|header|x-", re.I),
}
_SAFEGUARD_PATTERNS: dict[CORSPreflightSafeguard, re.Pattern[str]] = {
    "preflight_request_tests": re.compile(
        r"\b(?:(?:preflight|options).{0,80}(?:request|tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:preflight|options).{0,80}(?:request|response)|"
        r"options request tests?|preflight tests?)\b",
        re.I,
    ),
    "method_validation_tests": re.compile(
        r"\b(?:(?:method|access-control-request-method).{0,80}(?:validat|check|verif|tests?)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:method|access-control-request-method).{0,80}(?:validat|check|verif)|"
        r"method validation tests?)\b",
        re.I,
    ),
    "header_validation_tests": re.compile(
        r"\b(?:(?:header|access-control-request-headers).{0,80}(?:validat|check|verif|tests?)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:header|access-control-request-headers).{0,80}(?:validat|check|verif)|"
        r"header validation tests?)\b",
        re.I,
    ),
    "cache_behavior_tests": re.compile(
        r"\b(?:(?:preflight|max-age).{0,80}(?:cache|caching|ttl).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:preflight|max-age).{0,80}(?:cache|caching|ttl)|"
        r"cache behavior tests?)\b",
        re.I,
    ),
    "preflight_documentation": re.compile(
        r"\b(?:(?:preflight|cors|options).{0,80}(?:docs|documentation|guide|readme|openapi|swagger|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|examples?).{0,80}(?:preflight|cors|options)|"
        r"preflight.{0,80}(?:usage|integration|client))\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:cors|preflight|options)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[CORSPreflightSafeguard, str] = {
    "preflight_request_tests": "Add tests verifying OPTIONS request handling, preflight response headers, and CORS header validation.",
    "method_validation_tests": "Add tests for Access-Control-Request-Method validation, ensuring unsupported methods are rejected.",
    "header_validation_tests": "Add tests for Access-Control-Request-Headers validation, rejecting unsupported custom headers.",
    "cache_behavior_tests": "Add tests verifying Access-Control-Max-Age cache behavior and preflight response reuse.",
    "preflight_documentation": "Document CORS preflight flow, allowed methods/headers, cache duration, and client integration examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiCORSPreflightReadinessFinding:
    """API CORS preflight readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[CORSPreflightSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CORSPreflightSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CORSPreflightSafeguard, ...] = field(default_factory=tuple)
    readiness: CORSPreflightReadiness = "partial"
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
class TaskApiCORSPreflightReadinessPlan:
    """Plan-level API CORS preflight readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiCORSPreflightReadinessFinding, ...] = field(default_factory=tuple)
    cors_preflight_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiCORSPreflightReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "cors_preflight_task_ids": list(self.cors_preflight_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }


def build_task_api_cors_preflight_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCORSPreflightReadinessPlan:
    """Build a CORS preflight readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    cors_preflight_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in cors_preflight_ids
    )
    return TaskApiCORSPreflightReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        cors_preflight_task_ids=cors_preflight_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, cors_preflight_ids, not_applicable_ids),
    )


def derive_task_api_cors_preflight_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCORSPreflightReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_cors_preflight_readiness_plan(plan)


def generate_task_api_cors_preflight_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCORSPreflightReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_cors_preflight_readiness_plan(plan)


def extract_task_api_cors_preflight_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiCORSPreflightReadinessFinding, ...]:
    """Return CORS preflight readiness findings for applicable tasks."""
    return build_task_api_cors_preflight_readiness_plan(plan).findings


def summarize_task_api_cors_preflight_readiness(
    result: TaskApiCORSPreflightReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for CORS preflight readiness review."""
    if isinstance(result, TaskApiCORSPreflightReadinessPlan):
        return dict(result.summary)
    return build_task_api_cors_preflight_readiness_plan(result).summary


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


def _analyze_task(task: Mapping[str, Any]) -> TaskApiCORSPreflightReadinessFinding | None:
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

    return TaskApiCORSPreflightReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[CORSPreflightSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    detected: list[CORSPreflightSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[CORSPreflightSafeguard, ...]:
    searchable = _searchable_task_text(task)
    detected: list[CORSPreflightSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[CORSPreflightSignal, ...],
    present: tuple[CORSPreflightSafeguard, ...],
    missing: tuple[CORSPreflightSafeguard, ...],
) -> CORSPreflightReadiness:
    if not signals:
        return "weak"
    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[CORSPreflightSignal, ...],
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
    findings: tuple[TaskApiCORSPreflightReadinessFinding, ...],
    cors_preflight_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "cors_preflight_task_count": len(cors_preflight_ids),
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


def _overall_readiness(findings: tuple[TaskApiCORSPreflightReadinessFinding, ...]) -> CORSPreflightReadiness:
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
    "CORSPreflightReadiness",
    "CORSPreflightSafeguard",
    "CORSPreflightSignal",
    "TaskApiCORSPreflightReadinessFinding",
    "TaskApiCORSPreflightReadinessPlan",
    "build_task_api_cors_preflight_readiness_plan",
    "derive_task_api_cors_preflight_readiness_plan",
    "extract_task_api_cors_preflight_readiness_findings",
    "generate_task_api_cors_preflight_readiness_plan",
    "summarize_task_api_cors_preflight_readiness",
]
