"""Plan API hypermedia readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


HypermediaSignal = Literal[
    "hal_links",
    "jsonapi_relationships",
    "link_relations",
    "uri_templates",
    "hypermedia_controls",
    "state_transitions",
    "embedded_resources",
]
HypermediaSafeguard = Literal[
    "link_generation_tests",
    "uri_template_expansion_tests",
    "relation_type_consistency",
    "circular_reference_handling",
    "hypermedia_documentation",
]
HypermediaReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[HypermediaReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[HypermediaSignal, ...] = (
    "hal_links",
    "jsonapi_relationships",
    "link_relations",
    "uri_templates",
    "hypermedia_controls",
    "state_transitions",
    "embedded_resources",
)
_SAFEGUARD_ORDER: tuple[HypermediaSafeguard, ...] = (
    "link_generation_tests",
    "uri_template_expansion_tests",
    "relation_type_consistency",
    "circular_reference_handling",
    "hypermedia_documentation",
)
_SIGNAL_PATTERNS: dict[HypermediaSignal, re.Pattern[str]] = {
    "hal_links": re.compile(
        r"\b(?:hal|_links|_embedded|application/hal\+json|hypertext application language)\b",
        re.I,
    ),
    "jsonapi_relationships": re.compile(
        r"\b(?:json:?api|relationships?|application/vnd\.api\+json|included resources)\b",
        re.I,
    ),
    "link_relations": re.compile(
        r"\b(?:link relations?|rel=|self link|next link|prev(?:ious)? link|related link|iana link relations?)\b",
        re.I,
    ),
    "uri_templates": re.compile(
        r"\b(?:uri templates?|rfc\s*6570|templated|template expansion|variable expansion)\b",
        re.I,
    ),
    "hypermedia_controls": re.compile(
        r"\b(?:hypermedia controls?|affordances?|allowed methods?|link methods?|operations?)\b",
        re.I,
    ),
    "state_transitions": re.compile(
        r"\b(?:state transitions?|resource states?|workflow states?|state machine|resource lifecycle)\b",
        re.I,
    ),
    "embedded_resources": re.compile(
        r"\b(?:_embedded|embedded resources?|sideload(?:ed|ing)?|resource embedding|expand(?:ed)?)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[HypermediaSignal, re.Pattern[str]] = {
    "hal_links": re.compile(r"hal|_links|embed", re.I),
    "jsonapi_relationships": re.compile(r"jsonapi|json[_-]?api|relationship", re.I),
    "link_relations": re.compile(r"link|relation|rel", re.I),
    "uri_templates": re.compile(r"template|uri|rfc", re.I),
    "hypermedia_controls": re.compile(r"control|affordance|action", re.I),
    "state_transitions": re.compile(r"state|transition|workflow", re.I),
    "embedded_resources": re.compile(r"embed|sideload|expand", re.I),
}
_SAFEGUARD_PATTERNS: dict[HypermediaSafeguard, re.Pattern[str]] = {
    "link_generation_tests": re.compile(
        r"\b(?:(?:link|href|url).{0,80}(?:generat|build|construct).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:link|href|url).{0,80}(?:generat|build|construct)|"
        r"link tests?|href tests?)\b",
        re.I,
    ),
    "uri_template_expansion_tests": re.compile(
        r"\b(?:(?:uri|url).{0,80}template.{0,80}(?:expand|expansion|substitut).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:uri|url).{0,80}template.{0,80}(?:expand|expansion|substitut)|"
        r"template expansion tests?)\b",
        re.I,
    ),
    "relation_type_consistency": re.compile(
        r"\b(?:(?:relation type|rel type|link relation).{0,80}(?:consisten|valid|check|verif)|"
        r"(?:consisten|valid|check|verif).{0,80}(?:relation type|rel type|link relation)|"
        r"iana relation|standard relation)\b",
        re.I,
    ),
    "circular_reference_handling": re.compile(
        r"\b(?:circular reference|circular embed|cyclic reference|recursive embed|embed(?:ding)? depth|nesting limit|"
        r"max depth|recursion limit|self-referenc)\b",
        re.I,
    ),
    "hypermedia_documentation": re.compile(
        r"\b(?:hypermedia.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}hypermedia|"
        r"hateoas.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}hateoas)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:hypermedia|hateoas|links?)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[HypermediaSafeguard, str] = {
    "link_generation_tests": "Add tests verifying link generation logic, href construction, and relation type inclusion across different resource states.",
    "uri_template_expansion_tests": "Add tests for URI template expansion per RFC 6570, variable substitution, and malformed template handling.",
    "relation_type_consistency": "Ensure link relation types (self, next, prev, related) are consistent, follow IANA standards, and are validated.",
    "circular_reference_handling": "Define circular reference handling for embedded resources, set nesting depth limits, and prevent infinite recursion.",
    "hypermedia_documentation": "Document hypermedia format (HAL, JSON:API), link relation types, URI template syntax, and client integration examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiHypermediaReadinessFinding:
    """API hypermedia readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[HypermediaSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[HypermediaSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[HypermediaSafeguard, ...] = field(default_factory=tuple)
    readiness: HypermediaReadiness = "partial"
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
class TaskApiHypermediaReadinessPlan:
    """Plan-level API hypermedia readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiHypermediaReadinessFinding, ...] = field(default_factory=tuple)
    hypermedia_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiHypermediaReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "hypermedia_task_ids": list(self.hypermedia_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }


def build_task_api_hypermedia_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiHypermediaReadinessPlan:
    """Build a hypermedia readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    hypermedia_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in hypermedia_ids
    )
    return TaskApiHypermediaReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        hypermedia_task_ids=hypermedia_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, hypermedia_ids, not_applicable_ids),
    )


def derive_task_api_hypermedia_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiHypermediaReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_hypermedia_readiness_plan(plan)


def generate_task_api_hypermedia_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiHypermediaReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_hypermedia_readiness_plan(plan)


def extract_task_api_hypermedia_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiHypermediaReadinessFinding, ...]:
    """Return hypermedia readiness findings for applicable tasks."""
    return build_task_api_hypermedia_readiness_plan(plan).findings


def summarize_task_api_hypermedia_readiness(
    result: TaskApiHypermediaReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for hypermedia readiness review."""
    if isinstance(result, TaskApiHypermediaReadinessPlan):
        return dict(result.summary)
    return build_task_api_hypermedia_readiness_plan(result).summary


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


def _analyze_task(task: Mapping[str, Any]) -> TaskApiHypermediaReadinessFinding | None:
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

    return TaskApiHypermediaReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[HypermediaSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    detected: list[HypermediaSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[HypermediaSafeguard, ...]:
    searchable = _searchable_task_text(task)
    detected: list[HypermediaSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[HypermediaSignal, ...],
    present: tuple[HypermediaSafeguard, ...],
    missing: tuple[HypermediaSafeguard, ...],
) -> HypermediaReadiness:
    if not signals:
        return "weak"
    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[HypermediaSignal, ...],
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
    findings: tuple[TaskApiHypermediaReadinessFinding, ...],
    hypermedia_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "hypermedia_task_count": len(hypermedia_ids),
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


def _overall_readiness(findings: tuple[TaskApiHypermediaReadinessFinding, ...]) -> HypermediaReadiness:
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
    "HypermediaReadiness",
    "HypermediaSafeguard",
    "HypermediaSignal",
    "TaskApiHypermediaReadinessFinding",
    "TaskApiHypermediaReadinessPlan",
    "build_task_api_hypermedia_readiness_plan",
    "derive_task_api_hypermedia_readiness_plan",
    "extract_task_api_hypermedia_readiness_findings",
    "generate_task_api_hypermedia_readiness_plan",
    "summarize_task_api_hypermedia_readiness",
]
