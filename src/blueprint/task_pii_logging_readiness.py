"""Assess readiness for execution tasks that may log personally identifiable information."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PIILoggingSignal = Literal[
    "pii_logging",
    "sensitive_field_logging",
    "request_response_logging",
    "analytics_event_logging",
    "audit_event_logging",
]
PIILoggingSafeguard = Literal[
    "masking",
    "log_retention",
    "sampling",
    "access_controls",
    "alerting",
    "test_coverage",
]
PIILoggingReadiness = Literal["weak", "partial", "strong"]

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[PIILoggingSignal, ...] = (
    "pii_logging",
    "sensitive_field_logging",
    "request_response_logging",
    "analytics_event_logging",
    "audit_event_logging",
)
_SAFEGUARD_ORDER: tuple[PIILoggingSafeguard, ...] = (
    "masking",
    "log_retention",
    "sampling",
    "access_controls",
    "alerting",
    "test_coverage",
)
_READINESS_ORDER: dict[PIILoggingReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_PATTERNS: dict[PIILoggingSignal, re.Pattern[str]] = {
    "pii_logging": re.compile(
        r"\b(?:pii|personally identifiable|personal data|customer data|user data|sensitive data)"
        r".{0,80}\b(?:log|logging|logged|telemetry|event|audit)\b|"
        r"\b(?:log|logging|logged|telemetry|event|audit).{0,80}"
        r"\b(?:pii|personally identifiable|personal data|customer data|user data|sensitive data)\b",
        re.I,
    ),
    "sensitive_field_logging": re.compile(
        r"\b(?:email|phone|address|ssn|social security|date of birth|dob|passport|"
        r"driver'?s license|tax id|credit card|card number|bank account|ip address)"
        r".{0,80}\b(?:log|logging|logged|telemetry|event|audit)\b|"
        r"\b(?:log|logging|logged|telemetry|event|audit).{0,80}"
        r"\b(?:email|phone|address|ssn|social security|date of birth|dob|passport|"
        r"driver'?s license|tax id|credit card|card number|bank account|ip address)\b",
        re.I,
    ),
    "request_response_logging": re.compile(
        r"\b(?:request|response|payload|headers?|body|api call|http)"
        r".{0,80}\b(?:log|logging|logged|trace|telemetry)\b|"
        r"\b(?:log|logging|trace).{0,80}\b(?:request|response|payload|headers?|body)\b",
        re.I,
    ),
    "analytics_event_logging": re.compile(
        r"\b(?:analytics event|product analytics|tracking event|event tracking|"
        r"instrumentation event|telemetry event)\b",
        re.I,
    ),
    "audit_event_logging": re.compile(
        r"\b(?:audit event|audit log|security log|activity log|compliance log)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[PIILoggingSignal, re.Pattern[str]] = {
    "pii_logging": re.compile(r"pii|privacy|personal[-_]?data", re.I),
    "sensitive_field_logging": re.compile(r"email|phone|address|ssn|sensitive", re.I),
    "request_response_logging": re.compile(r"request|response|payload|http|middleware", re.I),
    "analytics_event_logging": re.compile(r"analytics|tracking|telemetry|events?", re.I),
    "audit_event_logging": re.compile(r"audit|activity[-_]?log|security[-_]?log", re.I),
}
_SAFEGUARD_PATTERNS: dict[PIILoggingSafeguard, re.Pattern[str]] = {
    "masking": re.compile(
        r"\b(?:mask|masked|masking|redact|redacted|redaction|scrub|scrubbing|tokeni[sz]e|hash)\b",
        re.I,
    ),
    "log_retention": re.compile(
        r"\b(?:log retention|retention period|ttl|expire logs?|delete logs?|purge logs?|"
        r"\d+\s*(?:days?|weeks?|months?)\s+(?:retention|ttl))\b",
        re.I,
    ),
    "sampling": re.compile(r"\b(?:sample|sampling|sample rate|rate limit logs?|drop debug logs?)\b", re.I),
    "access_controls": re.compile(
        r"\b(?:access control|rbac|least privilege|restricted access|log access|"
        r"security team only|admin only|permission)\b",
        re.I,
    ),
    "alerting": re.compile(
        r"\b(?:alert|alerting|monitor|monitoring|detect leakage|leak detection|"
        r"pii leak|sensitive data alert)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:test|tests|coverage|unit test|integration test|redaction test|"
        r"masking test|log assertion|fixture)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:pii|personal data|sensitive data|customer data|"
    r"request body|response body|payload|logging|logs?)\b.{0,80}"
    r"\b(?:scope|impact|changes?|required|needed|logged|stored)\b",
    re.I,
)
_ACTIONABLE_REMEDIATIONS: dict[PIILoggingSafeguard, str] = {
    "masking": "Mask, redact, hash, or tokenize PII and sensitive fields before writing logs.",
    "log_retention": "Define retention, TTL, and purge behavior for logs that may contain PII.",
    "sampling": "Define sampling or suppression rules so sensitive payloads are not logged at full volume.",
    "access_controls": "Restrict access to sensitive logs with RBAC, least privilege, and reviewable permissions.",
    "alerting": "Add monitoring or alerts for PII leakage and unexpected sensitive-field logging.",
    "test_coverage": "Add tests that assert PII fields are masked, omitted, or rejected in logs.",
}


@dataclass(frozen=True, slots=True)
class TaskPIILoggingReadinessFinding:
    """PII logging readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[PIILoggingSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[PIILoggingSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[PIILoggingSafeguard, ...] = field(default_factory=tuple)
    readiness: PIILoggingReadiness = "weak"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

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
class TaskPIILoggingReadinessPlan:
    """Plan-level PII logging readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskPIILoggingReadinessFinding, ...] = field(default_factory=tuple)
    pii_logging_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskPIILoggingReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "pii_logging_task_ids": list(self.pii_logging_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render PII logging readiness as deterministic Markdown."""
        title = "# Task PII Logging Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('total_task_count', 0)}",
            f"- PII logging task count: {self.summary.get('pii_logging_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.findings:
            lines.extend(["", "No PII logging readiness findings were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Remediations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.actionable_remediations) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_pii_logging_readiness_plan(source: Any) -> TaskPIILoggingReadinessPlan:
    """Build PII logging readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_READINESS_ORDER[finding.readiness], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskPIILoggingReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        pii_logging_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_pii_logging_readiness(source: Any) -> TaskPIILoggingReadinessPlan:
    """Compatibility alias for building PII logging readiness plans."""
    return build_task_pii_logging_readiness_plan(source)


def summarize_task_pii_logging_readiness(source: Any) -> TaskPIILoggingReadinessPlan:
    """Compatibility alias for building PII logging readiness plans."""
    return build_task_pii_logging_readiness_plan(source)


def extract_task_pii_logging_readiness(source: Any) -> TaskPIILoggingReadinessPlan:
    """Compatibility alias for extracting PII logging readiness plans."""
    return build_task_pii_logging_readiness_plan(source)


def generate_task_pii_logging_readiness(source: Any) -> TaskPIILoggingReadinessPlan:
    """Compatibility alias for generating PII logging readiness plans."""
    return build_task_pii_logging_readiness_plan(source)


def recommend_task_pii_logging_readiness(source: Any) -> TaskPIILoggingReadinessPlan:
    """Compatibility alias for recommending PII logging safeguards."""
    return build_task_pii_logging_readiness_plan(source)


def task_pii_logging_readiness_plan_to_dict(result: TaskPIILoggingReadinessPlan) -> dict[str, Any]:
    """Serialize a PII logging readiness plan to a plain dictionary."""
    return result.to_dict()


task_pii_logging_readiness_plan_to_dict.__test__ = False


def task_pii_logging_readiness_plan_to_dicts(
    result: TaskPIILoggingReadinessPlan | Iterable[TaskPIILoggingReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize PII logging readiness findings to plain dictionaries."""
    if isinstance(result, TaskPIILoggingReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_pii_logging_readiness_plan_to_dicts.__test__ = False


def task_pii_logging_readiness_plan_to_markdown(result: TaskPIILoggingReadinessPlan) -> str:
    """Render a PII logging readiness plan as Markdown."""
    return result.to_markdown()


task_pii_logging_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[PIILoggingSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[PIILoggingSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskPIILoggingReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    return TaskPIILoggingReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.safeguards),
        evidence=signals.evidence,
        actionable_remediations=tuple(_ACTIONABLE_REMEDIATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[PIILoggingSignal] = set()
    safeguard_hits: set[PIILoggingSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness(safeguards: tuple[PIILoggingSafeguard, ...]) -> PIILoggingReadiness:
    if len(safeguards) >= 5 and "masking" in safeguards and "test_coverage" in safeguards:
        return "strong"
    if len(safeguards) >= 3 or ("masking" in safeguards and len(safeguards) >= 2):
        return "partial"
    return "weak"


def _summary(
    findings: tuple[TaskPIILoggingReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "pii_logging_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "pii_logging_task_ids": [finding.task_id for finding in findings],
        "missing_safeguard_count": sum(len(finding.missing_safeguards) for finding in findings),
        "readiness_counts": {
            readiness: sum(1 for finding in findings if finding.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
    )


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _normalized_path(value: str) -> str:
    return value.strip().replace("\\", "/")


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _SPACE_RE.sub(" ", text).strip()
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = re.sub(r"\[\d+\]", "[]", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PIILoggingSignal",
    "PIILoggingSafeguard",
    "PIILoggingReadiness",
    "TaskPIILoggingReadinessFinding",
    "TaskPIILoggingReadinessPlan",
    "build_task_pii_logging_readiness_plan",
    "analyze_task_pii_logging_readiness",
    "summarize_task_pii_logging_readiness",
    "extract_task_pii_logging_readiness",
    "generate_task_pii_logging_readiness",
    "recommend_task_pii_logging_readiness",
    "task_pii_logging_readiness_plan_to_dict",
    "task_pii_logging_readiness_plan_to_dicts",
    "task_pii_logging_readiness_plan_to_markdown",
]
