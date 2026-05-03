"""Plan idempotency readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IdempotencySignal = Literal[
    "webhook_retry",
    "payment_mutation",
    "bulk_import",
    "background_job",
    "external_api_call",
    "mutation_endpoint",
]
IdempotencySafeguard = Literal[
    "idempotency_key",
    "replay_handling",
    "duplicate_suppression",
    "retry_safe_persistence",
    "idempotency_tests",
]
IdempotencyRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[IdempotencyRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[IdempotencySignal, ...] = (
    "webhook_retry",
    "payment_mutation",
    "bulk_import",
    "background_job",
    "external_api_call",
    "mutation_endpoint",
)
_SAFEGUARD_ORDER: tuple[IdempotencySafeguard, ...] = (
    "idempotency_key",
    "replay_handling",
    "duplicate_suppression",
    "retry_safe_persistence",
    "idempotency_tests",
)
_SIGNAL_PATTERNS: dict[IdempotencySignal, re.Pattern[str]] = {
    "webhook_retry": re.compile(
        r"\b(?:webhooks?|web hooks?|event callbacks?|callback receiver|event receiver|"
        r"provider events?|retry delivery|redeliver(?:y|ed)|replay webhook)\b",
        re.I,
    ),
    "payment_mutation": re.compile(
        r"\b(?:payments?|charge|charges|capture|refund|payout|invoice|subscription|checkout|billing).{0,80}"
        r"(?:create|update|delete|mutat|write|post|put|patch|retry|provider|stripe|adyen|paypal)|"
        r"\b(?:stripe|adyen|paypal).{0,80}(?:payment|charge|refund|checkout|subscription)\b",
        re.I,
    ),
    "bulk_import": re.compile(
        r"\b(?:bulk import|csv import|data import|import job|bulk load|backfill|batch import|"
        r"reprocess import|import retry)\b",
        re.I,
    ),
    "background_job": re.compile(
        r"\b(?:background jobs?|workers?|job queue|task queue|queue consumer|async jobs?|scheduled jobs?|"
        r"cron jobs?|dlq|dead[- ]letter|retry queue)\b",
        re.I,
    ),
    "external_api_call": re.compile(
        r"\b(?:external api|third[- ]party api|provider api|vendor api|outbound api|integration call|"
        r"remote service|webhook provider|stripe|shopify|salesforce|slack|github|sendgrid)\b",
        re.I,
    ),
    "mutation_endpoint": re.compile(
        r"\b(?:mutation endpoint|write endpoint|create endpoint|update endpoint|delete endpoint|"
        r"post endpoint|put endpoint|patch endpoint|delete request|post request|put request|patch request|"
        r"create|update|delete|upsert|persist|save|mutate|crud)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[IdempotencySignal, re.Pattern[str]] = {
    "webhook_retry": re.compile(r"(?:webhooks?|callbacks?|events?)(?:/|$)|webhook[_-]?retry|redeliver", re.I),
    "payment_mutation": re.compile(r"(?:payments?|billing|checkout|stripe|refund|charge|invoice)", re.I),
    "bulk_import": re.compile(r"(?:imports?|bulk|backfill|csv)", re.I),
    "background_job": re.compile(r"(?:workers?|jobs?|queues?|cron|dlq|dead[_-]?letter)", re.I),
    "external_api_call": re.compile(r"(?:integrations?|providers?|connectors?|external|vendor|stripe|shopify|salesforce)", re.I),
    "mutation_endpoint": re.compile(r"(?:controllers?|routes?|endpoints?|mutations?|commands?|handlers?)", re.I),
}
_SAFEGUARD_PATTERNS: dict[IdempotencySafeguard, re.Pattern[str]] = {
    "idempotency_key": re.compile(
        r"\b(?:idempotency[-_ ]key|idempotent key|request id|request-id|client token|dedupe key|"
        r"operation key|idempotency token)\b",
        re.I,
    ),
    "replay_handling": re.compile(
        r"\b(?:replay handling|replay[- ]safe|webhook replay|event replay|repeated delivery|"
        r"duplicate delivery|redelivery|redeliver(?:y|ed)|at[- ]least[- ]once|out[- ]of[- ]order)\b",
        re.I,
    ),
    "duplicate_suppression": re.compile(
        r"\b(?:duplicate suppression|suppress duplicates?|dedupe|de[- ]duplicate|deduplication|"
        r"already processed|processed events?|unique constraint|unique index|natural key|de[- ]dupe)\b",
        re.I,
    ),
    "retry_safe_persistence": re.compile(
        r"\b(?:retry[- ]safe persistence|retry[- ]safe write|atomic write|transaction|transactional|"
        r"upsert|compare[- ]and[- ]swap|optimistic lock|outbox|inbox table|savepoint|partial failure)\b",
        re.I,
    ),
    "idempotency_tests": re.compile(
        r"\b(?:(?:idempotency|idempotent|duplicate|replay|retry|redelivery).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:idempotency|idempotent|duplicate|replay|retry|redelivery))\b",
        re.I,
    ),
}
_READ_ONLY_RE = re.compile(
    r"\b(?:read[- ]only|get endpoint|fetch|list|view|query|search|report|dashboard|docs?|copy only|"
    r"no mutation|without writes?|does not write|no writes?|no side effects?)\b",
    re.I,
)
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:idempotency|retry|webhook|payment|import|background job|mutation|write)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[IdempotencySafeguard, str] = {
    "idempotency_key": "Require a stable idempotency key or operation key for each retried mutation.",
    "replay_handling": "Define replay handling for repeated, delayed, and out-of-order delivery.",
    "duplicate_suppression": "Persist processed-operation state and suppress duplicate webhook, import, or job work.",
    "retry_safe_persistence": "Use retry-safe persistence with atomic writes, upserts, transactions, or recovery for partial failures.",
    "idempotency_tests": "Add tests that retry or replay the task path and assert only one durable side effect.",
}


@dataclass(frozen=True, slots=True)
class TaskIdempotencyReadinessFinding:
    """Idempotency readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[IdempotencySignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[IdempotencySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[IdempotencySafeguard, ...] = field(default_factory=tuple)
    risk_level: IdempotencyRiskLevel = "medium"
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
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskIdempotencyReadinessPlan:
    """Plan-level idempotency readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskIdempotencyReadinessFinding, ...] = field(default_factory=tuple)
    idempotency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskIdempotencyReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    @property
    def recommendations(self) -> tuple[TaskIdempotencyReadinessFinding, ...]:
        """Compatibility view for older callers that exposed recommendations."""
        return self.findings

    @property
    def sensitive_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older callers that named idempotency tasks sensitive."""
        return self.idempotency_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "idempotency_task_ids": list(self.idempotency_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render idempotency readiness as deterministic Markdown."""
        title = "# Task Idempotency Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('total_task_count', 0)}",
            f"- Idempotency task count: {self.summary.get('idempotency_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.findings:
            lines.extend(["", "No idempotency readiness findings were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Remediation | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.risk_level} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.actionable_remediations) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_idempotency_readiness_plan(source: Any) -> TaskIdempotencyReadinessPlan:
    """Build idempotency readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_RISK_ORDER[finding.risk_level], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskIdempotencyReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        idempotency_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_idempotency_readiness(source: Any) -> TaskIdempotencyReadinessPlan:
    """Compatibility alias for building idempotency readiness plans."""
    return build_task_idempotency_readiness_plan(source)


def summarize_task_idempotency_readiness(source: Any) -> TaskIdempotencyReadinessPlan:
    """Compatibility alias for building idempotency readiness plans."""
    return build_task_idempotency_readiness_plan(source)


def extract_task_idempotency_readiness(source: Any) -> TaskIdempotencyReadinessPlan:
    """Compatibility alias for extracting idempotency readiness plans."""
    return build_task_idempotency_readiness_plan(source)


def generate_task_idempotency_readiness(source: Any) -> TaskIdempotencyReadinessPlan:
    """Compatibility alias for generating idempotency readiness plans."""
    return build_task_idempotency_readiness_plan(source)


def task_idempotency_readiness_plan_to_dict(result: TaskIdempotencyReadinessPlan) -> dict[str, Any]:
    """Serialize an idempotency readiness plan to a plain dictionary."""
    return result.to_dict()


task_idempotency_readiness_plan_to_dict.__test__ = False


def task_idempotency_readiness_plan_to_dicts(
    result: TaskIdempotencyReadinessPlan | Iterable[TaskIdempotencyReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize idempotency readiness findings to plain dictionaries."""
    if isinstance(result, TaskIdempotencyReadinessPlan):
        return result.to_dicts()
    return [finding.to_dict() for finding in result]


task_idempotency_readiness_plan_to_dicts.__test__ = False


def task_idempotency_readiness_plan_to_markdown(result: TaskIdempotencyReadinessPlan) -> str:
    """Render an idempotency readiness plan as Markdown."""
    return result.to_markdown()


task_idempotency_readiness_plan_to_markdown.__test__ = False


def extract_task_idempotency_readiness_recommendations(
    source: Any,
) -> tuple[TaskIdempotencyReadinessFinding, ...]:
    """Return idempotency readiness findings for older recommendation callers."""
    return build_task_idempotency_readiness_plan(source).findings


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[IdempotencySignal, ...] = field(default_factory=tuple)
    safeguards: tuple[IdempotencySafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False
    read_only: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskIdempotencyReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not _is_idempotency_sensitive(signals):
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    task_id = _task_id(task, index)
    return TaskIdempotencyReadinessFinding(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals, missing),
        evidence=signals.evidence,
        actionable_remediations=tuple(_REMEDIATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[IdempotencySignal] = set()
    safeguard_hits: set[IdempotencySafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False
    read_only = False

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
        if _READ_ONLY_RE.search(text):
            read_only = True
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

    if "webhook_retry" in signal_hits:
        signal_hits.add("external_api_call")
    if "payment_mutation" in signal_hits:
        signal_hits.add("external_api_call")
        signal_hits.add("mutation_endpoint")
    if "bulk_import" in signal_hits or "background_job" in signal_hits:
        signal_hits.add("mutation_endpoint")

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
        read_only=read_only,
    )


def _is_idempotency_sensitive(signals: _Signals) -> bool:
    signal_set = set(signals.signals)
    if not signal_set:
        return False
    if signals.read_only and not signal_set & {"webhook_retry", "payment_mutation", "bulk_import", "background_job", "mutation_endpoint"}:
        return False
    return bool(signal_set & {"webhook_retry", "payment_mutation", "bulk_import", "background_job", "mutation_endpoint"})


def _risk_level(signals: _Signals, missing: tuple[IdempotencySafeguard, ...]) -> IdempotencyRiskLevel:
    if not missing:
        return "low"
    if signals.read_only:
        return "medium"
    signal_set = set(signals.signals)
    missing_set = set(missing)
    high_risk_signal = bool(signal_set & {"webhook_retry", "payment_mutation", "bulk_import", "background_job"})
    if "payment_mutation" in signal_set and missing_set & {"idempotency_key", "duplicate_suppression", "retry_safe_persistence"}:
        return "high"
    if "webhook_retry" in signal_set and missing_set & {"replay_handling", "duplicate_suppression"}:
        return "high"
    if high_risk_signal and len(missing) >= 3:
        return "high"
    if "mutation_endpoint" in signal_set and len(missing) >= 4:
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskIdempotencyReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "idempotency_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(finding.missing_safeguards) for finding in findings),
        "risk_counts": {risk: sum(1 for finding in findings if finding.risk_level == risk) for risk in _RISK_ORDER},
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
        "estimated_complexity",
        "estimated_hours",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
        "validation_commands",
        "test_commands",
    ):
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
        texts = []
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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


def _normalized_path(value: str) -> str:
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
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
    return tuple(deduped)


IdempotencySurface = IdempotencySignal
IdempotencyAcceptanceCriterion = IdempotencySafeguard
TaskIdempotencyReadinessRecommendation = TaskIdempotencyReadinessFinding


__all__ = [
    "IdempotencyAcceptanceCriterion",
    "IdempotencyRiskLevel",
    "IdempotencySafeguard",
    "IdempotencySignal",
    "IdempotencySurface",
    "TaskIdempotencyReadinessFinding",
    "TaskIdempotencyReadinessPlan",
    "TaskIdempotencyReadinessRecommendation",
    "analyze_task_idempotency_readiness",
    "build_task_idempotency_readiness_plan",
    "extract_task_idempotency_readiness",
    "extract_task_idempotency_readiness_recommendations",
    "generate_task_idempotency_readiness",
    "summarize_task_idempotency_readiness",
    "task_idempotency_readiness_plan_to_dict",
    "task_idempotency_readiness_plan_to_dicts",
    "task_idempotency_readiness_plan_to_markdown",
]
