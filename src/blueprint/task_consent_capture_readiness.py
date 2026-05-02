"""Plan consent capture readiness checks for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ConsentReadinessSignal = Literal[
    "user_facing_capture",
    "consent_change",
    "consent_storage",
    "withdrawal_path",
    "auditability",
    "downstream_propagation",
]
ConsentReadinessSafeguard = Literal[
    "explicit_consent_ui",
    "consent_timestamp_storage",
    "withdrawal_path",
    "audit_trail",
    "downstream_propagation",
]
ConsentReadinessImpactLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ConsentReadinessSignal, ...] = (
    "user_facing_capture",
    "consent_change",
    "consent_storage",
    "withdrawal_path",
    "auditability",
    "downstream_propagation",
)
_SAFEGUARD_ORDER: tuple[ConsentReadinessSafeguard, ...] = (
    "explicit_consent_ui",
    "consent_timestamp_storage",
    "withdrawal_path",
    "audit_trail",
    "downstream_propagation",
)
_IMPACT_ORDER: dict[ConsentReadinessImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_KEYS = (
    "consent_signals",
    "consent_capture_signals",
    "privacy_signals",
    "readiness_signals",
    "signals",
)
_SAFEGUARD_KEYS = (
    "consent_safeguards",
    "safeguards",
    "recommended_safeguards",
    "checks",
    "test_coverage",
)
_PATH_SIGNAL_PATTERNS: tuple[tuple[ConsentReadinessSignal, re.Pattern[str]], ...] = (
    (
        "user_facing_capture",
        re.compile(
            r"\b(?:consent|opt[-_]?in|preference|permission)s?\b.*\b(?:form|ui|modal|banner|settings|checkout|signup|profile)\b|\b(?:form|ui|modal|banner|settings|checkout|signup|profile)\b.*\b(?:consent|opt[-_]?in|permission)s?\b",
            re.I,
        ),
    ),
    (
        "consent_change",
        re.compile(r"\b(?:consent|opt[-_]?in|opt[-_]?out|preference|permission)s?\b", re.I),
    ),
    (
        "consent_storage",
        re.compile(
            r"\b(?:consent|preference|permission)s?\b.*\b(?:store|storage|model|schema|migration|repository|database|db|table|record)s?\b|\b(?:store|storage|model|schema|migration|repository|database|db|table|record)s?\b.*\b(?:consent|preference|permission)s?\b",
            re.I,
        ),
    ),
    (
        "withdrawal_path",
        re.compile(r"\b(?:withdraw|withdrawal|revoke|revocation|opt[-_]?out|unsubscribe)\b", re.I),
    ),
    (
        "auditability",
        re.compile(r"\b(?:audit|audit[-_]?log|history|evidence|event|ledger)\b", re.I),
    ),
    (
        "downstream_propagation",
        re.compile(
            r"\b(?:propagat|sync|webhook|worker|queue|event|downstream|third[-_]?party|crm|marketing|notification|analytics)\b",
            re.I,
        ),
    ),
)
_TEXT_SIGNAL_PATTERNS: dict[ConsentReadinessSignal, re.Pattern[str]] = {
    "user_facing_capture": re.compile(
        r"\b(?:(?:capture|collect|ask for|request|record|present|show)\s+(?:explicit\s+)?(?:user\s+)?consent|"
        r"consent\s+(?:form|checkbox|banner|modal|dialog|screen|ui|copy|prompt)|"
        r"(?:opt[- ]in|permission)\s+(?:form|checkbox|toggle|screen|ui|prompt)|"
        r"user[- ]facing\s+consent)\b",
        re.I,
    ),
    "consent_change": re.compile(
        r"\b(?:consent\s+(?:change|update|edit|toggle|preference|version|state)|"
        r"(?:opt[- ]in|opt[- ]out|permission|preference)s?\s+(?:change|update|edit|toggle|state)|"
        r"change\s+(?:user\s+)?consent)\b",
        re.I,
    ),
    "consent_storage": re.compile(
        r"\b(?:store|persist|save|record|database|schema|model|table|column|migration|repository)\s+"
        r"(?:the\s+)?(?:user\s+)?consent\b|\bconsent\s+"
        r"(?:storage|store|record|database|schema|model|table|column|migration|repository|state)\b",
        re.I,
    ),
    "withdrawal_path": re.compile(
        r"\b(?:withdraw(?:al)?|revoke|revocation|remove consent|delete consent|opt[- ]out|unsubscribe|"
        r"stop processing|consent cancellation)\b",
        re.I,
    ),
    "auditability": re.compile(
        r"\b(?:consent\s+(?:audit|audit trail|audit log|history|event|evidence|ledger)|"
        r"(?:audit trail|audit log|audit event|history|evidence)\s+(?:for\s+)?consent)\b",
        re.I,
    ),
    "downstream_propagation": re.compile(
        r"\b(?:propagat(?:e|ion|ing)|sync|fan[- ]out|publish|webhook|event|worker|queue|downstream|"
        r"third[- ]party|crm|marketing|notification|analytics)\b.*\bconsent\b|\bconsent\b.*\b(?:propagat(?:e|ion|ing)|sync|fan[- ]out|publish|webhook|event|worker|queue|downstream|third[- ]party|crm|marketing|notification|analytics)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[ConsentReadinessSafeguard, re.Pattern[str]] = {
    "explicit_consent_ui": re.compile(
        r"\b(?:explicit consent|clear consent|unchecked checkbox|not pre[- ]checked|affirmative action|"
        r"granular consent|consent copy|consent UI|consent screen|consent banner)\b",
        re.I,
    ),
    "consent_timestamp_storage": re.compile(
        r"\b(?:consent timestamp|consented_at|withdrawn_at|revoked_at|timestamped consent|"
        r"consent version|policy version|terms version|capture time|recorded at)\b",
        re.I,
    ),
    "withdrawal_path": re.compile(
        r"\b(?:withdrawal path|withdraw consent|revoke consent|revocation path|opt[- ]out path|"
        r"unsubscribe path|self[- ]service withdrawal|delete consent)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audit event|consent history|history event|immutable log|"
        r"compliance evidence|audit evidence)\b",
        re.I,
    ),
    "downstream_propagation": re.compile(
        r"\b(?:downstream propagation|propagate withdrawal|propagate consent|consent sync|"
        r"webhook propagation|event fan[- ]out|third[- ]party sync|processing stop signal)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[ConsentReadinessSafeguard, str] = {
    "explicit_consent_ui": "Verify the task defines explicit, user-facing consent capture with clear copy and no pre-checked default.",
    "consent_timestamp_storage": "Verify consent state stores capture time, withdrawal time when applicable, and consent or policy version.",
    "withdrawal_path": "Verify users can withdraw consent and that withdrawal behavior is covered before launch.",
    "audit_trail": "Verify consent capture, changes, withdrawal, and propagation emit durable audit evidence.",
    "downstream_propagation": "Verify consent changes and withdrawals propagate to downstream systems, queues, and third parties.",
}
_ALIASES: dict[str, ConsentReadinessSignal | ConsentReadinessSafeguard] = {
    "capture": "user_facing_capture",
    "consent_capture": "user_facing_capture",
    "ui": "user_facing_capture",
    "consent_ui": "explicit_consent_ui",
    "explicit_ui": "explicit_consent_ui",
    "change": "consent_change",
    "update": "consent_change",
    "storage": "consent_storage",
    "store": "consent_storage",
    "timestamp": "consent_timestamp_storage",
    "timestamp_storage": "consent_timestamp_storage",
    "withdrawal": "withdrawal_path",
    "revoke": "withdrawal_path",
    "revocation": "withdrawal_path",
    "audit": "auditability",
    "audit_log": "audit_trail",
    "auditability": "auditability",
    "propagation": "downstream_propagation",
    "downstream": "downstream_propagation",
    "sync": "downstream_propagation",
}


@dataclass(frozen=True, slots=True)
class TaskConsentCaptureReadinessRecord:
    """Consent capture readiness guidance for one execution task."""

    task_id: str
    title: str
    matched_signals: tuple[ConsentReadinessSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ConsentReadinessSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ConsentReadinessSafeguard, ...] = field(default_factory=tuple)
    impact_level: ConsentReadinessImpactLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "impact_level": self.impact_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskConsentCaptureReadinessPlan:
    """Plan-level consent capture readiness review."""

    plan_id: str | None = None
    records: tuple[TaskConsentCaptureReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskConsentCaptureReadinessRecord, ...]:
        """Compatibility view matching planners that name rows findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return consent readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the consent capture readiness plan as deterministic Markdown."""
        title = "# Task Consent Capture Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        impact_counts = self.summary.get("impact_level_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Impact counts: "
            + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No consent capture readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(
                    ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Impact | Matched Signals | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(
                ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
            )
        return "\n".join(lines)


def build_task_consent_capture_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskConsentCaptureReadinessPlan:
    """Build task-level consent capture readiness guidance."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskConsentCaptureReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_consent_capture_readiness(source: Any) -> TaskConsentCaptureReadinessPlan:
    """Compatibility alias for building consent capture readiness guidance."""
    return build_task_consent_capture_readiness_plan(source)


def summarize_task_consent_capture_readiness(source: Any) -> TaskConsentCaptureReadinessPlan:
    """Compatibility alias for building consent capture readiness guidance."""
    return build_task_consent_capture_readiness_plan(source)


def derive_task_consent_capture_readiness_plan(source: Any) -> TaskConsentCaptureReadinessPlan:
    """Compatibility alias for deriving consent capture readiness guidance."""
    return build_task_consent_capture_readiness_plan(source)


def task_consent_capture_readiness_plan_to_dict(
    result: TaskConsentCaptureReadinessPlan,
) -> dict[str, Any]:
    """Serialize a consent capture readiness plan to a plain dictionary."""
    return result.to_dict()


task_consent_capture_readiness_plan_to_dict.__test__ = False


def task_consent_capture_readiness_plan_to_dicts(
    result: TaskConsentCaptureReadinessPlan | Iterable[TaskConsentCaptureReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize consent capture readiness records to plain dictionaries."""
    if isinstance(result, TaskConsentCaptureReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_consent_capture_readiness_plan_to_dicts.__test__ = False


def task_consent_capture_readiness_plan_to_markdown(
    result: TaskConsentCaptureReadinessPlan,
) -> str:
    """Render a consent capture readiness plan as Markdown."""
    return result.to_markdown()


task_consent_capture_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Matches:
    signals: tuple[ConsentReadinessSignal, ...]
    safeguards: tuple[ConsentReadinessSafeguard, ...]
    evidence: tuple[str, ...]


def _record(task: Mapping[str, Any], index: int) -> TaskConsentCaptureReadinessRecord | None:
    matches = _matches(task)
    if not matches.signals:
        return None
    missing = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in matches.safeguards
    )
    task_id = _task_id(task, index)
    return TaskConsentCaptureReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_signals=matches.signals,
        present_safeguards=matches.safeguards,
        missing_safeguards=missing,
        impact_level=_impact_level(matches.signals),
        evidence=matches.evidence,
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
    )


def _matches(task: Mapping[str, Any]) -> _Matches:
    signals: set[ConsentReadinessSignal] = set()
    safeguards: set[ConsentReadinessSafeguard] = set()
    evidence: list[str] = []

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for signal in _metadata_signals(metadata):
            signals.add(signal)
            evidence.append(f"metadata.consent_signals: {signal}")
        for safeguard in _metadata_safeguards(metadata):
            safeguards.add(safeguard)
            evidence.append(f"metadata.consent_safeguards: {safeguard}")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(text)
        if path_signals:
            signals.update(path_signals)
            evidence.append(f"files_or_modules: {path}")
        path_safeguards = _safeguards_from_text(text)
        if path_safeguards:
            safeguards.update(path_safeguards)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        text_signals = _text_signals(text)
        text_safeguards = _safeguards_from_text(text)
        if text_signals:
            signals.update(text_signals)
            evidence.append(_evidence_snippet(source_field, text))
        if text_safeguards:
            safeguards.update(text_safeguards)
            evidence.append(_evidence_snippet(source_field, text))

    for command in _validation_commands(task):
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        command_signals = {*_text_signals(command), *_text_signals(command_text)}
        command_safeguards = {*_safeguards_from_text(command), *_safeguards_from_text(command_text)}
        if command_signals:
            signals.update(command_signals)
            evidence.append(_evidence_snippet("validation_commands", command))
        if command_safeguards:
            safeguards.update(command_safeguards)
            evidence.append(_evidence_snippet("validation_commands", command))

    return _Matches(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signals),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguards),
        evidence=tuple(_dedupe(evidence)),
    )


def _metadata_signals(metadata: Mapping[str, Any]) -> tuple[ConsentReadinessSignal, ...]:
    signals: set[ConsentReadinessSignal] = set()
    for key in _SIGNAL_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = _normalized_key(value)
            alias = _ALIASES.get(normalized, normalized)
            if alias in _SIGNAL_ORDER:
                signals.add(alias)  # type: ignore[arg-type]
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals)


def _metadata_safeguards(metadata: Mapping[str, Any]) -> tuple[ConsentReadinessSafeguard, ...]:
    safeguards: set[ConsentReadinessSafeguard] = set()
    for key in _SAFEGUARD_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = _normalized_key(value)
            alias = _ALIASES.get(normalized, normalized)
            if alias in _SAFEGUARD_ORDER:
                safeguards.add(alias)  # type: ignore[arg-type]
            safeguards.update(_safeguards_from_text(value))
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguards)


def _path_signals(text: str) -> tuple[ConsentReadinessSignal, ...]:
    signals = {signal for signal, pattern in _PATH_SIGNAL_PATTERNS if pattern.search(text)}
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals)


def _text_signals(text: str) -> tuple[ConsentReadinessSignal, ...]:
    matched = {signal for signal, pattern in _TEXT_SIGNAL_PATTERNS.items() if pattern.search(text)}
    if "consent_change" not in matched and re.search(
        r"\b(?:consent|opt[- ]in|opt[- ]out|permission|preference)s?\b", text, re.I
    ):
        matched.add("consent_change")
    return tuple(signal for signal in _SIGNAL_ORDER if signal in matched)


def _safeguards_from_text(text: str) -> set[ConsentReadinessSafeguard]:
    normalized = _normalized_key(text)
    alias = _ALIASES.get(normalized, normalized)
    safeguards = {
        safeguard
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items()
        if pattern.search(text) or alias == safeguard
    }
    return safeguards


def _impact_level(signals: tuple[ConsentReadinessSignal, ...]) -> ConsentReadinessImpactLevel:
    signal_set = set(signals)
    if "user_facing_capture" in signal_set and (
        "consent_storage" in signal_set
        or "withdrawal_path" in signal_set
        or "downstream_propagation" in signal_set
    ):
        return "high"
    if "withdrawal_path" in signal_set and (
        "consent_storage" in signal_set or "downstream_propagation" in signal_set
    ):
        return "high"
    if "downstream_propagation" in signal_set and (
        "consent_storage" in signal_set or "auditability" in signal_set
    ):
        return "high"
    if {
        "user_facing_capture",
        "consent_storage",
        "withdrawal_path",
        "downstream_propagation",
    } & signal_set:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskConsentCaptureReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "record_count": len(records),
        "impacted_task_count": len(records),
        "no_impact_task_count": len(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "impact_level_counts": {
            impact: sum(1 for record in records if record.impact_level == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
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
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
        "risks",
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
            if _metadata_key_has_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_has_signal(key_text):
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


def _metadata_key_has_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


def _normalized_key(value: str) -> str:
    return _text(value).casefold().replace("-", "_").replace(" ", "_").replace("/", "_")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "ConsentReadinessImpactLevel",
    "ConsentReadinessSafeguard",
    "ConsentReadinessSignal",
    "TaskConsentCaptureReadinessPlan",
    "TaskConsentCaptureReadinessRecord",
    "analyze_task_consent_capture_readiness",
    "build_task_consent_capture_readiness_plan",
    "derive_task_consent_capture_readiness_plan",
    "summarize_task_consent_capture_readiness",
    "task_consent_capture_readiness_plan_to_dict",
    "task_consent_capture_readiness_plan_to_dicts",
    "task_consent_capture_readiness_plan_to_markdown",
]
