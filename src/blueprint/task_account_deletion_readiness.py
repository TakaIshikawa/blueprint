"""Plan account deletion readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


AccountDeletionSignal = Literal[
    "account_deletion",
    "erasure",
    "anonymization",
    "retention_exception",
    "deletion_queue",
    "processor_deletion",
    "restore_window",
    "audit_log",
]
AccountDeletionSafeguard = Literal[
    "irreversible_confirmation",
    "restore_window_handling",
    "background_purge_job",
    "downstream_processor_propagation",
    "audit_retention_exception_handling",
    "customer_notification",
    "validation_coverage",
]
AccountDeletionReadinessLevel = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[AccountDeletionSignal, ...] = (
    "account_deletion",
    "erasure",
    "anonymization",
    "retention_exception",
    "deletion_queue",
    "processor_deletion",
    "restore_window",
    "audit_log",
)
_SAFEGUARD_ORDER: tuple[AccountDeletionSafeguard, ...] = (
    "irreversible_confirmation",
    "restore_window_handling",
    "background_purge_job",
    "downstream_processor_propagation",
    "audit_retention_exception_handling",
    "customer_notification",
    "validation_coverage",
)
_READINESS_ORDER: dict[AccountDeletionReadinessLevel, int] = {
    "weak": 0,
    "partial": 1,
    "strong": 2,
}

_SIGNAL_PATTERNS: dict[AccountDeletionSignal, re.Pattern[str]] = {
    "account_deletion": re.compile(
        r"\b(?:account deletion|delete account|delete user account|user deletion|user delete|"
        r"account closure|close account|delete profile|remove account|deleted account)\b",
        re.I,
    ),
    "erasure": re.compile(
        r"\b(?:erase|erasure|data erasure|right to erasure|gdpr deletion|permanent delete|"
        r"hard delete|delete personal data|remove personal data)\b",
        re.I,
    ),
    "anonymization": re.compile(
        r"\b(?:anonymi[sz]e|anonymi[sz]ation|pseudonymi[sz]e|pseudonymi[sz]ation|"
        r"de[- ]?identify|redact|scrub pii|pii cleanup)\b",
        re.I,
    ),
    "retention_exception": re.compile(
        r"\b(?:retention exception|retention hold|legal hold|compliance hold|retain(?:ed)? for compliance|"
        r"tax retention|billing retention|data retention exception)\b",
        re.I,
    ),
    "deletion_queue": re.compile(
        r"\b(?:deletion queue|delete queue|erasure queue|purge queue|queued deletion|deletion job|"
        r"purge job|cleanup worker|deletion worker|delete worker)\b",
        re.I,
    ),
    "processor_deletion": re.compile(
        r"\b(?:processor deletion|processor erasure|subprocessor deletion|downstream deletion|"
        r"third[- ]party deletion|vendor deletion|delete from stripe|delete from analytics|"
        r"delete from processor|data processor)\b",
        re.I,
    ),
    "restore_window": re.compile(
        r"\b(?:restore window|recovery window|undo deletion|recover account|soft delete|grace period|"
        r"deletion delay|pending deletion)\b",
        re.I,
    ),
    "audit_log": re.compile(
        r"\b(?:audit logs?|audit trails?|audit events?|deletion audit|compliance logs?|evidence logs?|"
        r"deletion record)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[AccountDeletionSafeguard, re.Pattern[str]] = {
    "irreversible_confirmation": re.compile(
        r"\b(?:irreversible confirmation|typed confirmation|type delete|confirm irreversible|"
        r"confirm permanent deletion|explicit confirmation|final confirmation|destructive confirmation)\b",
        re.I,
    ),
    "restore_window_handling": re.compile(
        r"\b(?:restore window handling|restore window|recovery window|undo deletion|recover account|"
        r"soft delete|grace period|pending deletion|deletion delay)\b",
        re.I,
    ),
    "background_purge_job": re.compile(
        r"\b(?:background purge job|purge job|purge worker|deletion job|deletion worker|"
        r"scheduled purge|async purge|queue purge|permanent purge)\b",
        re.I,
    ),
    "downstream_processor_propagation": re.compile(
        r"\b(?:downstream processor propagation|processor propagation|processor deletion|processor erasure|"
        r"subprocessor deletion|third[- ]party deletion|vendor deletion|delete from stripe|"
        r"delete from analytics|propagate deletion|propagate erasure)\b",
        re.I,
    ),
    "audit_retention_exception_handling": re.compile(
        r"\b(?:(?:audit logs?|audit trails?|audit events?|audit evidence|compliance logs?).{0,100}"
        r"(?:retention exceptions?|legal holds?|compliance holds?|deletion)|"
        r"(?:retention exceptions?|legal holds?|compliance holds?).{0,100}"
        r"(?:audit logs?|audit trails?|audit events?|audit evidence|handling|approval|record)|"
        r"audit logs? record retention exceptions?)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|user notification|notify customer|notify user|email user|"
        r"notification email|deletion confirmation email|account deletion email|completion email)\b",
        re.I,
    ),
    "validation_coverage": re.compile(
        r"\b(?:validation coverage|unit tests?|integration tests?|e2e tests?|end[- ]to[- ]end tests?|"
        r"deletion tests?|erasure tests?|purge tests?|restore tests?|processor deletion tests?|test coverage)\b",
        re.I,
    ),
}
_CHECK_GUIDANCE: dict[AccountDeletionSafeguard, str] = {
    "irreversible_confirmation": "Add an irreversible confirmation check so users must explicitly confirm permanent account deletion.",
    "restore_window_handling": "Define restore-window behavior, including recoverability, expiry, and what remains blocked while deletion is pending.",
    "background_purge_job": "Add a background purge job or worker that permanently removes eligible records after any restore or retention window.",
    "downstream_processor_propagation": "Verify deletion requests propagate to downstream processors, subprocessors, exports, and analytics stores.",
    "audit_retention_exception_handling": "Record audit evidence and retention exceptions such as legal holds, compliance holds, and retained billing records.",
    "customer_notification": "Notify the customer when deletion is scheduled, cancelled, and completed where applicable.",
    "validation_coverage": "Cover confirmation, restore-window, purge, downstream propagation, audit, and notification paths in validation.",
}


@dataclass(frozen=True, slots=True)
class TaskAccountDeletionReadinessRecord:
    """Readiness guidance for one task touching account deletion work."""

    task_id: str
    title: str
    matched_signals: tuple[AccountDeletionSignal, ...]
    present_safeguards: tuple[AccountDeletionSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[AccountDeletionSafeguard, ...] = field(default_factory=tuple)
    readiness_level: AccountDeletionReadinessLevel = "weak"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAccountDeletionReadinessPlan:
    """Plan-level account deletion readiness review."""

    plan_id: str | None = None
    records: tuple[TaskAccountDeletionReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskAccountDeletionReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskAccountDeletionReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render account deletion readiness guidance as deterministic Markdown."""
        title = "# Task Account Deletion Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(
                f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER
            ),
        ]
        if not self.records:
            lines.extend(["", "No task account deletion readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_account_deletion_readiness_plan(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Build account deletion readiness records for impacted execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskAccountDeletionReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def build_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for building account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def analyze_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for analyzing account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def summarize_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for summarizing account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def extract_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for extracting account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def generate_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for generating account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def derive_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for deriving account deletion readiness plans."""
    return build_task_account_deletion_readiness_plan(source)


def recommend_task_account_deletion_readiness(source: Any) -> TaskAccountDeletionReadinessPlan:
    """Compatibility alias for recommending account deletion readiness checks."""
    return build_task_account_deletion_readiness_plan(source)


def task_account_deletion_readiness_plan_to_dict(
    result: TaskAccountDeletionReadinessPlan,
) -> dict[str, Any]:
    """Serialize an account deletion readiness plan to a plain dictionary."""
    return result.to_dict()


task_account_deletion_readiness_plan_to_dict.__test__ = False


def task_account_deletion_readiness_plan_to_dicts(
    result: TaskAccountDeletionReadinessPlan
    | Iterable[TaskAccountDeletionReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize account deletion readiness records to plain dictionaries."""
    if isinstance(result, TaskAccountDeletionReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_account_deletion_readiness_plan_to_dicts.__test__ = False


def task_account_deletion_readiness_plan_to_markdown(
    result: TaskAccountDeletionReadinessPlan,
) -> str:
    """Render an account deletion readiness plan as Markdown."""
    return result.to_markdown()


task_account_deletion_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[AccountDeletionSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[AccountDeletionSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskAccountDeletionReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskAccountDeletionReadinessRecord(
        task_id=task_id,
        title=title,
        matched_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.present_safeguards, missing),
        recommended_checks=tuple(_CHECK_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[AccountDeletionSignal] = set()
    safeguard_hits: set[AccountDeletionSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if _validation_context(source_field, text) and _deletion_validation_text(text):
            safeguard_hits.add("validation_coverage")
            matched_safeguard = True
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits
        ),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[AccountDeletionSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[AccountDeletionSignal] = set()
    if any(token in text for token in ("account deletion", "delete account", "user deletion")):
        signals.add("account_deletion")
    if {"deletion", "delete", "erasure", "gdpr"} & parts or any(
        token in name for token in ("deletion", "delete", "erasure", "gdpr")
    ):
        signals.add("erasure")
    if any(token in text for token in ("anonym", "pseudonym", "redact", "de ident", "deid")):
        signals.add("anonymization")
    if any(token in text for token in ("retention exception", "legal hold", "compliance hold")):
        signals.add("retention_exception")
    if any(token in text for token in ("deletion queue", "delete queue", "purge queue", "deletion worker", "purge worker")):
        signals.add("deletion_queue")
    if any(token in text for token in ("processor delete", "processor deletion", "processor erasure", "subprocessor", "vendor deletion")):
        signals.add("processor_deletion")
    if any(token in text for token in ("restore window", "recovery window", "soft delete", "pending deletion")):
        signals.add("restore_window")
    if any(token in text for token in ("audit log", "audit trail", "deletion audit", "compliance log")):
        signals.add("audit_log")
    return signals


def _readiness_level(
    present: tuple[AccountDeletionSafeguard, ...],
    missing: tuple[AccountDeletionSafeguard, ...],
) -> AccountDeletionReadinessLevel:
    if not missing:
        return "strong"
    if present:
        return "partial"
    return "weak"


def _summary(
    records: tuple[TaskAccountDeletionReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
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


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
        commands.extend(_nested_validation_commands(metadata, key))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


def _nested_validation_commands(value: Any, key_name: str) -> list[str]:
    if isinstance(value, Mapping):
        commands: list[str] = []
        for key, child in value.items():
            if str(key) == key_name:
                if isinstance(child, Mapping):
                    commands.extend(flatten_validation_commands(child))
                else:
                    commands.extend(_strings(child))
                continue
            if isinstance(child, (Mapping, list, tuple, set)):
                commands.extend(_nested_validation_commands(child, key_name))
        return commands
    if isinstance(value, (list, tuple, set)):
        commands: list[str] = []
        for item in value:
            if isinstance(item, (Mapping, list, tuple, set)):
                commands.extend(_nested_validation_commands(item, key_name))
        return commands
    return []


def _validation_context(source_field: str, text: str) -> bool:
    return "validation" in source_field or "test_command" in source_field or bool(
        re.search(r"\b(?:test|pytest|spec|coverage)\b", text, re.I)
    )


def _deletion_validation_text(text: str) -> bool:
    searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
    return bool(
        re.search(
            r"\b(?:account deletion|delete account|deletion|erasure|purge|restore|processor delete|"
            r"audit|notification)\b",
            searchable,
            re.I,
        )
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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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
    "AccountDeletionReadinessLevel",
    "AccountDeletionSafeguard",
    "AccountDeletionSignal",
    "TaskAccountDeletionReadinessPlan",
    "TaskAccountDeletionReadinessRecord",
    "analyze_task_account_deletion_readiness",
    "build_task_account_deletion_readiness",
    "build_task_account_deletion_readiness_plan",
    "derive_task_account_deletion_readiness",
    "extract_task_account_deletion_readiness",
    "generate_task_account_deletion_readiness",
    "recommend_task_account_deletion_readiness",
    "summarize_task_account_deletion_readiness",
    "task_account_deletion_readiness_plan_to_dict",
    "task_account_deletion_readiness_plan_to_dicts",
    "task_account_deletion_readiness_plan_to_markdown",
]
