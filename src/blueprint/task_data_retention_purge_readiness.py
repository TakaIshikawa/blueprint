"""Plan data retention and purge readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RetentionPurgeSignal = Literal[
    "retention_policy",
    "purge_cadence",
    "expired_records",
    "archived_entities",
    "temporary_files",
    "sessions_tokens",
    "exports",
    "event_logs",
    "scheduled_deletion_job",
]
RetentionPurgeSafeguard = Literal[
    "retention_policy",
    "purge_cadence",
    "legal_hold",
    "dry_run_backfill",
    "idempotency",
    "restore_window",
    "audit_evidence",
    "monitoring",
]
RetentionPurgeRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[RetentionPurgeRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[RetentionPurgeSignal, ...] = (
    "retention_policy",
    "purge_cadence",
    "expired_records",
    "archived_entities",
    "temporary_files",
    "sessions_tokens",
    "exports",
    "event_logs",
    "scheduled_deletion_job",
)
_SAFEGUARD_ORDER: tuple[RetentionPurgeSafeguard, ...] = (
    "retention_policy",
    "purge_cadence",
    "legal_hold",
    "dry_run_backfill",
    "idempotency",
    "restore_window",
    "audit_evidence",
    "monitoring",
)
_PATH_SIGNAL_PATTERNS: dict[RetentionPurgeSignal, re.Pattern[str]] = {
    "retention_policy": re.compile(r"(?:retention|ttl|lifecycle)", re.I),
    "purge_cadence": re.compile(r"(?:purge|cleanup|clean[-_]?up|prune|expire|expiration)", re.I),
    "expired_records": re.compile(r"(?:expired|stale|old[-_]?records?|aged[-_]?out)", re.I),
    "archived_entities": re.compile(r"(?:archive|archived|cold[-_]?data)", re.I),
    "temporary_files": re.compile(r"(?:tmp|temp|temporary|scratch|uploads?|files?)", re.I),
    "sessions_tokens": re.compile(r"(?:sessions?|tokens?|refresh[-_]?tokens?|api[-_]?keys?)", re.I),
    "exports": re.compile(r"(?:exports?|downloads?|reports?|csv|pdf)", re.I),
    "event_logs": re.compile(r"(?:events?|event[-_]?logs?|activity[-_]?logs?|webhook[-_]?logs?)", re.I),
    "scheduled_deletion_job": re.compile(r"(?:cron|scheduled|scheduler|jobs?|worker|queue)", re.I),
}
_TEXT_SIGNAL_PATTERNS: dict[RetentionPurgeSignal, re.Pattern[str]] = {
    "retention_policy": re.compile(
        r"\b(?:retention policy|retention period|retention rule|retain for|kept for|keep for|ttl|"
        r"time[- ]to[- ]live|data lifecycle|lifecycle cleanup|expire after)\b",
        re.I,
    ),
    "purge_cadence": re.compile(
        r"\b(?:purge cadence|purge schedule|cleanup cadence|cleanup schedule|daily purge|weekly purge|"
        r"monthly purge|scheduled purge|prune cadence|expiration cadence|delete after)\b",
        re.I,
    ),
    "expired_records": re.compile(
        r"\b(?:expired records?|expired data|stale records?|old records?|aged[- ]out records?|"
        r"records? past retention|records? older than)\b",
        re.I,
    ),
    "archived_entities": re.compile(
        r"\b(?:archived entities|archived records?|archived projects?|archived accounts?|archive cleanup|"
        r"closed cases?|inactive entities|cold data)\b",
        re.I,
    ),
    "temporary_files": re.compile(
        r"\b(?:temporary files?|temp files?|tmp files?|scratch files?|staged uploads?|transient uploads?|"
        r"temporary exports?|temporary attachments?)\b",
        re.I,
    ),
    "sessions_tokens": re.compile(
        r"\b(?:sessions?|session data|session cleanup|refresh tokens?|access tokens?|api tokens?|"
        r"expired tokens?|token cleanup|revoked tokens?)\b",
        re.I,
    ),
    "exports": re.compile(
        r"\b(?:exports?|export files?|report exports?|csv exports?|download links?|generated reports?|"
        r"generated pdfs?)\b",
        re.I,
    ),
    "event_logs": re.compile(
        r"\b(?:event logs?|events? table|activity logs?|webhook logs?|integration logs?|event retention|"
        r"log retention|purge events?)\b",
        re.I,
    ),
    "scheduled_deletion_job": re.compile(
        r"\b(?:scheduled deletion job|scheduled cleanup|scheduled purge|purge job|cleanup job|"
        r"retention worker|expiration worker|cron job|nightly cleanup|background cleanup)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[RetentionPurgeSafeguard, re.Pattern[str]] = {
    "retention_policy": re.compile(
        r"\b(?:retention policy|retention period|retention rules?|retain for|kept for|keep for|ttl|"
        r"time[- ]to[- ]live|expire after|data lifecycle policy)\b",
        re.I,
    ),
    "purge_cadence": re.compile(
        r"\b(?:purge cadence|purge schedule|cleanup cadence|cleanup schedule|daily purge|weekly purge|"
        r"monthly purge|scheduled purge|cron cadence|expiration cadence)\b",
        re.I,
    ),
    "legal_hold": re.compile(
        r"\b(?:legal hold|litigation hold|compliance hold|retention hold|hold exemption|hold exception|"
        r"do not purge|do not delete|hold check)\b",
        re.I,
    ),
    "dry_run_backfill": re.compile(
        r"\b(?:dry[- ]?run|preview mode|backfill plan|backfill|shadow run|no-op run|would purge|"
        r"affected row counts?|purge counts?|batch sizing)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotent|idempotency|retry safe|retry-safe|safe retry|already purged|already expired|"
        r"repeatable cleanup|resume safely)\b",
        re.I,
    ),
    "restore_window": re.compile(
        r"\b(?:restore window|recovery window|rollback window|undelete window|soft delete|trash window|"
        r"restore from backup|point[- ]in[- ]time restore|pitr)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit trail|audit log|purge receipt|deletion receipt|evidence record|"
        r"purge event|cleanup event|actor recorded|reviewer sign[- ]?off)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitoring|alerts?|metrics?|dashboard|failure alert|lag metric|purge lag|cleanup lag|"
        r"job health|dead letter|dlq|slo|error budget)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[RetentionPurgeSafeguard, str] = {
    "retention_policy": "Define the record classes, retention periods, TTL rules, and owner-approved exceptions before purge execution.",
    "purge_cadence": "Document purge cadence, batch limits, scheduling, and rollout controls for recurring cleanup.",
    "legal_hold": "Check legal holds, compliance holds, and retention exceptions before records become purge-eligible.",
    "dry_run_backfill": "Add dry-run counts and a backfill plan that previews affected records before deleting or expiring data.",
    "idempotency": "Make cleanup idempotent and retry-safe for already-purged, partially processed, or resumed batches.",
    "restore_window": "Confirm restore, rollback, soft-delete, or recovery-window behavior before irreversible purge.",
    "audit_evidence": "Record audit evidence for purge scope, actor or job identity, timing, counts, holds skipped, and outcome.",
    "monitoring": "Add monitoring and alerts for purge failures, lag, unexpected volume, skipped holds, and job health.",
}


@dataclass(frozen=True, slots=True)
class TaskDataRetentionPurgeReadinessRecord:
    """Readiness guidance for one task touching retention-driven purge behavior."""

    task_id: str
    title: str
    detected_signals: tuple[RetentionPurgeSignal, ...]
    required_safeguards: tuple[RetentionPurgeSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[RetentionPurgeSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[RetentionPurgeSafeguard, ...] = field(default_factory=tuple)
    risk_level: RetentionPurgeRisk = "medium"
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataRetentionPurgeReadinessPlan:
    """Plan-level retention purge readiness review."""

    plan_id: str | None = None
    records: tuple[TaskDataRetentionPurgeReadinessRecord, ...] = field(default_factory=tuple)
    retention_purge_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskDataRetentionPurgeReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "retention_purge_task_ids": list(self.retention_purge_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render retention purge readiness as deterministic Markdown."""
        title = "# Task Data Retention Purge Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Retention purge task count: {self.summary.get('retention_purge_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task data retention purge readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Steps | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_readiness_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_data_retention_purge_readiness_plan(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Build retention purge readiness records for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDataRetentionPurgeReadinessPlan(
        plan_id=plan_id,
        records=records,
        retention_purge_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_data_retention_purge_readiness(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Compatibility alias for building retention purge readiness records."""
    return build_task_data_retention_purge_readiness_plan(source)


def summarize_task_data_retention_purge_readiness(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Compatibility alias for building retention purge readiness records."""
    return build_task_data_retention_purge_readiness_plan(source)


def extract_task_data_retention_purge_readiness(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Compatibility alias for building retention purge readiness records."""
    return build_task_data_retention_purge_readiness_plan(source)


def generate_task_data_retention_purge_readiness(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Compatibility alias for generating retention purge readiness records."""
    return build_task_data_retention_purge_readiness_plan(source)


def recommend_task_data_retention_purge_readiness(source: Any) -> TaskDataRetentionPurgeReadinessPlan:
    """Compatibility alias for recommending retention purge safeguards."""
    return build_task_data_retention_purge_readiness_plan(source)


def task_data_retention_purge_readiness_plan_to_dict(
    result: TaskDataRetentionPurgeReadinessPlan,
) -> dict[str, Any]:
    """Serialize a retention purge readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_retention_purge_readiness_plan_to_dict.__test__ = False


def task_data_retention_purge_readiness_plan_to_dicts(
    result: TaskDataRetentionPurgeReadinessPlan | Iterable[TaskDataRetentionPurgeReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize retention purge readiness records to plain dictionaries."""
    if isinstance(result, TaskDataRetentionPurgeReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_retention_purge_readiness_plan_to_dicts.__test__ = False


def task_data_retention_purge_readiness_plan_to_markdown(
    result: TaskDataRetentionPurgeReadinessPlan,
) -> str:
    """Render a retention purge readiness plan as Markdown."""
    return result.to_markdown()


task_data_retention_purge_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[RetentionPurgeSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[RetentionPurgeSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDataRetentionPurgeReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None
    if _audit_log_retention_only(task):
        return None

    required = _required_safeguards(signals.signals)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDataRetentionPurgeReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        required_safeguards=required,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, missing),
        recommended_readiness_steps=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[RetentionPurgeSignal] = set()
    safeguard_hits: set[RetentionPurgeSafeguard] = set()
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

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_signal = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched_signal = True
        if matched_signal:
            signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[RetentionPurgeSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals = {
        signal
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items()
        if pattern.search(normalized) or pattern.search(text)
    }
    name = PurePosixPath(normalized).name
    if name in {"purge.py", "cleanup.py", "retention.py", "ttl.py", "expiration.py"}:
        signals.add("purge_cadence" if name in {"purge.py", "cleanup.py", "expiration.py"} else "retention_policy")
    return signals


def _required_safeguards(signals: tuple[RetentionPurgeSignal, ...]) -> tuple[RetentionPurgeSafeguard, ...]:
    signal_set = set(signals)
    required: set[RetentionPurgeSafeguard] = {
        "retention_policy",
        "purge_cadence",
        "legal_hold",
        "dry_run_backfill",
        "idempotency",
        "restore_window",
        "audit_evidence",
        "monitoring",
    }
    if "scheduled_deletion_job" not in signal_set:
        required.discard("monitoring")
    if not (signal_set & {"purge_cadence", "scheduled_deletion_job", "expired_records"}):
        required.discard("dry_run_backfill")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    signals: tuple[RetentionPurgeSignal, ...],
    missing: tuple[RetentionPurgeSafeguard, ...],
) -> RetentionPurgeRisk:
    if not missing:
        return "low"
    signal_set = set(signals)
    missing_set = set(missing)
    if missing_set & {"retention_policy", "legal_hold", "restore_window"}:
        return "high"
    if "scheduled_deletion_job" in signal_set and "monitoring" in missing_set:
        return "high"
    if len(missing) >= 4:
        return "high"
    if len(missing) >= 2 or "purge_cadence" in missing_set:
        return "medium"
    return "low"


def _audit_log_retention_only(task: Mapping[str, Any]) -> bool:
    texts = [text for _, text in _candidate_texts(task)]
    combined = " ".join(texts)
    if not re.search(r"\baudit logs?\b", combined, re.I):
        return False
    return not re.search(
        r"\b(?:purge|cleanup|clean up|prune|expire|expiration|expired|temporary|sessions?|tokens?|exports?|"
        r"scheduled deletion job|scheduled cleanup|scheduled purge|purge job|cleanup job)\b",
        combined,
        re.I,
    )


def _summary(
    records: tuple[TaskDataRetentionPurgeReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "retention_purge_task_count": len(records),
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
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
        "retention_purge_task_ids": [record.task_id for record in records],
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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
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
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "RetentionPurgeRisk",
    "RetentionPurgeSafeguard",
    "RetentionPurgeSignal",
    "TaskDataRetentionPurgeReadinessPlan",
    "TaskDataRetentionPurgeReadinessRecord",
    "analyze_task_data_retention_purge_readiness",
    "build_task_data_retention_purge_readiness_plan",
    "extract_task_data_retention_purge_readiness",
    "generate_task_data_retention_purge_readiness",
    "recommend_task_data_retention_purge_readiness",
    "summarize_task_data_retention_purge_readiness",
    "task_data_retention_purge_readiness_plan_to_dict",
    "task_data_retention_purge_readiness_plan_to_dicts",
    "task_data_retention_purge_readiness_plan_to_markdown",
]
