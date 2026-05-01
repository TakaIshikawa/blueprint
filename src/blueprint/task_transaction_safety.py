"""Plan transaction-safety safeguards for database execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


TransactionRisk = Literal[
    "transaction",
    "row_lock",
    "isolation_level",
    "deadlock",
    "consistency",
    "rollback",
    "bulk_write",
    "idempotent_write",
    "read_only_database",
]
TransactionWriteProfile = Literal["high_risk_write", "write", "read_only"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[TransactionRisk, int] = {
    "transaction": 0,
    "row_lock": 1,
    "isolation_level": 2,
    "deadlock": 3,
    "consistency": 4,
    "rollback": 5,
    "bulk_write": 6,
    "idempotent_write": 7,
    "read_only_database": 8,
}
_PROFILE_ORDER: dict[TransactionWriteProfile, int] = {
    "high_risk_write": 0,
    "write": 1,
    "read_only": 2,
}
_TEXT_RISK_PATTERNS: dict[TransactionRisk, re.Pattern[str]] = {
    "transaction": re.compile(
        r"\b(?:transaction|transactional|atomic|commit|unit of work|savepoint|two[- ]phase commit|2pc)\b",
        re.I,
    ),
    "row_lock": re.compile(
        r"\b(?:row lock|row[- ]level lock|select for update|for update|advisory lock|lock wait|"
        r"pessimistic lock|optimistic lock|skip locked)\b",
        re.I,
    ),
    "isolation_level": re.compile(
        r"\b(?:isolation level|read committed|repeatable read|serializable|snapshot isolation|"
        r"phantom read|dirty read|non[- ]repeatable read)\b",
        re.I,
    ),
    "deadlock": re.compile(r"\b(?:deadlock|dead[- ]lock|lock timeout|lock ordering|cycle wait)\b", re.I),
    "consistency": re.compile(
        r"\b(?:consistency|consistent|invariant|referential integrity|constraint|race condition|"
        r"lost update|write skew|read your writes|eventual consistency)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|revert writes?|undo writes?|compensating transaction|"
        r"restore point|abort transaction)\b",
        re.I,
    ),
    "bulk_write": re.compile(
        r"\b(?:bulk write|bulk update|bulk delete|bulk insert|batch update|batch delete|backfill|"
        r"mass update|mass delete|data correction|write migration|update all|delete all)\b",
        re.I,
    ),
    "idempotent_write": re.compile(
        r"\b(?:idempotent write|idempotency|idempotency key|upsert|retry[- ]safe write|"
        r"safe to retry|dedupe key|deduplicate write)\b",
        re.I,
    ),
    "read_only_database": re.compile(
        r"\b(?:read[- ]only|select query|database read|db read|query|report|reader|replica)\b",
        re.I,
    ),
}
_DATABASE_RE = re.compile(
    r"\b(?:database|db|sql|postgres|postgresql|mysql|sqlite|transaction|table|row|record|"
    r"repository|orm|query|persistence|persisted|storage|datastore)\b",
    re.I,
)
_WRITE_RE = re.compile(
    r"\b(?:write|writes|insert|update|delete|upsert|mutate|persist|save|commit|bulk|batch|"
    r"backfill|migration|migrate|lock|transaction|rollback|retry[- ]safe|idempotent)\b",
    re.I,
)
_READ_ONLY_RE = re.compile(r"\b(?:read[- ]only|select|query|fetch|list|report|replica|reader)\b", re.I)
_VALIDATION_DB_RE = re.compile(
    r"\b(?:database|db|sql|postgres|postgresql|mysql|sqlite|transaction|transactions?|"
    r"deadlock|lock|isolation|consistency|rollback|persistence|repository)\b|"
    r"(?:tests?/[^ ]*(?:db|database|transaction|persistence|repository|sql)[^ ]*)",
    re.I,
)
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only|storybook)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskTransactionSafetyPlan:
    """Transaction-safety guidance for one affected execution task."""

    task_id: str
    title: str
    transaction_risks: tuple[TransactionRisk, ...] = field(default_factory=tuple)
    write_profile: TransactionWriteProfile = "write"
    required_safeguards: tuple[str, ...] = field(default_factory=tuple)
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)
    stop_conditions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "transaction_risks": list(self.transaction_risks),
            "write_profile": self.write_profile,
            "required_safeguards": list(self.required_safeguards),
            "validation_evidence": list(self.validation_evidence),
            "stop_conditions": list(self.stop_conditions),
            "evidence": list(self.evidence),
        }


def generate_task_transaction_safety_plans(
    plan: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Any] | object | None,
) -> list[TaskTransactionSafetyPlan]:
    """Return transaction-safety plans for database-sensitive execution tasks."""
    tasks = _source_tasks(plan)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record_for_task(task, index)) is not None
    ]
    return sorted(
        records,
        key=lambda record: (
            _PROFILE_ORDER[record.write_profile],
            record.task_id,
            record.title.casefold(),
        ),
    )


def task_transaction_safety_plans_to_dicts(
    plans: tuple[TaskTransactionSafetyPlan, ...] | list[TaskTransactionSafetyPlan],
) -> list[dict[str, Any]]:
    """Serialize task transaction-safety plans to dictionaries."""
    return [plan.to_dict() for plan in plans]


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskTransactionSafetyPlan | None:
    signals = _signals(task)
    risks = signals.risks
    if not risks or _only_low_signal_documentation(signals.texts):
        return None
    profile = _write_profile(risks, signals.write_signal, signals.read_only_signal)
    return TaskTransactionSafetyPlan(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or f"Task {index}",
        transaction_risks=risks,
        write_profile=profile,
        required_safeguards=_required_safeguards(risks, profile),
        validation_evidence=_validation_evidence(signals.validation_commands, risks, profile),
        stop_conditions=_stop_conditions(risks, profile),
        evidence=signals.evidence,
    )


@dataclass(frozen=True, slots=True)
class _Signals:
    risks: tuple[TransactionRisk, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)
    texts: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    write_signal: bool = False
    read_only_signal: bool = False


def _signals(task: Mapping[str, Any]) -> _Signals:
    risks: set[TransactionRisk] = set()
    evidence: list[str] = []
    texts = tuple(_candidate_texts(task))

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        path_risks = _path_risks(path)
        if path_risks:
            risks.update(path_risks)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in texts:
        matched = False
        for risk, pattern in _TEXT_RISK_PATTERNS.items():
            if pattern.search(text):
                risks.add(risk)
                matched = True
        if _DATABASE_RE.search(text) and _READ_ONLY_RE.search(text) and not _WRITE_RE.search(text):
            risks.add("read_only_database")
            matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = bool(_VALIDATION_DB_RE.search(command) or _VALIDATION_DB_RE.search(command_text))
        for risk, pattern in _TEXT_RISK_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                risks.add(risk)
                matched = True
        if matched:
            evidence.append(_evidence_snippet("validation_commands", command))

    combined = " ".join(text for _, text in texts)
    if risks == {"read_only_database"} and _WRITE_RE.search(combined):
        risks.discard("read_only_database")
    return _Signals(
        risks=tuple(risk for risk in _RISK_ORDER if risk in risks),
        evidence=tuple(_dedupe(evidence)),
        validation_commands=validation_commands,
        texts=texts,
        write_signal=bool(_WRITE_RE.search(combined)),
        read_only_signal=bool(_READ_ONLY_RE.search(combined)),
    )


def _path_risks(path: str) -> set[TransactionRisk]:
    normalized = path.replace("\\", "/").casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    risks: set[TransactionRisk] = set()
    if {"db", "database", "databases", "repositories", "repository", "models", "store", "storage"} & parts:
        if any(token in text for token in ("read", "query", "select", "report", "replica")):
            risks.add("read_only_database")
        if any(token in text for token in ("transaction", "unit of work")):
            risks.add("transaction")
        if any(token in text for token in ("lock", "for update")):
            risks.add("row_lock")
        if any(token in text for token in ("bulk", "batch", "backfill", "migration")):
            risks.add("bulk_write")
    if normalized.endswith(".sql") and any(token in text for token in ("transaction", "lock", "rollback", "bulk")):
        for risk, pattern in _TEXT_RISK_PATTERNS.items():
            if pattern.search(text):
                risks.add(risk)
    return risks


def _write_profile(
    risks: tuple[TransactionRisk, ...],
    write_signal: bool,
    read_only_signal: bool,
) -> TransactionWriteProfile:
    if "bulk_write" in risks or any(
        risk in risks for risk in ("row_lock", "isolation_level", "deadlock", "rollback", "idempotent_write")
    ):
        return "high_risk_write"
    if "transaction" in risks and write_signal:
        return "high_risk_write"
    if write_signal:
        return "write"
    if risks == ("read_only_database",) or ("read_only_database" in risks and read_only_signal):
        return "read_only"
    return "write"


def _required_safeguards(
    risks: tuple[TransactionRisk, ...],
    profile: TransactionWriteProfile,
) -> tuple[str, ...]:
    safeguards: list[str] = []
    if profile == "read_only":
        safeguards.extend(
            [
                "Confirm the task remains read-only in code review and cannot emit writes, locks, or side effects.",
                "Validate the query uses an acceptable isolation level or replica behavior for required consistency.",
            ]
        )
        return tuple(safeguards)

    safeguards.extend(
        [
            "Document the transaction boundary, commit point, and failure behavior for every affected write path.",
            "Add tests for partial failure so persisted state remains consistent after exceptions or retries.",
        ]
    )
    if profile == "high_risk_write":
        safeguards.extend(
            [
                "Dry-run or scope-limit the write before full execution, with row counts reviewed before commit.",
                "Define lock ordering, lock timeout, and retry/backoff behavior for contention and deadlocks.",
                "Provide rollback, compensation, or restore steps for data written before a failure is detected.",
            ]
        )
    if "isolation_level" in risks:
        safeguards.append("State the required isolation level and prove it prevents the named anomaly.")
    if "idempotent_write" in risks:
        safeguards.append("Use an idempotency key, upsert guard, or dedupe constraint for retry-safe writes.")
    if "consistency" in risks:
        safeguards.append("Assert the consistency invariants before and after the transaction completes.")
    return tuple(_dedupe(safeguards))


def _validation_evidence(
    validation_commands: tuple[str, ...],
    risks: tuple[TransactionRisk, ...],
    profile: TransactionWriteProfile,
) -> tuple[str, ...]:
    requirements: list[str] = []
    db_commands = [
        command
        for command in validation_commands
        if _VALIDATION_DB_RE.search(command) or _VALIDATION_DB_RE.search(command.replace("/", " "))
    ]
    for command in db_commands:
        requirements.append(f"Run database transaction validation command: {command}")
    if not db_commands:
        requirements.append("Add or identify focused database tests covering transaction safety.")
    if profile != "read_only":
        requirements.append("Capture before/after row counts or invariant checks for changed persisted data.")
    if any(risk in risks for risk in ("row_lock", "deadlock", "isolation_level")):
        requirements.append("Include contention or concurrent execution coverage for locking and isolation behavior.")
    return tuple(_dedupe(requirements))


def _stop_conditions(
    risks: tuple[TransactionRisk, ...],
    profile: TransactionWriteProfile,
) -> tuple[str, ...]:
    conditions = [
        "Stop if validation shows inconsistent persisted state, failed invariants, or unexpected row counts.",
    ]
    if profile != "read_only":
        conditions.append("Stop if rollback or compensation cannot restore affected data within the approved window.")
    if profile == "high_risk_write":
        conditions.append("Stop if dry-run volume, lock duration, or write amplification exceeds the reviewed limit.")
    if any(risk in risks for risk in ("row_lock", "deadlock", "isolation_level")):
        conditions.append("Stop on deadlock spikes, lock waits, lock timeouts, or isolation anomalies.")
    if "read_only_database" in risks and profile == "read_only":
        conditions.append("Stop if the implementation introduces writes, row locks, or transaction-side effects.")
    return tuple(_dedupe(conditions))


def _source_tasks(source: Any) -> list[dict[str, Any]]:
    if source is None or isinstance(source, (str, bytes)):
        return []
    if isinstance(source, ExecutionTask):
        return [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _task_payloads(source.model_dump(mode="python").get("tasks"))
    if isinstance(source, Mapping):
        if "tasks" in source:
            return _task_payloads(_plan_payload(source).get("tasks"))
        return [dict(source)]
    if hasattr(source, "model_dump"):
        payload = source.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _source_tasks(payload)
    try:
        iterator = iter(source)
    except TypeError:
        return []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


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
    return {}


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
    for field_name in ("acceptance_criteria", "depends_on", "tags", "labels", "risks"):
        for text in _strings(task.get(field_name)):
            texts.append((field_name, text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    commands.extend(_strings(task.get("validation_commands")))
    commands.extend(_strings(task.get("test_command")))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        raw_commands = metadata.get("validation_commands")
        if isinstance(raw_commands, Mapping):
            commands.extend(flatten_validation_commands(raw_commands))
        else:
            commands.extend(_strings(raw_commands))
    return _dedupe(commands)


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _SPACE_RE.sub(" ", text).strip()
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _only_low_signal_documentation(text_items: tuple[tuple[str, str], ...]) -> bool:
    combined = " ".join(text for _, text in text_items)
    return bool(_NEGATIVE_RE.search(combined)) and not re.search(
        r"\b(?:implement|add|update|delete|insert|write|transaction|lock|rollback|isolation|"
        r"deadlock|consistency|database|db|sql|persist)\b",
        combined,
        re.I,
    )


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


__all__ = [
    "TaskTransactionSafetyPlan",
    "TransactionRisk",
    "TransactionWriteProfile",
    "generate_task_transaction_safety_plans",
    "task_transaction_safety_plans_to_dicts",
]
