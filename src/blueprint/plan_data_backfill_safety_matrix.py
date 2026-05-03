"""Build plan-level data backfill safety matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DataBackfillSafetyReadiness = Literal["ready", "partial", "blocked"]
DataBackfillSafetySeverity = Literal["high", "medium", "low"]

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[DataBackfillSafetyReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[DataBackfillSafetySeverity, int] = {"high": 0, "medium": 1, "low": 2}
_BACKFILL_RE = re.compile(
    r"\b(?:backfill|backfills|replay|replays|migration repair|repair migration|data repair|"
    r"repair data|resync|re-sync|reconcile|reconciliation|bulk correction|bulk correct|"
    r"bulk fix|bulk update|bulk repair|data fix|historical import|rerun import)\b",
    re.I,
)
_TARGET_RE = re.compile(
    r"\b(?:target dataset|dataset|table|collection|index|topic|bucket|warehouse|data store|"
    r"target|against|for)\b",
    re.I,
)
_SELECTION_RE = re.compile(
    r"\b(?:selection criteria|where clause|filter|scope|cohort|ids?|tenant|partition|"
    r"date range|since|until|created_at|updated_at|eligible|only rows?|query)\b",
    re.I,
)
_DRY_RUN_RE = re.compile(r"\b(?:dry[- ]?run|preview|sample run|no[- ]?op|read[- ]?only|simulate|simulation)\b", re.I)
_IDEMPOTENCY_RE = re.compile(
    r"\b(?:idempotent|idempotency|dedupe|de[- ]?dupe|upsert|natural key|checkpoint|resume|"
    r"rerunnable|retry safe|safe to retry|exactly once|skip existing)\b",
    re.I,
)
_RATE_LIMIT_RE = re.compile(
    r"\b(?:batch|batches|batch size|chunk|chunks|page size|rate limit|throttle|qps|rps|"
    r"limit|sleep|concurrency|parallelism|rows per|per minute|per second)\b",
    re.I,
)
_VERIFICATION_RE = re.compile(
    r"\b(?:verification|verify|validation|validate|query|checksum|row count|counts?|parity|"
    r"reconcile|diff|audit|spot check|metric|dashboard)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|compensation|compensating|revert|restore|undo|backup|snapshot|"
    r"delete inserted|reverse migration|backout)\b",
    re.I,
)
_OWNER_RE = re.compile(r"\b(?:owner|owners|dri|responsible|assignee|team|lead|operator|on[- ]?call|data steward)\b", re.I)
_EXPLICIT_GAP_RE = re.compile(r"\b(?:gap|unknown|unresolved|tbd|todo|not documented|not defined)\b", re.I)
_SURFACE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"\b(?:backfill|replay|resync|re-sync|repair|correction|reconciliation)\s+"
        r"(?:for|of|to|against)?\s*[`'\"]?([a-z0-9][\w./:-]{2,})",
        re.I,
    ),
    re.compile(
        r"\b(?:target dataset|dataset|table|collection|index|topic)\s+[`'\"]?([a-z0-9][\w./:-]{2,})",
        re.I,
    ),
    re.compile(
        r"\b[`'\"]?([a-z0-9][\w./:-]{2,})[`'\"]?\s+"
        r"(?:backfill|replay|resync|re-sync|migration repair|bulk correction|bulk repair)",
        re.I,
    ),
)


@dataclass(frozen=True, slots=True)
class PlanDataBackfillSafetyRow:
    """One grouped data backfill safety row."""

    target_dataset: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    selection_criteria: str = "missing"
    dry_run_plan: str = "missing"
    idempotency_strategy: str = "missing"
    batch_or_rate_limit: str = "missing"
    verification_query: str = "missing"
    rollback_or_compensation: str = "missing"
    owner: str = "missing"
    missing_fields: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendation: str = "Document missing backfill safety controls before execution."
    readiness: DataBackfillSafetyReadiness = "partial"
    severity: DataBackfillSafetySeverity = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "target_dataset": self.target_dataset,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "selection_criteria": self.selection_criteria,
            "dry_run_plan": self.dry_run_plan,
            "idempotency_strategy": self.idempotency_strategy,
            "batch_or_rate_limit": self.batch_or_rate_limit,
            "verification_query": self.verification_query,
            "rollback_or_compensation": self.rollback_or_compensation,
            "owner": self.owner,
            "missing_fields": list(self.missing_fields),
            "evidence": list(self.evidence),
            "recommendation": self.recommendation,
            "readiness": self.readiness,
            "severity": self.severity,
        }


@dataclass(frozen=True, slots=True)
class PlanDataBackfillSafetyMatrix:
    """Plan-level data backfill safety matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanDataBackfillSafetyRow, ...] = field(default_factory=tuple)
    backfill_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_backfill_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanDataBackfillSafetyRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "backfill_task_ids": list(self.backfill_task_ids),
            "no_backfill_task_ids": list(self.no_backfill_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return safety rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the data backfill safety matrix as deterministic Markdown."""
        title = "# Plan Data Backfill Safety Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('backfill_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require backfill safety review "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)}; "
                f"high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No data backfill safety rows were inferred."])
            if self.no_backfill_task_ids:
                lines.extend(["", f"No backfill signals: {_markdown_cell(', '.join(self.no_backfill_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Target Dataset | Tasks | Titles | Selection Criteria | Dry Run | Idempotency | "
                    "Batch/Rate Limit | Verification | Rollback/Compensation | Owner | "
                    "Readiness | Severity | Missing Fields | Evidence | Recommendation |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.target_dataset)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{_markdown_cell('; '.join(row.titles))} | "
                f"{row.selection_criteria} | {row.dry_run_plan} | {row.idempotency_strategy} | "
                f"{row.batch_or_rate_limit} | {row.verification_query} | "
                f"{row.rollback_or_compensation} | {row.owner} | {row.readiness} | {row.severity} | "
                f"{_markdown_cell('; '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell(row.recommendation)} |"
            )
        if self.no_backfill_task_ids:
            lines.extend(["", f"No backfill signals: {_markdown_cell(', '.join(self.no_backfill_task_ids))}"])
        return "\n".join(lines)


def build_plan_data_backfill_safety_matrix(source: Any) -> PlanDataBackfillSafetyMatrix:
    """Build grouped operational safety review for backfill-like execution tasks."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[str, list[_TaskBackfillSignals]] = {}
    no_backfill_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_backfill:
            no_backfill_task_ids.append(signals.task_id)
            continue
        grouped.setdefault(signals.target_dataset, []).append(signals)

    rows = tuple(sorted((_row_from_group(dataset, values) for dataset, values in grouped.items()), key=_row_sort_key))
    backfill_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return PlanDataBackfillSafetyMatrix(
        plan_id=plan_id,
        rows=rows,
        backfill_task_ids=backfill_task_ids,
        no_backfill_task_ids=tuple(no_backfill_task_ids),
        summary=_summary(len(tasks), rows, no_backfill_task_ids),
    )


def generate_plan_data_backfill_safety_matrix(source: Any) -> PlanDataBackfillSafetyMatrix:
    """Generate a data backfill safety matrix from a plan-like source."""
    return build_plan_data_backfill_safety_matrix(source)


def analyze_plan_data_backfill_safety_matrix(source: Any) -> PlanDataBackfillSafetyMatrix:
    """Analyze an execution plan for data backfill safety."""
    if isinstance(source, PlanDataBackfillSafetyMatrix):
        return source
    return build_plan_data_backfill_safety_matrix(source)


def derive_plan_data_backfill_safety_matrix(source: Any) -> PlanDataBackfillSafetyMatrix:
    """Derive a data backfill safety matrix from a plan-like source."""
    return analyze_plan_data_backfill_safety_matrix(source)


def extract_plan_data_backfill_safety_matrix(source: Any) -> PlanDataBackfillSafetyMatrix:
    """Extract a data backfill safety matrix from a plan-like source."""
    return derive_plan_data_backfill_safety_matrix(source)


def summarize_plan_data_backfill_safety_matrix(
    source: PlanDataBackfillSafetyMatrix | Iterable[PlanDataBackfillSafetyRow] | Any,
) -> dict[str, Any] | PlanDataBackfillSafetyMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanDataBackfillSafetyMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_data_backfill_safety_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_data_backfill_safety_matrix_to_dict(matrix: PlanDataBackfillSafetyMatrix) -> dict[str, Any]:
    """Serialize a data backfill safety matrix to a plain dictionary."""
    return matrix.to_dict()


plan_data_backfill_safety_matrix_to_dict.__test__ = False


def plan_data_backfill_safety_matrix_to_dicts(
    matrix: PlanDataBackfillSafetyMatrix | Iterable[PlanDataBackfillSafetyRow],
) -> list[dict[str, Any]]:
    """Serialize data backfill safety rows to plain dictionaries."""
    if isinstance(matrix, PlanDataBackfillSafetyMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_data_backfill_safety_matrix_to_dicts.__test__ = False


def plan_data_backfill_safety_matrix_to_markdown(matrix: PlanDataBackfillSafetyMatrix) -> str:
    """Render a data backfill safety matrix as Markdown."""
    return matrix.to_markdown()


plan_data_backfill_safety_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskBackfillSignals:
    task_id: str
    title: str
    target_dataset: str
    statuses: dict[str, str]
    missing_fields: tuple[str, ...]
    evidence: tuple[str, ...]
    has_backfill: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskBackfillSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    has_backfill = bool(_BACKFILL_RE.search(context) or _path_backfill_signal(texts))
    target_status = "present" if _target_dataset(texts) else "missing"
    statuses = {
        "target_dataset": target_status,
        "selection_criteria": _status(_SELECTION_RE, texts),
        "dry_run_plan": _status(_DRY_RUN_RE, texts),
        "idempotency_strategy": _status(_IDEMPOTENCY_RE, texts),
        "batch_or_rate_limit": _status(_RATE_LIMIT_RE, texts),
        "verification_query": _status(_VERIFICATION_RE, texts, skip_fields=("id",)),
        "rollback_or_compensation": _status(_ROLLBACK_RE, texts),
        "owner": _status(_OWNER_RE, texts, skip_fields=("id",)),
    }
    missing_fields = [
        field_name
        for field_name in (
            "target_dataset",
            "selection_criteria",
            "dry_run_plan",
            "idempotency_strategy",
            "batch_or_rate_limit",
            "verification_query",
            "rollback_or_compensation",
            "owner",
        )
        if statuses[field_name] == "missing"
    ]
    missing_fields.extend(
        _evidence_snippet(field, text)
        for field, text in texts
        if field != "id" and _EXPLICIT_GAP_RE.search(text)
    )
    return _TaskBackfillSignals(
        task_id=task_id,
        title=title,
        target_dataset=_target_dataset(texts) or "unspecified_target_dataset",
        statuses=statuses,
        missing_fields=tuple(_dedupe(missing_fields)),
        evidence=tuple(
            _dedupe(
                _evidence_snippet(field, text)
                for field, text in texts
                if _backfill_evidence_match(text) or _metadata_safety_field(field)
            )
        ),
        has_backfill=has_backfill,
    )


def _row_from_group(dataset: str, signals: list[_TaskBackfillSignals]) -> PlanDataBackfillSafetyRow:
    statuses = {
        field_name: "present" if any(signal.statuses[field_name] == "present" for signal in signals) else "missing"
        for field_name in (
            "selection_criteria",
            "dry_run_plan",
            "idempotency_strategy",
            "batch_or_rate_limit",
            "verification_query",
            "rollback_or_compensation",
            "owner",
        )
    }
    target_dataset_status = "missing" if dataset == "unspecified_target_dataset" else "present"
    missing_fields = tuple(
        _dedupe(
            field_name
            for signal in signals
            for field_name in signal.missing_fields
            if not _FIELD_NAME_RE.fullmatch(field_name)
            or (field_name == "target_dataset" and target_dataset_status == "missing")
            or statuses.get(field_name) == "missing"
        )
    )
    readiness = _readiness({**statuses, "target_dataset": target_dataset_status})
    return PlanDataBackfillSafetyRow(
        target_dataset=dataset,
        task_ids=tuple(_dedupe(signal.task_id for signal in signals)),
        titles=tuple(_dedupe(signal.title for signal in signals)),
        missing_fields=missing_fields,
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
        recommendation=_recommendation(missing_fields),
        readiness=readiness,
        severity=_severity(readiness),
        **statuses,
    )


_FIELD_NAME_RE = re.compile(
    r"target_dataset|selection_criteria|dry_run_plan|idempotency_strategy|batch_or_rate_limit|"
    r"verification_query|rollback_or_compensation|owner"
)


def _readiness(statuses: Mapping[str, str]) -> DataBackfillSafetyReadiness:
    if any(
        statuses[field_name] == "missing"
        for field_name in (
            "target_dataset",
            "selection_criteria",
            "idempotency_strategy",
            "verification_query",
            "rollback_or_compensation",
            "owner",
        )
    ):
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _severity(readiness: DataBackfillSafetyReadiness) -> DataBackfillSafetySeverity:
    return {"blocked": "high", "partial": "medium", "ready": "low"}[readiness]


def _recommendation(missing_fields: Iterable[str]) -> str:
    missing = tuple(field for field in missing_fields if _FIELD_NAME_RE.fullmatch(field))
    if not missing:
        return "Backfill safety controls are documented; execute through the approved runbook."
    labels = ", ".join(field.replace("_", " ") for field in missing)
    return f"Document {labels} before running the backfill."


def _summary(
    task_count: int,
    rows: Iterable[PlanDataBackfillSafetyRow],
    no_backfill_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    no_backfill_ids = tuple(no_backfill_task_ids)
    backfill_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "backfill_task_count": len(backfill_task_ids),
        "no_backfill_task_count": len(no_backfill_ids),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "missing_field_counts": {
            field_name: sum(1 for row in row_list if field_name in row.missing_fields)
            for field_name in sorted({field_name for row in row_list for field_name in row.missing_fields})
        },
        "dataset_counts": {
            dataset: sum(1 for row in row_list if row.target_dataset == dataset)
            for dataset in sorted({row.target_dataset for row in row_list})
        },
    }


def _row_sort_key(row: PlanDataBackfillSafetyRow) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.target_dataset,
        ",".join(row.task_ids),
    )


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _path_backfill_signal(texts: Iterable[tuple[str, str]]) -> bool:
    return any(
        field.startswith("files")
        and re.search(r"(?:^|/)(?:backfills?|replays?|repairs?|resyncs?|data_fixes?|bulk)(?:/|$)", text, re.I)
        for field, text in texts
    )


def _target_dataset(texts: Iterable[tuple[str, str]]) -> str | None:
    text_list = tuple(texts)
    for field, text in text_list:
        if field.startswith("metadata.") and field.lower().endswith(
            (
                "target_dataset",
                "dataset",
                "table",
                "collection",
                "index",
                "topic",
            )
        ):
            candidate = _normalise_surface(text.split(":", 1)[-1])
            if candidate:
                return candidate
    for field, text in text_list:
        for pattern in _SURFACE_PATTERNS:
            match = pattern.search(text)
            if match:
                candidate = _normalise_surface(match.group(1))
                if candidate and candidate not in {
                    "the",
                    "for",
                    "of",
                    "to",
                    "against",
                    "backfill",
                    "replay",
                    "resync",
                    "repair",
                    "bulk",
                    "data",
                    "migration",
                    "verification",
                    "validation",
                    "safety",
                    "plan",
                    "query",
                    "owner",
                    "dry_run",
                    "dry-run",
                    "idempotency",
                    "batch",
                    "rollback",
                    "compensation",
                }:
                    return candidate
        if field.startswith("files"):
            parts = [part for part in re.split(r"[/\\]", text) if part]
            for part in reversed(parts):
                if not re.search(r"\.(?:py|ts|tsx|js|md|sql|yaml|yml)$", part, re.I):
                    return _normalise_surface(part)
    return None


def _normalise_surface(value: str) -> str:
    text = _text(value).strip("`'\".,;:()[]{}")
    text = re.sub(r"[^a-zA-Z0-9./:-]+", "_", text)
    return text.strip("_").lower()


def _backfill_evidence_match(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _BACKFILL_RE,
            _TARGET_RE,
            _SELECTION_RE,
            _DRY_RUN_RE,
            _IDEMPOTENCY_RE,
            _RATE_LIMIT_RE,
            _VERIFICATION_RE,
            _ROLLBACK_RE,
            _OWNER_RE,
        )
    )


def _metadata_safety_field(field: str) -> bool:
    return field.lower() in {
        "metadata.target_dataset",
        "metadata.dataset",
        "metadata.table",
        "metadata.collection",
        "metadata.index",
        "metadata.topic",
        "metadata.selection_criteria",
        "metadata.dry_run_plan",
        "metadata.idempotency_strategy",
        "metadata.batch_or_rate_limit",
        "metadata.verification_query",
        "metadata.rollback_or_compensation",
        "metadata.owner",
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
    return None, _task_payloads(iterator)


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    if value is None:
        return tasks
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            payload = item.model_dump(mode="python")
            if isinstance(payload, Mapping):
                tasks.append(dict(payload))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for key in ("id", "title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        value = _optional_text(task.get(key))
        if value:
            texts.append((key, value))
    for key in ("depends_on", "dependencies", "files_or_modules", "acceptance_criteria", "tags", "validation_commands"):
        for idx, value in enumerate(_strings(task.get(key))):
            texts.append((f"{key}[{idx}]", value))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in sorted(metadata.items()):
            for idx, item in enumerate(_strings(value)):
                texts.append((f"metadata.{key}" if idx == 0 else f"metadata.{key}[{idx}]", item))
    return tuple(texts)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_text(value),) if _text(value) else ()
    if isinstance(value, Mapping):
        return tuple(_text(f"{key}: {item}") for key, item in value.items() if _text(item))
    if isinstance(value, Iterable):
        return tuple(_text(item) for item in value if _text(item))
    text = _text(value)
    return (text,) if text else ()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value).strip())


def _evidence_snippet(field: str, text: str) -> str:
    return f"{field}: {_text(text)[:220]}"


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def _object_payload(value: object) -> dict[str, Any]:
    return {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key))
    }


def _looks_like_plan(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )
