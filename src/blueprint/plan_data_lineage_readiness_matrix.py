"""Build plan-level data lineage readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


LineageReadiness = Literal["ready", "partial", "blocked"]
LineageSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[LineageReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[LineageSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_LINEAGE_TRIGGER_RE = re.compile(
    r"\b(?:analytics?|report(?:ing|s)?|dashboard|warehouse|etl|elt|pipeline|migration|"
    r"backfill|data model|dataset|metric|metrics|event stream|lineage|source[- ]to[- ]consumer|"
    r"transform(?:ation)?|sync|replication|audit trail|retention evidence)\b",
    re.I,
)
_SOURCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("billing_system", re.compile(r"\b(?:billing|invoice|invoices|payment|payments|stripe|revenue)\b", re.I)),
    ("customer_profile", re.compile(r"\b(?:customer profile|user profile|account profile|users?|customers?|accounts?)\b", re.I)),
    ("application_database", re.compile(r"\b(?:database|db|postgres|mysql|table|tables|records?|schema)\b", re.I)),
    ("event_stream", re.compile(r"\b(?:event stream|events?|kafka|kinesis|segment|tracking|clickstream)\b", re.I)),
    ("warehouse", re.compile(r"\b(?:warehouse|snowflake|bigquery|redshift|datamart|data mart)\b", re.I)),
    ("file_export", re.compile(r"\b(?:csv|export|exports|file drop|s3|bucket|parquet|spreadsheet)\b", re.I)),
    ("third_party_api", re.compile(r"\b(?:third[- ]party|vendor api|external api|salesforce|zendesk|hubspot)\b", re.I)),
    ("audit_log", re.compile(r"\b(?:audit log|audit trail|compliance log|immutable log)\b", re.I)),
)
_TRANSFORMATION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"\b(?:migration|migrate|cutover|schema change)\b", re.I)),
    ("backfill", re.compile(r"\b(?:backfill|historical load|replay)\b", re.I)),
    ("aggregation", re.compile(r"\b(?:aggregate|aggregation|rollup|metric|metrics|summary)\b", re.I)),
    ("sync", re.compile(r"\b(?:sync|replicate|replication|copy|publish)\b", re.I)),
    ("enrichment", re.compile(r"\b(?:join|enrich|enrichment|map|mapping|normalize|derive)\b", re.I)),
    ("redaction", re.compile(r"\b(?:redact|redaction|anonymize|anonymization|mask|pseudonymize)\b", re.I)),
    ("etl_pipeline", re.compile(r"\b(?:etl|elt|pipeline|transform|transformation|dataflow)\b", re.I)),
)
_CONSUMER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("reporting", re.compile(r"\b(?:reporting|report|reports|monthly report|executive report)\b", re.I)),
    ("analytics_dashboard", re.compile(r"\b(?:analytics dashboard|dashboard|bi|looker|mode|tableau)\b", re.I)),
    ("warehouse", re.compile(r"\b(?:warehouse|snowflake|bigquery|redshift|datamart|data mart)\b", re.I)),
    ("finance_operations", re.compile(r"\b(?:finance|revenue ops|billing ops|reconciliation|month[- ]end)\b", re.I)),
    ("support_operations", re.compile(r"\b(?:support|customer success|csm|helpdesk|zendesk)\b", re.I)),
    ("customer_export", re.compile(r"\b(?:customer export|download|csv export|data export)\b", re.I)),
    ("audit_consumer", re.compile(r"\b(?:audit|auditor|compliance|soc 2|sox|evidence review)\b", re.I)),
    ("ml_model", re.compile(r"\b(?:model|ml|machine learning|feature store|prediction)\b", re.I)),
)
_AUDIT_EVIDENCE_RE = re.compile(
    r"\b(?:retention|audit evidence|audit trail|audit log|lineage evidence|control evidence|"
    r"reconciliation|checksum|row count|data quality|validation query|traceability|source mapping|"
    r"consumer mapping|owner|dri|review)\b",
    re.I,
)
_EXPLICIT_GAP_RE = re.compile(
    r"\b(?:gap|missing|unknown|unresolved|tbd|todo|not documented|not defined|needs source|"
    r"source unclear|consumer unclear|lineage unclear)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanDataLineageReadinessRow:
    """One grouped source-to-consumer lineage readiness row."""

    source: str
    transformation: str
    consumer: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    retention_audit_evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: LineageReadiness = "partial"
    severity: LineageSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source": self.source,
            "transformation": self.transformation,
            "consumer": self.consumer,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "retention_audit_evidence": list(self.retention_audit_evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanDataLineageReadinessMatrix:
    """Plan-level data lineage readiness matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanDataLineageReadinessRow, ...] = field(default_factory=tuple)
    lineage_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_source_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_lineage_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanDataLineageReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "lineage_task_ids": list(self.lineage_task_ids),
            "missing_source_task_ids": list(self.missing_source_task_ids),
            "no_lineage_task_ids": list(self.no_lineage_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return lineage rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the lineage readiness matrix as deterministic Markdown."""
        title = "# Plan Data Lineage Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('lineage_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks need lineage readiness "
                f"(high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No data lineage readiness rows were inferred."])
            if self.no_lineage_task_ids:
                lines.extend(["", f"No lineage signals: {_markdown_cell(', '.join(self.no_lineage_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Source | Transformation | Consumer | Tasks | Readiness | Severity | Retention/Audit Evidence | Gaps | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.source)} | "
                f"{_markdown_cell(row.transformation)} | "
                f"{_markdown_cell(row.consumer)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{row.readiness} | "
                f"{row.severity} | "
                f"{_markdown_cell('; '.join(row.retention_audit_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.missing_source_task_ids:
            lines.extend(["", f"Missing source tasks: {_markdown_cell(', '.join(self.missing_source_task_ids))}"])
        if self.no_lineage_task_ids:
            lines.extend(["", f"No lineage signals: {_markdown_cell(', '.join(self.no_lineage_task_ids))}"])
        return "\n".join(lines)


def build_plan_data_lineage_readiness_matrix(source: Any) -> PlanDataLineageReadinessMatrix:
    """Build grouped data lineage readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[tuple[str, str, str], list[_TaskLineageSignals]] = {}
    no_lineage_task_ids: list[str] = []
    missing_source_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_lineage:
            no_lineage_task_ids.append(signals.task_id)
            continue
        if signals.source == "missing_source":
            missing_source_task_ids.append(signals.task_id)
        grouped.setdefault((signals.source, signals.transformation, signals.consumer), []).append(signals)

    rows = tuple(sorted((_row_from_group(key, values) for key, values in grouped.items()), key=_row_sort_key))
    lineage_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    missing_source_task_ids_tuple = tuple(task_id for task_id in lineage_task_ids if task_id in set(missing_source_task_ids))

    return PlanDataLineageReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        lineage_task_ids=lineage_task_ids,
        missing_source_task_ids=missing_source_task_ids_tuple,
        no_lineage_task_ids=tuple(no_lineage_task_ids),
        summary=_summary(len(tasks), rows, missing_source_task_ids_tuple, no_lineage_task_ids),
    )


def generate_plan_data_lineage_readiness_matrix(source: Any) -> PlanDataLineageReadinessMatrix:
    """Generate a data lineage readiness matrix from a plan-like source."""
    return build_plan_data_lineage_readiness_matrix(source)


def analyze_plan_data_lineage_readiness_matrix(source: Any) -> PlanDataLineageReadinessMatrix:
    """Analyze an execution plan for data lineage readiness."""
    if isinstance(source, PlanDataLineageReadinessMatrix):
        return source
    return build_plan_data_lineage_readiness_matrix(source)


def derive_plan_data_lineage_readiness_matrix(source: Any) -> PlanDataLineageReadinessMatrix:
    """Derive a data lineage readiness matrix from a plan-like source."""
    return analyze_plan_data_lineage_readiness_matrix(source)


def extract_plan_data_lineage_readiness_matrix(source: Any) -> PlanDataLineageReadinessMatrix:
    """Extract a data lineage readiness matrix from a plan-like source."""
    return derive_plan_data_lineage_readiness_matrix(source)


def summarize_plan_data_lineage_readiness_matrix(
    source: PlanDataLineageReadinessMatrix | Iterable[PlanDataLineageReadinessRow] | Any,
) -> dict[str, Any] | PlanDataLineageReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanDataLineageReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_data_lineage_readiness_matrix(source)
    rows = tuple(source)
    missing_source_task_ids = tuple(
        _dedupe(task_id for row in rows if row.source == "missing_source" for task_id in row.task_ids)
    )
    return _summary(len(rows), rows, missing_source_task_ids, ())


def plan_data_lineage_readiness_matrix_to_dict(
    matrix: PlanDataLineageReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a data lineage readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_data_lineage_readiness_matrix_to_dict.__test__ = False


def plan_data_lineage_readiness_matrix_to_dicts(
    matrix: PlanDataLineageReadinessMatrix | Iterable[PlanDataLineageReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize data lineage rows to plain dictionaries."""
    if isinstance(matrix, PlanDataLineageReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_data_lineage_readiness_matrix_to_dicts.__test__ = False


def plan_data_lineage_readiness_matrix_to_markdown(
    matrix: PlanDataLineageReadinessMatrix,
) -> str:
    """Render a data lineage readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_data_lineage_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskLineageSignals:
    task_id: str
    title: str
    source: str
    transformation: str
    consumer: str
    retention_audit_evidence: tuple[str, ...]
    gaps: tuple[str, ...]
    evidence: tuple[str, ...]
    has_lineage: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskLineageSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    has_lineage = bool(_LINEAGE_TRIGGER_RE.search(context))
    source, source_evidence = _first_match(_SOURCE_PATTERNS, texts)
    transformation, transformation_evidence = _first_match(_TRANSFORMATION_PATTERNS, texts)
    consumer, consumer_evidence = _first_match(_CONSUMER_PATTERNS, texts)
    retention_audit_evidence = tuple(
        _dedupe(_evidence_snippet(field, text) for field, text in texts if _AUDIT_EVIDENCE_RE.search(text))
    )
    explicit_gaps = tuple(
        _dedupe(_evidence_snippet(field, text) for field, text in texts if _EXPLICIT_GAP_RE.search(text))
    )
    lineage_evidence = _lineage_evidence(texts)

    gaps: list[str] = list(explicit_gaps)
    if has_lineage and not source:
        gaps.append("Missing upstream data source.")
    if has_lineage and not transformation:
        gaps.append("Missing transformation or mapping description.")
    if has_lineage and not consumer:
        gaps.append("Missing downstream consumer.")
    if has_lineage and not retention_audit_evidence:
        gaps.append("Missing retention or audit evidence.")

    return _TaskLineageSignals(
        task_id=task_id,
        title=title,
        source=source or "missing_source",
        transformation=transformation or "unspecified_transformation",
        consumer=consumer or "unspecified_consumer",
        retention_audit_evidence=retention_audit_evidence,
        gaps=tuple(_dedupe(gaps)),
        evidence=tuple(
            _dedupe(
                (
                    *source_evidence,
                    *transformation_evidence,
                    *consumer_evidence,
                    *lineage_evidence,
                )
            )
        ),
        has_lineage=has_lineage
        or bool(source and consumer)
        or bool(transformation and (source or consumer)),
    )


def _row_from_group(
    key: tuple[str, str, str],
    signals: list[_TaskLineageSignals],
) -> PlanDataLineageReadinessRow:
    source, transformation, consumer = key
    gaps = tuple(_dedupe(gap for signal in signals for gap in signal.gaps))
    retention_audit_evidence = tuple(
        _dedupe(item for signal in signals for item in signal.retention_audit_evidence)
    )
    readiness = _readiness(source, transformation, consumer, retention_audit_evidence, gaps)
    return PlanDataLineageReadinessRow(
        source=source,
        transformation=transformation,
        consumer=consumer,
        task_ids=tuple(_dedupe(signal.task_id for signal in sorted(signals, key=lambda item: item.task_id))),
        titles=tuple(_dedupe(signal.title for signal in sorted(signals, key=lambda item: item.task_id))),
        retention_audit_evidence=retention_audit_evidence,
        gaps=gaps,
        readiness=readiness,
        severity=_severity(source, transformation, consumer, retention_audit_evidence, gaps),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
    )


def _readiness(
    source: str,
    transformation: str,
    consumer: str,
    retention_audit_evidence: tuple[str, ...],
    gaps: tuple[str, ...],
) -> LineageReadiness:
    if source == "missing_source" or consumer == "unspecified_consumer":
        return "blocked"
    if transformation == "unspecified_transformation" or not retention_audit_evidence or gaps:
        return "partial"
    return "ready"


def _severity(
    source: str,
    transformation: str,
    consumer: str,
    retention_audit_evidence: tuple[str, ...],
    gaps: tuple[str, ...],
) -> LineageSeverity:
    if source == "missing_source" or consumer == "unspecified_consumer":
        return "high"
    if transformation == "unspecified_transformation" or not retention_audit_evidence or gaps:
        return "medium"
    return "low"


def _summary(
    task_count: int,
    rows: Iterable[PlanDataLineageReadinessRow],
    missing_source_task_ids: tuple[str, ...],
    no_lineage_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    lineage_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "lineage_task_count": len(lineage_task_ids),
        "missing_source_task_count": len(missing_source_task_ids),
        "no_lineage_task_count": len(tuple(no_lineage_task_ids)),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "source_counts": {
            source: sum(1 for row in row_list if row.source == source)
            for source in sorted({row.source for row in row_list})
        },
        "consumer_counts": {
            consumer: sum(1 for row in row_list if row.consumer == consumer)
            for consumer in sorted({row.consumer for row in row_list})
        },
    }


def _row_sort_key(row: PlanDataLineageReadinessRow) -> tuple[int, int, str, str, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.source,
        row.transformation,
        row.consumer,
        ",".join(row.task_ids),
    )


def _first_match(
    patterns: tuple[tuple[str, re.Pattern[str]], ...],
    texts: Iterable[tuple[str, str]],
) -> tuple[str | None, tuple[str, ...]]:
    evidence: list[str] = []
    for value, pattern in patterns:
        for source_field, text in texts:
            if pattern.search(text):
                evidence.append(_evidence_snippet(source_field, text))
                return value, tuple(_dedupe(evidence))
    return None, ()


def _lineage_evidence(texts: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    evidence: list[str] = []
    patterns = (
        _LINEAGE_TRIGGER_RE,
        _AUDIT_EVIDENCE_RE,
        *[pattern for _, pattern in _SOURCE_PATTERNS],
        *[pattern for _, pattern in _TRANSFORMATION_PATTERNS],
        *[pattern for _, pattern in _CONSUMER_PATTERNS],
    )
    for source_field, text in texts:
        if source_field.startswith(("depends_on", "dependencies")) or any(
            pattern.search(text) for pattern in patterns
        ):
            evidence.append(_evidence_snippet(source_field, text))
    return tuple(_dedupe(evidence))


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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "tags",
        "labels",
        "notes",
        "metadata",
        "blocked_reason",
        "tasks",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "dependencies", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
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


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "LineageReadiness",
    "LineageSeverity",
    "PlanDataLineageReadinessMatrix",
    "PlanDataLineageReadinessRow",
    "analyze_plan_data_lineage_readiness_matrix",
    "build_plan_data_lineage_readiness_matrix",
    "derive_plan_data_lineage_readiness_matrix",
    "extract_plan_data_lineage_readiness_matrix",
    "generate_plan_data_lineage_readiness_matrix",
    "plan_data_lineage_readiness_matrix_to_dict",
    "plan_data_lineage_readiness_matrix_to_dicts",
    "plan_data_lineage_readiness_matrix_to_markdown",
    "summarize_plan_data_lineage_readiness_matrix",
]
