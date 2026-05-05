"""Plan database index and query performance safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar, cast

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DatabaseIndexImpactLevel = Literal["high", "medium", "low"]
DataAccessPattern = Literal[
    "migration",
    "schema_change",
    "index_change",
    "sql_query",
    "orm_model",
    "filtering",
    "sorting",
    "pagination",
    "search",
    "backfill",
    "dashboard",
    "high_volume_table",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_IMPACT_ORDER: dict[DatabaseIndexImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_PATTERN_ORDER: dict[DataAccessPattern, int] = {
    "migration": 0,
    "schema_change": 1,
    "index_change": 2,
    "sql_query": 3,
    "orm_model": 4,
    "filtering": 5,
    "sorting": 6,
    "pagination": 7,
    "search": 8,
    "backfill": 9,
    "dashboard": 10,
    "high_volume_table": 11,
}
_PATH_PATTERN_RE: tuple[tuple[DataAccessPattern, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|db/versions)(?:/|$)", re.I)),
    ("schema_change", re.compile(r"(?:^|/)(?:schema|schemas|ddl|database|db)(?:/|$)|\.(?:ddl)$", re.I)),
    ("index_change", re.compile(r"(?:^|/)(?:indexes?|indices)(?:/|$)|index", re.I)),
    ("sql_query", re.compile(r"\.sql$|(?:^|/)(?:sql|queries|query)(?:/|$)", re.I)),
    ("orm_model", re.compile(r"(?:^|/)(?:models?|entities|repositories|dao|orm)(?:/|$)|(?:model|repository|dao)", re.I)),
    ("filtering", re.compile(r"(?:filter|where|scope|queryset)", re.I)),
    ("sorting", re.compile(r"(?:sort|order_by|orderby|ordering)", re.I)),
    ("pagination", re.compile(r"(?:paginat|cursor|offset|limit)", re.I)),
    ("search", re.compile(r"(?:search|fulltext|full_text|fts|trigram|tsvector)", re.I)),
    ("backfill", re.compile(r"(?:backfill|data[-_]?migration|batch[-_]?job)", re.I)),
    ("dashboard", re.compile(r"(?:dashboard|report|analytics|metrics)", re.I)),
)
_TEXT_PATTERN_RE: tuple[tuple[DataAccessPattern, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"\b(?:migration|alembic|db migrate|database migration)\b", re.I)),
    ("schema_change", re.compile(r"\b(?:schema change|table|column|ddl|alter table|create table|drop table)\b", re.I)),
    ("index_change", re.compile(r"\b(?:index|indexes|indices|create index|drop index|unique index|composite index)\b", re.I)),
    (
        "sql_query",
        re.compile(
            r"\b(?:sql|sqlalchemy|select|join|where clause|group by|order by|cte|query plan)\b",
            re.I,
        ),
    ),
    ("orm_model", re.compile(r"\b(?:orm|model|repository|queryset|active record|sqlalchemy|django model|prisma)\b", re.I)),
    ("filtering", re.compile(r"\b(?:filter|filters|filtering|where|facet|faceted|scope by)\b", re.I)),
    ("sorting", re.compile(r"\b(?:sort|sorted|sorting|order by|order_by|ordering)\b", re.I)),
    ("pagination", re.compile(r"\b(?:pagination|paginate|cursor|offset|limit|page size|infinite scroll)\b", re.I)),
    ("search", re.compile(r"\b(?:search|full[- ]?text|fts|trigram|tsvector|autocomplete|similarity)\b", re.I)),
    ("backfill", re.compile(r"\b(?:backfill|data migration|batch update|batch job|reindex|rehydrate)\b", re.I)),
    ("dashboard", re.compile(r"\b(?:dashboard|reporting|analytics|metric|aggregate|chart|leaderboard)\b", re.I)),
    (
        "high_volume_table",
        re.compile(
            r"\b(?:high[- ]?volume|large table|hot table|production table|millions of rows|"
            r"billions of rows|events table|audit log|orders table|activity feed|multi[- ]?tenant)\b",
            re.I,
        ),
    ),
)
_HIGH_IMPACT_RE = re.compile(
    r"\b(?:concurrent index|lock|table lock|backfill|reindex|large table|high[- ]?volume|"
    r"millions of rows|production table|hot path|rollback|rollout|zero[- ]?downtime|"
    r"drop index|drop column|unique index)\b",
    re.I,
)
_LOW_ONLY_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskDatabaseIndexImpactRecord:
    """Database index and query performance guidance for one execution task."""

    task_id: str
    title: str
    impact_level: DatabaseIndexImpactLevel
    data_access_patterns: tuple[DataAccessPattern, ...] = field(default_factory=tuple)
    safeguards: tuple[str, ...] = field(default_factory=tuple)
    validation_commands_to_add: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impact_level": self.impact_level,
            "data_access_patterns": list(self.data_access_patterns),
            "safeguards": list(self.safeguards),
            "validation_commands_to_add": list(self.validation_commands_to_add),
            "evidence": list(self.evidence),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class TaskDatabaseIndexImpactPlan:
    """Plan-level database index and query performance review."""

    plan_id: str | None = None
    records: tuple[TaskDatabaseIndexImpactRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return database index impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the database index impact plan as deterministic Markdown."""
        title = "# Task Database Index Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were available for database index impact planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Data Access Patterns | Safeguards | Validation Commands |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.data_access_patterns) or 'none')} | "
                f"{_markdown_cell('; '.join(record.safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.validation_commands_to_add) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_database_index_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseIndexImpactPlan:
    """Build database index and query performance guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(
        record.task_id for record in records if record.data_access_patterns
    )
    impact_counts = {
        impact: sum(1 for record in records if record.impact_level == impact)
        for impact in _IMPACT_ORDER
    }
    pattern_counts = {
        pattern: sum(1 for record in records if pattern in record.data_access_patterns)
        for pattern in _PATTERN_ORDER
    }
    return TaskDatabaseIndexImpactPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        summary={
            "task_count": len(tasks),
            "impacted_task_count": len(impacted_task_ids),
            "impact_counts": impact_counts,
            "pattern_counts": pattern_counts,
        },
    )


def analyze_task_database_index_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseIndexImpactPlan:
    """Compatibility alias for building database index impact plans."""
    return build_task_database_index_impact_plan(source)


def summarize_task_database_index_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseIndexImpactPlan:
    """Compatibility alias for building database index impact plans."""
    return build_task_database_index_impact_plan(source)


def summarize_task_database_index_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseIndexImpactPlan:
    """Compatibility alias for building database index impact plans."""
    return build_task_database_index_impact_plan(source)


def task_database_index_impact_plan_to_dict(
    result: TaskDatabaseIndexImpactPlan,
) -> dict[str, Any]:
    """Serialize a database index impact plan to a plain dictionary."""
    return result.to_dict()


task_database_index_impact_plan_to_dict.__test__ = False


def task_database_index_impact_plan_to_markdown(
    result: TaskDatabaseIndexImpactPlan,
) -> str:
    """Render a database index impact plan as Markdown."""
    return result.to_markdown()


task_database_index_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    patterns: tuple[DataAccessPattern, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    high_impact_evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_command_evidence: tuple[str, ...] = field(default_factory=tuple)
    doc_or_test_only: bool = False

    @property
    def has_pattern(self) -> bool:
        return bool(self.patterns)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDatabaseIndexImpactRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    impact = _impact_level(signals)
    return TaskDatabaseIndexImpactRecord(
        task_id=task_id,
        title=title,
        impact_level=impact,
        data_access_patterns=signals.patterns,
        safeguards=_safeguards(signals, impact),
        validation_commands_to_add=_validation_commands_to_add(signals, impact),
        evidence=signals.evidence,
        follow_up_questions=_follow_up_questions(signals, impact),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    patterns: set[DataAccessPattern] = set()
    evidence: list[str] = []
    high_impact_evidence: list[str] = []
    validation_command_evidence: list[str] = []
    paths = _strings(task.get("files_or_modules") or task.get("files"))

    for path in paths:
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for pattern_name, pattern in _PATH_PATTERN_RE:
            if pattern.search(normalized) or pattern.search(path_text):
                patterns.add(pattern_name)
                evidence.append(path_evidence)
        if _HIGH_IMPACT_RE.search(path_text):
            high_impact_evidence.append(path_evidence)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for pattern_name, pattern in _TEXT_PATTERN_RE:
            if pattern.search(text):
                patterns.add(pattern_name)
                evidence.append(snippet)
        if _HIGH_IMPACT_RE.search(text):
            high_impact_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for pattern_name, pattern in (*_TEXT_PATTERN_RE, *_PATH_PATTERN_RE):
            if pattern.search(command) or pattern.search(command_text):
                patterns.add(pattern_name)
                evidence.append(snippet)
                validation_command_evidence.append(snippet)
        if _HIGH_IMPACT_RE.search(command) or _HIGH_IMPACT_RE.search(command_text):
            high_impact_evidence.append(snippet)

    ordered_patterns = tuple(pattern for pattern in _PATTERN_ORDER if pattern in patterns)
    return _Signals(
        patterns=ordered_patterns,
        evidence=tuple(_dedupe(evidence)),
        high_impact_evidence=tuple(_dedupe(high_impact_evidence)),
        validation_command_evidence=tuple(_dedupe(validation_command_evidence)),
        doc_or_test_only=_is_doc_or_test_only(paths),
    )


def _impact_level(signals: _Signals) -> DatabaseIndexImpactLevel:
    if not signals.has_pattern:
        return "low"
    if signals.doc_or_test_only and not signals.high_impact_evidence:
        return "low"

    patterns = set(signals.patterns)
    if signals.high_impact_evidence:
        return "high"
    if "backfill" in patterns or "high_volume_table" in patterns:
        return "high"
    if "index_change" in patterns and {"migration", "schema_change", "sql_query"} & patterns:
        return "high"
    if {"filtering", "sorting", "pagination", "search"} <= patterns:
        return "high"
    if "dashboard" in patterns and {"sql_query", "filtering", "sorting", "pagination", "search"} & patterns:
        return "high"
    if {"migration", "schema_change", "sql_query", "orm_model"} & patterns:
        return "medium"
    if len(patterns) >= 2:
        return "medium"
    return "low"


def _safeguards(signals: _Signals, impact: DatabaseIndexImpactLevel) -> tuple[str, ...]:
    if not signals.has_pattern:
        return ()

    safeguards: list[str] = [
        "Document expected query shape, cardinality, and affected table size before implementation.",
    ]
    if impact in ("medium", "high"):
        safeguards.extend(
            [
                "Review existing indexes and avoid redundant or unused index additions.",
                "Validate representative queries against production-like row counts before release.",
            ]
        )
    if impact == "high":
        safeguards.extend(
            [
                "Capture before-and-after EXPLAIN plans for changed SQL, ORM, search, filtering, sorting, and pagination queries.",
                "Use backfill safety controls: chunked batches, throttling, resumability, progress metrics, and lock-time limits.",
                "Roll out behind a feature flag or staged migration window with database latency and lock monitoring.",
                "Prepare rollback steps for migrations, index changes, query plans, and partially completed backfills.",
            ]
        )

    patterns = set(signals.patterns)
    if "migration" in patterns or "schema_change" in patterns:
        safeguards.append(
            "Separate expand, backfill, verify, and contract phases for schema and index changes."
        )
    if "index_change" in patterns:
        safeguards.append(
            "Prefer online or concurrent index creation where the database supports it, with explicit lock timeout settings."
        )
    if {"filtering", "sorting", "pagination", "search"} & patterns:
        safeguards.append(
            "Confirm composite index column order matches filter equality, sort order, pagination cursor, and search predicates."
        )
    if "dashboard" in patterns:
        safeguards.append(
            "Check dashboard query fan-out, aggregate cost, and cache strategy under expected refresh frequency."
        )
    if "high_volume_table" in patterns:
        safeguards.append(
            "Estimate write amplification, index storage growth, and maintenance overhead for high-volume tables."
        )
    return tuple(_dedupe(safeguards))


def _validation_commands_to_add(
    signals: _Signals,
    impact: DatabaseIndexImpactLevel,
) -> tuple[str, ...]:
    if not signals.has_pattern:
        return ()

    commands = [
        "Add query regression tests covering representative filters, sorts, pagination cursors, and search terms.",
    ]
    if impact in ("medium", "high"):
        commands.append(
            "Add migration dry-run or schema validation command against a production-like database snapshot."
        )
    if impact == "high":
        commands.extend(
            [
                "Add EXPLAIN plan capture for changed queries and compare row estimates, index usage, and sort strategy.",
                "Add backfill rehearsal command with chunk size, resume behavior, and lock timeout assertions.",
            ]
        )
    if signals.validation_command_evidence:
        commands.append(
            "Extend existing validation commands with database performance assertions rather than adding only functional checks."
        )
    return tuple(_dedupe(commands))


def _follow_up_questions(
    signals: _Signals,
    impact: DatabaseIndexImpactLevel,
) -> tuple[str, ...]:
    if not signals.has_pattern:
        return ()

    questions = [
        "Which table or query is expected to carry the highest row count or request rate?",
        "What production-like dataset will be used to validate the index and query plan?",
    ]
    if impact == "high":
        questions.extend(
            [
                "Can the migration or index build run online without blocking writes?",
                "What metric or threshold will trigger rollback during rollout?",
            ]
        )
    if "search" in signals.patterns:
        questions.append("Does search require full-text, trigram, vector, or conventional b-tree indexing?")
    return tuple(_dedupe(questions))


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
        # Type narrowing: source is an iterable at this point
        iterator = iter(cast(Iterable[object], source))
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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
                if any(pattern.search(key_text) for _, pattern in _TEXT_PATTERN_RE):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for _, pattern in _TEXT_PATTERN_RE):
                texts.append((field, str(key)))
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


def _is_doc_or_test_only(paths: list[str]) -> bool:
    if not paths:
        return False
    return all(_LOW_ONLY_PATH_RE.search(_normalized_path(path)) for path in paths)


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
    "DataAccessPattern",
    "DatabaseIndexImpactLevel",
    "TaskDatabaseIndexImpactPlan",
    "TaskDatabaseIndexImpactRecord",
    "analyze_task_database_index_impact",
    "build_task_database_index_impact_plan",
    "summarize_task_database_index_impact",
    "summarize_task_database_index_impacts",
    "task_database_index_impact_plan_to_dict",
    "task_database_index_impact_plan_to_markdown",
]
