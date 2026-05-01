"""Build plan-level schema migration readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MigrationSurface = Literal[
    "migration",
    "ddl",
    "index",
    "backfill",
    "generated_schema",
    "orm_model",
]
MigrationSafeguard = Literal[
    "reversible_migration_or_rollback_path",
    "expand_contract_compatibility",
    "backfill_validation",
    "lock_downtime_assessment",
    "deployment_ordering",
    "data_verification",
]
MigrationRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[MigrationSurface, int] = {
    "migration": 0,
    "ddl": 1,
    "index": 2,
    "backfill": 3,
    "generated_schema": 4,
    "orm_model": 5,
}
_SAFEGUARD_ORDER: tuple[MigrationSafeguard, ...] = (
    "reversible_migration_or_rollback_path",
    "expand_contract_compatibility",
    "backfill_validation",
    "lock_downtime_assessment",
    "deployment_ordering",
    "data_verification",
)
_RISK_ORDER: dict[MigrationRisk, int] = {"high": 0, "medium": 1, "low": 2}
_PATH_SURFACE_PATTERNS: tuple[tuple[MigrationSurface, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|db/versions|prisma/migrations)(?:/|$)", re.I)),
    ("ddl", re.compile(r"(?:^|/)(?:ddl|schema|schemas|database|db)(?:/|$)|\.(?:sql|ddl)$", re.I)),
    ("index", re.compile(r"(?:^|/)(?:indexes?|indices)(?:/|$)|index", re.I)),
    ("backfill", re.compile(r"(?:^|/)(?:backfills?|data[-_]?migrations?|batch[-_]?jobs?)(?:/|$)|backfill", re.I)),
    (
        "generated_schema",
        re.compile(
            r"(?:^|/)(?:generated|gen|schema[-_]?generated|graphql|openapi|proto)(?:/|$)|"
            r"(?:schema|types?)\.generated\.|(?:openapi|schema)\.(?:json|ya?ml)$|\.proto$",
            re.I,
        ),
    ),
    (
        "orm_model",
        re.compile(r"(?:^|/)(?:models?|entities|orm|prisma|sequelize|typeorm|sqlalchemy|repositories)(?:/|$)|model", re.I),
    ),
)
_TEXT_SURFACE_PATTERNS: tuple[tuple[MigrationSurface, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"\b(?:schema migration|database migration|db migration|alembic|migrate|migration)\b", re.I)),
    (
        "ddl",
        re.compile(
            r"\b(?:ddl|alter table|create table|drop table|rename column|drop column|add column|"
            r"not null|foreign key|primary key|table|column)\b",
            re.I,
        ),
    ),
    ("index", re.compile(r"\b(?:index|indexes|indices|create index|drop index|unique index|reindex)\b", re.I)),
    ("backfill", re.compile(r"\b(?:backfill|data migration|batch update|rehydrate|recompute existing)\b", re.I)),
    (
        "generated_schema",
        re.compile(r"\b(?:generated schema|schema snapshot|generated types|prisma generate|openapi|graphql schema|protobuf|proto)\b", re.I),
    ),
    (
        "orm_model",
        re.compile(r"\b(?:orm model|model change|sqlalchemy model|django model|prisma model|entity model|repository model)\b", re.I),
    ),
)
_SAFEGUARD_PATTERNS: dict[MigrationSafeguard, re.Pattern[str]] = {
    "reversible_migration_or_rollback_path": re.compile(
        r"\b(?:rollback|roll back|reversible|down migration|undo migration|restore|revert|point[- ]in[- ]time)\b",
        re.I,
    ),
    "expand_contract_compatibility": re.compile(
        r"\b(?:expand[/-]?contract|expand contract|expand and contract|backward[- ]compatible|forward[- ]compatible|"
        r"dual[- ]?(?:read|write)|old and new|compatibility window|nullable|defaulted|additive)\b",
        re.I,
    ),
    "backfill_validation": re.compile(
        r"\b(?:backfill.*(?:validat\w*|verify|reconcile|checksum|count)|(?:validat\w*|verify|reconcile|checksum).*backfill|"
        r"chunk(?:ed)?|batch(?:ed)?|resume|idempotent backfill)\b",
        re.I,
    ),
    "lock_downtime_assessment": re.compile(
        r"\b(?:lock|lock timeout|downtime|online migration|concurrent index|non[- ]blocking|"
        r"blocking writes|table rewrite|maintenance window)\b",
        re.I,
    ),
    "deployment_ordering": re.compile(
        r"\b(?:deployment order(?:ing)?|deploy(?:ment)? sequencing|release order(?:ing)?|rollout order(?:ing)?|"
        r"before app \w+|after app \w+|phases?|expand,? backfill,? verify,? contract|contract phase)\b",
        re.I,
    ),
    "data_verification": re.compile(
        r"\b(?:data verification|verify data|row count|record count|checksum|reconciliation|"
        r"integrity check|smoke test|read-after-write|migration verification)\b",
        re.I,
    ),
}
_HIGH_RISK_RE = re.compile(
    r"\b(?:drop|rename|not null|non[- ]nullable|unique index|foreign key|large table|production table|"
    r"high[- ]volume|millions of rows|table rewrite|lock|downtime|backfill|contract phase)\b",
    re.I,
)
_DOC_TEST_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanSchemaMigrationReadinessRecord:
    """Readiness guidance for one schema migration execution-plan task."""

    task_id: str
    title: str
    migration_surfaces: tuple[MigrationSurface, ...] = field(default_factory=tuple)
    required_safeguards: tuple[MigrationSafeguard, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[MigrationSafeguard, ...] = field(default_factory=tuple)
    rollout_notes: tuple[str, ...] = field(default_factory=tuple)
    risk_level: MigrationRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "migration_surfaces": list(self.migration_surfaces),
            "required_safeguards": list(self.required_safeguards),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "rollout_notes": list(self.rollout_notes),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanSchemaMigrationReadinessMatrix:
    """Plan-level schema migration readiness matrix and rollup counts."""

    plan_id: str | None = None
    records: tuple[PlanSchemaMigrationReadinessRecord, ...] = field(default_factory=tuple)
    schema_migration_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "schema_migration_task_ids": list(self.schema_migration_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the schema migration readiness matrix as deterministic Markdown."""
        title = "# Plan Schema Migration Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        surface_counts = self.summary.get("surface_counts", {})
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Schema migration task count: {self.summary.get('schema_migration_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Surface counts: "
            + ", ".join(f"{surface} {surface_counts.get(surface, 0)}" for surface in _SURFACE_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No schema migration tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Surfaces | Required Safeguards | Missing Acceptance Criteria | Rollout Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.migration_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.rollout_notes) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_schema_migration_readiness_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanSchemaMigrationReadinessMatrix:
    """Derive schema migration sequencing and rollback readiness from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index)) is not None
    ]
    records.sort(key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()))
    result = tuple(records)
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    surface_counts = {
        surface: sum(1 for record in result if surface in record.migration_surfaces)
        for surface in _SURFACE_ORDER
    }

    return PlanSchemaMigrationReadinessMatrix(
        plan_id=_optional_text(plan.get("id")),
        records=result,
        schema_migration_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "schema_migration_task_count": len(result),
            "missing_acceptance_criteria_count": sum(len(record.missing_acceptance_criteria) for record in result),
            "risk_counts": risk_counts,
            "surface_counts": surface_counts,
        },
    )


def summarize_plan_schema_migration_readiness(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanSchemaMigrationReadinessMatrix:
    """Compatibility alias for building schema migration readiness matrices."""
    return build_plan_schema_migration_readiness_matrix(source)


def plan_schema_migration_readiness_matrix_to_dict(
    matrix: PlanSchemaMigrationReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a schema migration readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_schema_migration_readiness_matrix_to_dict.__test__ = False


def plan_schema_migration_readiness_matrix_to_markdown(
    matrix: PlanSchemaMigrationReadinessMatrix,
) -> str:
    """Render a schema migration readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_schema_migration_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[MigrationSurface, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    high_risk_evidence: tuple[str, ...] = field(default_factory=tuple)
    doc_or_test_only: bool = False


def _record(task: Mapping[str, Any], index: int) -> PlanSchemaMigrationReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces or (signals.doc_or_test_only and not signals.high_risk_evidence):
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    criteria_context = _acceptance_context(task)
    required = _required_safeguards(signals.surfaces)
    missing = tuple(safeguard for safeguard in required if not _SAFEGUARD_PATTERNS[safeguard].search(criteria_context))
    risk = _risk_level(signals, missing)
    return PlanSchemaMigrationReadinessRecord(
        task_id=task_id,
        title=title,
        migration_surfaces=signals.surfaces,
        required_safeguards=required,
        missing_acceptance_criteria=missing,
        rollout_notes=_rollout_notes(signals.surfaces, missing),
        risk_level=risk,
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[MigrationSurface] = set()
    evidence: list[str] = []
    high_risk_evidence: list[str] = []
    paths = _strings(task.get("files_or_modules") or task.get("files"))

    for path in paths:
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for surface, pattern in _PATH_SURFACE_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
        if _HIGH_RISK_RE.search(path_text):
            high_risk_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        if _surface_signal_source(source_field):
            for surface, pattern in _TEXT_SURFACE_PATTERNS:
                if pattern.search(text):
                    surfaces.add(surface)
                    matched = True
        if matched:
            evidence.append(snippet)
        if _HIGH_RISK_RE.search(text):
            high_risk_evidence.append(snippet)

    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    return _Signals(
        surfaces=ordered_surfaces,
        evidence=tuple(_dedupe(evidence)),
        high_risk_evidence=tuple(_dedupe(high_risk_evidence)),
        doc_or_test_only=_is_doc_or_test_only(paths),
    )


def _required_safeguards(surfaces: tuple[MigrationSurface, ...]) -> tuple[MigrationSafeguard, ...]:
    if not surfaces:
        return ()
    return _SAFEGUARD_ORDER


def _risk_level(signals: _Signals, missing: tuple[MigrationSafeguard, ...]) -> MigrationRisk:
    if signals.high_risk_evidence and missing:
        return "high"
    if len(missing) >= 4:
        return "high"
    if "backfill" in signals.surfaces and "backfill_validation" in missing:
        return "high"
    if {"ddl", "index"} <= set(signals.surfaces) and "lock_downtime_assessment" in missing:
        return "high"
    if missing:
        return "medium"
    return "low"


def _rollout_notes(
    surfaces: tuple[MigrationSurface, ...],
    missing: tuple[MigrationSafeguard, ...],
) -> tuple[str, ...]:
    notes = [
        "Sequence changes as expand, backfill, verify, deploy readers/writers, then contract.",
        "Keep old and new schema versions compatible until rollback and verification windows close.",
    ]
    surface_set = set(surfaces)
    if "index" in surface_set:
        notes.append("Use online or concurrent index operations where supported and set explicit lock timeouts.")
    if "backfill" in surface_set:
        notes.append("Run backfills in resumable chunks with reconciliation before contract cleanup.")
    if "generated_schema" in surface_set or "orm_model" in surface_set:
        notes.append("Regenerate clients or ORM artifacts after expand migrations and before application rollout.")
    if missing:
        notes.append("Add acceptance criteria for missing migration safeguards before agents modify persistence code.")
    return tuple(_dedupe(notes))


def _acceptance_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in ("acceptance_criteria", "criteria"):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(keyword in normalized for keyword in ("acceptance", "criteria", "safeguard", "rollout", "rollback", "verification")):
                values.append(text)
    return " ".join(values)


def _surface_signal_source(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    if normalized.startswith("acceptance_criteria") or normalized.startswith("criteria"):
        return False
    if "safeguard" in normalized or "acceptance" in normalized or "criteria" in normalized:
        return False
    return True


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "depends_on", "dependencies"):
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for _, pattern in _TEXT_SURFACE_PATTERNS):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for _, pattern in _TEXT_SURFACE_PATTERNS):
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


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if isinstance(plan, ExecutionPlan):
        return dict(plan.model_dump(mode="python"))
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
        if isinstance(item, ExecutionTask):
            tasks.append(dict(item.model_dump(mode="python")))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    return all(_DOC_TEST_PATH_RE.search(_normalized_path(path)) for path in paths)


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
    "MigrationRisk",
    "MigrationSafeguard",
    "MigrationSurface",
    "PlanSchemaMigrationReadinessMatrix",
    "PlanSchemaMigrationReadinessRecord",
    "build_plan_schema_migration_readiness_matrix",
    "plan_schema_migration_readiness_matrix_to_dict",
    "plan_schema_migration_readiness_matrix_to_markdown",
    "summarize_plan_schema_migration_readiness",
]
