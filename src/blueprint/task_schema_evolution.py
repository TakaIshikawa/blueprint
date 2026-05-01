"""Plan schema evolution guardrails for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SchemaEvolutionRisk = Literal[
    "additive_safe",
    "compatibility_guarded",
    "breaking_change_risk",
    "not_schema_related",
]
SchemaSurface = Literal[
    "database",
    "api_schema",
    "graphql",
    "openapi",
    "protobuf",
    "event_payload",
    "config_schema",
    "serialization_model",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[SchemaSurface, int] = {
    "database": 0,
    "api_schema": 1,
    "graphql": 2,
    "openapi": 3,
    "protobuf": 4,
    "event_payload": 5,
    "config_schema": 6,
    "serialization_model": 7,
}
_RISK_ORDER: dict[SchemaEvolutionRisk, int] = {
    "breaking_change_risk": 0,
    "compatibility_guarded": 1,
    "additive_safe": 2,
    "not_schema_related": 3,
}
_PATH_SURFACE_PATTERNS: tuple[tuple[SchemaSurface, re.Pattern[str]], ...] = (
    (
        "database",
        re.compile(
            r"(?:^|/)(?:db|database|migrations?|alembic|ddl)(?:/|$)|"
            r"(?:^|/)(?:db|database)/(?:schema|schemas)(?:/|$)|"
            r"\.(?:sql|ddl)$",
            re.I,
        ),
    ),
    (
        "api_schema",
        re.compile(
            r"(?:^|/)(?:api|apis|contracts?|schemas?|specs?)(?:/|$)|"
            r"(?:request|response|dto|contract)\.(?:json|ya?ml|ts|py|go|java)$",
            re.I,
        ),
    ),
    ("graphql", re.compile(r"(?:^|/)(?:graphql|gql)(?:/|$)|\.(?:graphql|gql)$", re.I)),
    (
        "openapi",
        re.compile(r"(?:^|/)(?:openapi|swagger)(?:/|$)|openapi\.(?:json|ya?ml)$|swagger", re.I),
    ),
    ("protobuf", re.compile(r"(?:^|/)(?:proto|protos|protobuf)(?:/|$)|\.proto$", re.I)),
    (
        "event_payload",
        re.compile(
            r"(?:^|/)(?:events?|payloads?|messages?|topics?|schemas?/events?)(?:/|$)|"
            r"(?:event|payload|message)[._-]?schema",
            re.I,
        ),
    ),
    (
        "config_schema",
        re.compile(
            r"(?:^|/)(?:config|configs|configuration|settings)(?:/|$).*(?:schema|spec)|"
            r"(?:config|settings)[._-]?schema",
            re.I,
        ),
    ),
    (
        "serialization_model",
        re.compile(
            r"(?:^|/)(?:models?|serializers?|dto|entities)(?:/|$)|"
            r"(?:model|serializer|dto|entity|pydantic|dataclass)",
            re.I,
        ),
    ),
)
_TEXT_SURFACE_PATTERNS: tuple[tuple[SchemaSurface, re.Pattern[str]], ...] = (
    (
        "database",
        re.compile(r"\b(?:database|table|column|migration|ddl|alembic|sql schema|index)\b", re.I),
    ),
    (
        "api_schema",
        re.compile(r"\b(?:api contract|api schema|request schema|response schema|json schema|dto)\b", re.I),
    ),
    ("graphql", re.compile(r"\b(?:graphql|gql|resolver schema)\b", re.I)),
    ("openapi", re.compile(r"\b(?:openapi|swagger)\b", re.I)),
    ("protobuf", re.compile(r"\b(?:protobuf|proto3?|\.proto|grpc)\b", re.I)),
    (
        "event_payload",
        re.compile(r"\b(?:event payload|event schema|message schema|topic payload|webhook payload)\b", re.I),
    ),
    ("config_schema", re.compile(r"\b(?:config schema|settings schema|configuration schema)\b", re.I)),
    (
        "serialization_model",
        re.compile(r"\b(?:serialization model|serializer|pydantic model|dataclass|domain model|enum)\b", re.I),
    ),
)
_ADDITIVE_RE = re.compile(
    r"\b(?:add|adds|adding|additive|new|append|optional|nullable|defaulted|backward[- ]?compatible|"
    r"non[- ]?breaking|extend|extension|extra field|new column|add column|add field|add enum value)\b",
    re.I,
)
_BREAKING_RE = re.compile(
    r"\b(?:breaking|remove|removes|removed|delete|drop|dropped|rename|renamed|replace|change type|"
    r"type change|required field|make required|non[- ]?nullable|not null|contract break|incompatible|"
    r"enum removal|remove enum|rename enum|field rename|column rename|drop column|drop field)\b",
    re.I,
)
_COMPATIBILITY_RE = re.compile(
    r"\b(?:compatibility|backward compatibility|forward compatibility|deprecat\w*|versioned|"
    r"dual[- ]?(?:read|write)|backfill|fixture|consumer contract|contract test|rollback compatible)\b",
    re.I,
)
_ENUM_RE = re.compile(r"\benum(?:eration)?s?\b", re.I)
_DOC_TEST_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|spec|specs|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskSchemaEvolutionRecord:
    """Schema evolution guardrail guidance for one execution task."""

    task_id: str
    title: str
    evolution_risk: SchemaEvolutionRisk
    schema_surfaces: tuple[SchemaSurface, ...] = field(default_factory=tuple)
    change_indicators: tuple[str, ...] = field(default_factory=tuple)
    guardrails: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "evolution_risk": self.evolution_risk,
            "schema_surfaces": list(self.schema_surfaces),
            "change_indicators": list(self.change_indicators),
            "guardrails": list(self.guardrails),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSchemaEvolutionPlan:
    """Plan-level schema evolution guardrail review."""

    plan_id: str | None = None
    records: tuple[TaskSchemaEvolutionRecord, ...] = field(default_factory=tuple)
    schema_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "schema_task_ids": list(self.schema_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return schema evolution records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the schema evolution plan as deterministic Markdown."""
        title = "# Task Schema Evolution Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were available for schema evolution planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Surfaces | Guardrails |",
                "| --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.evolution_risk} | "
                f"{_markdown_cell(', '.join(record.schema_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(record.guardrails) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_schema_evolution_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskSchemaEvolutionPlan:
    """Build schema evolution guardrail guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _RISK_ORDER[record.evolution_risk],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    schema_task_ids = tuple(
        record.task_id
        for record in records
        if record.evolution_risk != "not_schema_related"
    )
    risk_counts = {
        risk: sum(1 for record in records if record.evolution_risk == risk)
        for risk in _RISK_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in records if surface in record.schema_surfaces)
        for surface in _SURFACE_ORDER
    }
    return TaskSchemaEvolutionPlan(
        plan_id=plan_id,
        records=records,
        schema_task_ids=schema_task_ids,
        summary={
            "task_count": len(tasks),
            "schema_task_count": len(schema_task_ids),
            "risk_counts": risk_counts,
            "surface_counts": surface_counts,
        },
    )


def analyze_task_schema_evolution(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskSchemaEvolutionPlan:
    """Compatibility alias for building schema evolution guardrail plans."""
    return build_task_schema_evolution_plan(source)


def summarize_task_schema_evolution(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskSchemaEvolutionPlan:
    """Compatibility alias for building schema evolution guardrail plans."""
    return build_task_schema_evolution_plan(source)


def task_schema_evolution_plan_to_dict(result: TaskSchemaEvolutionPlan) -> dict[str, Any]:
    """Serialize a task schema evolution plan to a plain dictionary."""
    return result.to_dict()


task_schema_evolution_plan_to_dict.__test__ = False


def task_schema_evolution_plan_to_markdown(result: TaskSchemaEvolutionPlan) -> str:
    """Render a task schema evolution plan as Markdown."""
    return result.to_markdown()


task_schema_evolution_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[SchemaSurface, ...] = field(default_factory=tuple)
    additive_evidence: tuple[str, ...] = field(default_factory=tuple)
    breaking_evidence: tuple[str, ...] = field(default_factory=tuple)
    compatibility_evidence: tuple[str, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_schema_surface(self) -> bool:
        return bool(self.surface_evidence)

    @property
    def has_additive(self) -> bool:
        return bool(self.additive_evidence)

    @property
    def has_breaking(self) -> bool:
        return bool(self.breaking_evidence)

    @property
    def has_compatibility(self) -> bool:
        return bool(self.compatibility_evidence)


def _task_record(task: Mapping[str, Any], index: int) -> TaskSchemaEvolutionRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    risk = _evolution_risk(signals, task)
    return TaskSchemaEvolutionRecord(
        task_id=task_id,
        title=title,
        evolution_risk=risk,
        schema_surfaces=signals.surfaces if risk != "not_schema_related" else (),
        change_indicators=_change_indicators(signals, risk),
        guardrails=_guardrails(signals, risk),
        evidence=signals.evidence if risk != "not_schema_related" else (),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[SchemaSurface] = set()
    surface_evidence: list[str] = []
    additive_evidence: list[str] = []
    breaking_evidence: list[str] = []
    compatibility_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for surface, pattern in _PATH_SURFACE_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                surface_evidence.append(path_evidence)
        _collect_change_evidence(
            path_text,
            path_evidence,
            additive_evidence,
            breaking_evidence,
            compatibility_evidence,
        )

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for surface, pattern in _TEXT_SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)
                surface_evidence.append(snippet)
        _collect_change_evidence(
            text,
            snippet,
            additive_evidence,
            breaking_evidence,
            compatibility_evidence,
        )

    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    evidence = tuple(
        _dedupe(
            [*surface_evidence, *breaking_evidence, *additive_evidence, *compatibility_evidence]
        )
    )
    return _Signals(
        surfaces=ordered_surfaces,
        additive_evidence=tuple(_dedupe(additive_evidence)),
        breaking_evidence=tuple(_dedupe(breaking_evidence)),
        compatibility_evidence=tuple(_dedupe(compatibility_evidence)),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        evidence=evidence,
    )


def _collect_change_evidence(
    text: str,
    snippet: str,
    additive_evidence: list[str],
    breaking_evidence: list[str],
    compatibility_evidence: list[str],
) -> None:
    if _BREAKING_RE.search(text):
        breaking_evidence.append(snippet)
    if _ADDITIVE_RE.search(text):
        additive_evidence.append(snippet)
    if _COMPATIBILITY_RE.search(text):
        compatibility_evidence.append(snippet)


def _evolution_risk(signals: _Signals, task: Mapping[str, Any]) -> SchemaEvolutionRisk:
    if not signals.has_schema_surface:
        return "not_schema_related"
    if _is_doc_or_test_only(task) and not (signals.has_breaking or signals.has_additive):
        return "not_schema_related"
    if signals.has_breaking:
        return "breaking_change_risk"
    if _has_enum_signal(signals):
        return "compatibility_guarded"
    if signals.has_additive and not signals.has_compatibility:
        return "additive_safe"
    return "compatibility_guarded"


def _change_indicators(signals: _Signals, risk: SchemaEvolutionRisk) -> tuple[str, ...]:
    if risk == "not_schema_related":
        return ()
    indicators: list[str] = []
    if signals.has_breaking:
        indicators.append("breaking schema or serialized contract change")
    if signals.has_additive:
        indicators.append("additive schema or contract extension")
    if signals.has_compatibility:
        indicators.append("compatibility, versioning, migration, or deprecation signal")
    if signals.has_schema_surface and not indicators:
        indicators.append("schema surface changed without explicit compatibility posture")
    return tuple(indicators)


def _has_enum_signal(signals: _Signals) -> bool:
    return any(_ENUM_RE.search(evidence) for evidence in signals.evidence)


def _guardrails(signals: _Signals, risk: SchemaEvolutionRisk) -> tuple[str, ...]:
    if risk == "not_schema_related":
        return ()
    guardrails: list[str] = [
        "Maintain rollback compatibility for readers, writers, and deployed clients until the rollout completes.",
        "Update serialized fixtures and schema snapshots for every affected contract surface.",
    ]
    if risk == "breaking_change_risk":
        guardrails.extend(
            [
                "Use dual-read/write or adapter compatibility so old and new field names or shapes can coexist.",
                "Publish a deprecation notice with owner, migration window, and consumer action before removal or rename.",
                "Run versioned contract tests against previous and next schema versions before release.",
            ]
        )
    elif risk == "compatibility_guarded":
        guardrails.extend(
            [
                "Run versioned contract tests for existing consumers and generated clients.",
                "Document the compatibility window and deprecation notice if consumers must migrate.",
            ]
        )
    else:
        guardrails.append(
            "Confirm additive fields are optional, nullable, or defaulted for existing producers and consumers."
        )

    if "database" in signals.surfaces:
        guardrails.extend(
            [
                "Sequence database migration as expand, backfill, verify, contract, with backfill sequencing owned separately from DDL.",
                "Keep old columns or tables readable until rollback compatibility and backfill verification pass.",
            ]
        )
    if any(surface in signals.surfaces for surface in ("api_schema", "graphql", "openapi")):
        guardrails.append(
            "Run versioned contract tests for API requests, responses, generated clients, and documented examples."
        )
    if "protobuf" in signals.surfaces:
        guardrails.append(
            "Preserve protobuf field numbers and reserve removed fields or enum values before regenerating clients."
        )
    if "event_payload" in signals.surfaces:
        guardrails.extend(
            [
                "Support dual-read/write event payload handling until all consumers have processed the new shape.",
                "Replay representative event fixtures through old and new consumers before rollout.",
            ]
        )
    if "config_schema" in signals.surfaces:
        guardrails.append(
            "Provide config defaults and validation fallback for old configuration files during rollout."
        )
    if "serialization_model" in signals.surfaces:
        guardrails.append(
            "Update serializer/deserializer round-trip fixtures and keep backward-compatible aliases while data is mixed."
        )
    return tuple(_dedupe(guardrails))


def _is_doc_or_test_only(task: Mapping[str, Any]) -> bool:
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    if not paths:
        return False
    return all(_DOC_TEST_PATH_RE.search(_normalized_path(path)) for path in paths)


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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
    return None, tasks


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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
                if _COMPATIBILITY_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _COMPATIBILITY_RE.search(key_text):
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
    "SchemaEvolutionRisk",
    "SchemaSurface",
    "TaskSchemaEvolutionPlan",
    "TaskSchemaEvolutionRecord",
    "analyze_task_schema_evolution",
    "build_task_schema_evolution_plan",
    "summarize_task_schema_evolution",
    "task_schema_evolution_plan_to_dict",
    "task_schema_evolution_plan_to_markdown",
]
