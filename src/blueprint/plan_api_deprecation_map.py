"""Identify execution-plan tasks that touch deprecated API contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiDeprecationImpactCategory = Literal["producer", "consumer", "migration", "cleanup"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[ApiDeprecationImpactCategory, int] = {
    "producer": 0,
    "consumer": 1,
    "migration": 2,
    "cleanup": 3,
}
_TEXT_PATTERNS: dict[ApiDeprecationImpactCategory, re.Pattern[str]] = {
    "producer": re.compile(
        r"\b(?:endpoint|route|handler|controller|api|graphql|grpc|schema|contract|"
        r"openapi|swagger|request|response|webhook|producer|server|service)\b",
        re.IGNORECASE,
    ),
    "consumer": re.compile(
        r"\b(?:client|consumers?|callers?|integration|sdk|library|adapter|connector|"
        r"downstream|upstream|webhook subscriber|api call|request to|consume)\b",
        re.IGNORECASE,
    ),
    "migration": re.compile(
        r"\b(?:migration|migrate|upgrade|version bump|bump .*version|v\d+(?:\.\d+)?|"
        r"api version|versioned|compatibility|backward compatible|backwards compatible|"
        r"dual[- ]?write|dual[- ]?read|feature flag|rollout|canary|fallback)\b",
        re.IGNORECASE,
    ),
    "cleanup": re.compile(
        r"\b(?:deprecat(?:e|ed|ion)|legacy|remove|removal|delete|sunset|retire|"
        r"obsolete|end[- ]?of[- ]?life|eol|contract removal|drop support|breaking change)\b",
        re.IGNORECASE,
    ),
}
_INCLUSION_RE = re.compile(
    r"\b(?:deprecat(?:e|ed|ion)|legacy|sunset|retire|obsolete|end[- ]?of[- ]?life|"
    r"eol|contract removal|drop support|breaking change|api version|version bump|"
    r"bump .*version|sdk upgrade|upgrade .*sdk|v\d+(?:\.\d+)?|endpoint|route|"
    r"schema|contract|openapi|swagger|compatibility|backward compatible|"
    r"backwards compatible)\b",
    re.IGNORECASE,
)
_SURFACE_PATTERNS: dict[str, re.Pattern[str]] = {
    "endpoint": re.compile(r"\b(?:endpoint|route|handler|controller|rest|http)\b", re.I),
    "sdk": re.compile(r"\b(?:sdk|client library|library|package)\b", re.I),
    "schema": re.compile(r"\b(?:schema|contract|openapi|swagger|graphql|protobuf|proto|avro)\b", re.I),
    "webhook": re.compile(r"\bwebhook\b", re.I),
    "compatibility": re.compile(
        r"\b(?:compatibility|backward compatible|backwards compatible|breaking change|fallback)\b",
        re.I,
    ),
}
_SURFACE_ORDER = {
    "endpoint": 0,
    "sdk": 1,
    "schema": 2,
    "webhook": 3,
    "compatibility": 4,
}
_MIGRATION_NOTE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\b(?:deprecat(?:e|ed|ion)|sunset|retire|end[- ]?of[- ]?life|eol)\b", re.I),
        "Document the deprecated API timeline, replacement surface, and removal criteria.",
    ),
    (
        re.compile(r"\b(?:version bump|api version|v\d+(?:\.\d+)?|upgrade|migrate|migration)\b", re.I),
        "Record the source and target API versions plus the migration sequence.",
    ),
    (
        re.compile(r"\b(?:sdk|client library|package)\b", re.I),
        "Identify SDK consumers, minimum supported versions, and upgrade guidance.",
    ),
    (
        re.compile(r"\b(?:legacy|contract removal|remove|removal|delete|drop support|breaking change)\b", re.I),
        "Confirm legacy contract removal is gated by adoption evidence and rollback expectations.",
    ),
    (
        re.compile(r"\b(?:compatibility|backward compatible|backwards compatible|fallback)\b", re.I),
        "Keep compatibility expectations explicit for mixed-version producers and consumers.",
    ),
)


@dataclass(frozen=True, slots=True)
class PlanApiDeprecationMapRecord:
    """Deprecation impact guidance for one execution task."""

    task_id: str
    task_title: str
    impact_category: ApiDeprecationImpactCategory
    affected_surfaces: tuple[str, ...]
    mitigation_checklist: tuple[str, ...]
    migration_notes: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "impact_category": self.impact_category,
            "affected_surfaces": list(self.affected_surfaces),
            "mitigation_checklist": list(self.mitigation_checklist),
            "migration_notes": list(self.migration_notes),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanApiDeprecationMap:
    """Plan-level map of API deprecation and compatibility impact."""

    plan_id: str | None = None
    records: tuple[PlanApiDeprecationMapRecord, ...] = field(default_factory=tuple)
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
        """Return deprecation impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the deprecation impact map as deterministic Markdown."""
        title = "# Plan API Deprecation Impact Map"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No API deprecation impacts were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Affected Surfaces | Mitigation Checklist | Migration Notes |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.impact_category} | "
                f"{_markdown_cell(', '.join(record.affected_surfaces))} | "
                f"{_markdown_cell('; '.join(record.mitigation_checklist))} | "
                f"{_markdown_cell('; '.join(record.migration_notes))} |"
            )
        return "\n".join(lines)


def build_plan_api_deprecation_map(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanApiDeprecationMap:
    """Build API deprecation and compatibility guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (record.task_id, record.task_title),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    category_counts = {
        category: sum(1 for record in records if record.impact_category == category)
        for category in _CATEGORY_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in records if surface in record.affected_surfaces)
        for surface in sorted({surface for record in records for surface in record.affected_surfaces})
    }

    return PlanApiDeprecationMap(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        summary={
            "record_count": len(records),
            "impacted_task_count": len(impacted_task_ids),
            "category_counts": category_counts,
            "surface_counts": surface_counts,
        },
    )


def plan_api_deprecation_map_to_dict(result: PlanApiDeprecationMap) -> dict[str, Any]:
    """Serialize an API deprecation impact map to a plain dictionary."""
    return result.to_dict()


plan_api_deprecation_map_to_dict.__test__ = False


def plan_api_deprecation_map_to_markdown(result: PlanApiDeprecationMap) -> str:
    """Render an API deprecation impact map as Markdown."""
    return result.to_markdown()


plan_api_deprecation_map_to_markdown.__test__ = False


def summarize_plan_api_deprecations(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanApiDeprecationMap:
    """Compatibility alias for building API deprecation impact maps."""
    return build_plan_api_deprecation_map(source)


def _task_record(
    task: Mapping[str, Any],
    index: int,
) -> PlanApiDeprecationMapRecord | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals:
        return None

    evidence = tuple(_dedupe(item for entries in signals.values() for item in entries))
    combined = " ".join(evidence)
    surfaces = tuple(_affected_surfaces(task, evidence))
    return PlanApiDeprecationMapRecord(
        task_id=task_id,
        task_title=title,
        impact_category=_impact_category(signals),
        affected_surfaces=surfaces,
        mitigation_checklist=tuple(_mitigation_checklist(signals, surfaces, combined)),
        migration_notes=tuple(_migration_notes(combined)),
        evidence=evidence,
    )


def _signals(
    task: Mapping[str, Any],
) -> dict[ApiDeprecationImpactCategory, tuple[str, ...]]:
    signals: dict[ApiDeprecationImpactCategory, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        if not _INCLUSION_RE.search(text):
            continue
        for category, pattern in _TEXT_PATTERNS.items():
            if pattern.search(text):
                _append(signals, category, f"{source_field}: {text}")

    return {
        category: tuple(_dedupe(evidence))
        for category, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[ApiDeprecationImpactCategory, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    evidence = f"files_or_modules: {original}"
    has_api_surface = bool(
        {"api", "apis", "endpoint", "endpoints", "routes", "controllers", "handlers"} & parts
    )
    has_contract_surface = suffix in {".proto", ".graphql", ".gql", ".yaml", ".yml", ".json"} or any(
        token in name for token in ("schema", "contract", "openapi", "swagger")
    )
    has_version_surface = re.search(r"(?:^|[-_/])v\d+(?:[-_.]|$)", normalized) is not None
    has_sdk_surface = bool({"sdk", "client", "clients", "integrations", "adapters"} & parts)
    has_cleanup_surface = any(
        token in normalized
        for token in ("deprecated", "deprecation", "legacy", "sunset", "eol", "migration")
    )

    if has_api_surface or has_contract_surface or has_version_surface:
        _append(signals, "producer", evidence)
    if has_sdk_surface:
        _append(signals, "consumer", evidence)
    if has_version_surface or has_cleanup_surface or "migration" in name:
        _append(signals, "migration", evidence)
    if has_cleanup_surface:
        _append(signals, "cleanup", evidence)


def _impact_category(
    signals: Mapping[ApiDeprecationImpactCategory, tuple[str, ...]],
) -> ApiDeprecationImpactCategory:
    for category in ("cleanup", "migration", "producer", "consumer"):
        if signals.get(category):
            return category
    return "consumer"


def _affected_surfaces(task: Mapping[str, Any], evidence: tuple[str, ...]) -> list[str]:
    surfaces: list[str] = []
    combined = " ".join(evidence)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(combined):
            surfaces.append(surface)

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path).casefold()
        if not normalized:
            continue
        path_obj = PurePosixPath(normalized)
        parts = set(path_obj.parts)
        if {"api", "apis", "routes", "controllers", "handlers"} & parts:
            surfaces.append("endpoint")
        if {"sdk", "client", "clients", "integrations", "adapters"} & parts:
            surfaces.append("sdk")
        if path_obj.suffix in {".proto", ".graphql", ".gql", ".yaml", ".yml", ".json"} or any(
            token in path_obj.name for token in ("schema", "contract", "openapi", "swagger")
        ):
            surfaces.append("schema")
        version_match = re.search(r"(?<![A-Za-z0-9])v\d+(?:\.\d+)?(?![A-Za-z0-9])", path)
        if version_match:
            surfaces.append(f"api_version:{version_match.group(0)}")

    deduped = _dedupe(surfaces) or ["api_contract"]
    return sorted(
        deduped,
        key=lambda surface: (
            _SURFACE_ORDER.get(surface, 10 if surface.startswith("api_version:") else 9),
            surface,
        ),
    )


def _mitigation_checklist(
    signals: Mapping[ApiDeprecationImpactCategory, tuple[str, ...]],
    surfaces: tuple[str, ...],
    combined_evidence: str,
) -> list[str]:
    checklist = [
        "Add compatibility tests covering old and new API behavior.",
        "Record rollout notes with sequencing, flags, rollback, and monitoring expectations.",
    ]
    if signals.get("producer"):
        checklist.append("Verify producer responses, schemas, status codes, and version negotiation remain compatible.")
    if signals.get("consumer"):
        checklist.append("Verify consumers, SDK callers, integrations, and downstream clients handle both contracts.")
    if signals.get("migration"):
        checklist.append("Document migration steps, version support window, and validation evidence.")
    if signals.get("cleanup"):
        checklist.append("Gate legacy removal on adoption data, deprecation timeline, and rollback readiness.")
    if _requires_communication(signals, surfaces, combined_evidence):
        checklist.append("Prepare communication for affected owners, consumers, support, and release notes.")
    return _dedupe(checklist)


def _requires_communication(
    signals: Mapping[ApiDeprecationImpactCategory, tuple[str, ...]],
    surfaces: tuple[str, ...],
    combined_evidence: str,
) -> bool:
    if signals.get("cleanup") or signals.get("consumer"):
        return True
    if any(surface in {"sdk", "webhook"} for surface in surfaces):
        return True
    return bool(re.search(r"\b(?:public|external|customer|partner|downstream|release notes)\b", combined_evidence, re.I))


def _migration_notes(combined_evidence: str) -> list[str]:
    notes = [
        note
        for pattern, note in _MIGRATION_NOTE_PATTERNS
        if pattern.search(combined_evidence)
    ]
    return _dedupe(notes) or [
        "Confirm the task owner records why this API surface is safe for the planned change."
    ]


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
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


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


def _append(
    signals: dict[ApiDeprecationImpactCategory, list[str]],
    category: ApiDeprecationImpactCategory,
    evidence: str,
) -> None:
    signals.setdefault(category, []).append(evidence)


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
    "ApiDeprecationImpactCategory",
    "PlanApiDeprecationMap",
    "PlanApiDeprecationMapRecord",
    "build_plan_api_deprecation_map",
    "plan_api_deprecation_map_to_dict",
    "plan_api_deprecation_map_to_markdown",
    "summarize_plan_api_deprecations",
]
