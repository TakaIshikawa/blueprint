"""Recommend idempotency readiness safeguards for retryable execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IdempotencySurface = Literal[
    "retryable_workflow",
    "webhook_receiver",
    "queue_consumer",
    "payment_flow",
    "import_job",
    "migration",
    "external_callback",
]
IdempotencyAcceptanceCriterion = Literal[
    "idempotency_key_or_dedupe_key",
    "duplicate_delivery_tests",
    "retry_replay_acceptance",
    "side_effect_deduplication",
    "safe_rerun_or_compensation",
    "duplicate_observability",
]
IdempotencyRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: tuple[IdempotencySurface, ...] = (
    "retryable_workflow",
    "webhook_receiver",
    "queue_consumer",
    "payment_flow",
    "import_job",
    "migration",
    "external_callback",
)
_RISK_ORDER: dict[IdempotencyRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_ACCEPTANCE_ORDER: tuple[IdempotencyAcceptanceCriterion, ...] = (
    "idempotency_key_or_dedupe_key",
    "duplicate_delivery_tests",
    "retry_replay_acceptance",
    "side_effect_deduplication",
    "safe_rerun_or_compensation",
    "duplicate_observability",
)
_SURFACE_PATTERNS: dict[IdempotencySurface, re.Pattern[str]] = {
    "retryable_workflow": re.compile(
        r"\b(?:retry|retries|retryable|rerun|re-run|replay|redeliver|at[- ]least[- ]once|"
        r"backoff|transient failure|recoverable failure)\b",
        re.I,
    ),
    "webhook_receiver": re.compile(
        r"\b(?:webhooks?|incoming webhook|webhook receiver|webhook endpoint|webhook handler)\b",
        re.I,
    ),
    "queue_consumer": re.compile(
        r"\b(?:queue consumers?|message consumers?|consumer worker|worker consumes?|job queue|"
        r"event consumers?|dead[- ]letter|dlq|poison message)\b",
        re.I,
    ),
    "payment_flow": re.compile(
        r"\b(?:payments?|checkout|billing|invoice|refund|charge|subscription|stripe|adyen|paypal)\b",
        re.I,
    ),
    "import_job": re.compile(
        r"\b(?:imports?|csv import|bulk upload|file ingest(?:ion)?|data ingest(?:ion)?|"
        r"external feed|partner feed)\b",
        re.I,
    ),
    "migration": re.compile(
        r"\b(?:migrations?|schema migration|data migration|backfill|data repair|reconciliation job)\b",
        re.I,
    ),
    "external_callback": re.compile(
        r"\b(?:callbacks?|external callback|provider callback|partner callback|return url|redirect callback)\b",
        re.I,
    ),
}
_PATH_SURFACE_PATTERNS: dict[IdempotencySurface, re.Pattern[str]] = {
    "retryable_workflow": re.compile(
        r"(?:^|/)(?:retries?|retry|backoff|replay)(?:/|\.|_|-|$)", re.I
    ),
    "webhook_receiver": re.compile(
        r"(?:^|/)(?:webhooks?|webhook[_-]?handlers?)(?:/|\.|_|-|$)", re.I
    ),
    "queue_consumer": re.compile(r"(?:^|/)(?:queues?|consumers?|workers?|dlq)(?:/|\.|_|-|$)", re.I),
    "payment_flow": re.compile(
        r"(?:^|/)(?:payments?|checkout|billing|stripe|adyen|paypal)(?:/|\.|_|-|$)", re.I
    ),
    "import_job": re.compile(
        r"(?:^|/)(?:imports?|ingest|feeds?|bulk[_-]?upload)(?:/|\.|_|-|$)", re.I
    ),
    "migration": re.compile(
        r"(?:^|/)(?:migrations?|backfills?|data[_-]?repairs?)(?:/|\.|_|-|$)", re.I
    ),
    "external_callback": re.compile(r"(?:^|/)(?:callbacks?|return[_-]?urls?)(?:/|\.|_|-|$)", re.I),
}
_ACCEPTANCE_PATTERNS: dict[IdempotencyAcceptanceCriterion, re.Pattern[str]] = {
    "idempotency_key_or_dedupe_key": re.compile(
        r"\b(?:idempotenc|dedup(?:e|lication)?|duplicate prevention|event id|event_id|"
        r"request id|request_id|idempotency key|unique constraint)\b",
        re.I,
    ),
    "duplicate_delivery_tests": re.compile(
        r"\b(?:(?:duplicate|replayed?|redelivered?) (?:delivery|event|request|message|payload)s?|"
        r"duplicate.*test|test.*duplicate|replay.*test|redelivery.*test)\b",
        re.I,
    ),
    "retry_replay_acceptance": re.compile(
        r"\b(?:retry acceptance|retry-safe|safe retry|replay-safe|safe replay|rerun-safe|"
        r"safe to rerun|at[- ]least[- ]once)\b",
        re.I,
    ),
    "side_effect_deduplication": re.compile(
        r"\b(?:side[- ]effects?|double charge|double email|duplicate write|duplicate record|"
        r"exactly[- ]once|single charge|send only once|create only once)\b",
        re.I,
    ),
    "safe_rerun_or_compensation": re.compile(
        r"\b(?:safe rerun|safe to rerun|compensation|compensating action|rollback step|"
        r"resume from checkpoint|checkpoint|dry run|transaction boundary)\b",
        re.I,
    ),
    "duplicate_observability": re.compile(
        r"\b(?:duplicate metric|idempotency metric|dedupe metric|duplicate log|audit.*duplicate|"
        r"observability|alert.*duplicate|trace.*duplicate)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskIdempotencyReadinessRecommendation:
    """Idempotency readiness guidance for one execution task."""

    task_id: str
    title: str
    idempotency_surfaces: tuple[IdempotencySurface, ...]
    missing_acceptance_criteria: tuple[IdempotencyAcceptanceCriterion, ...]
    risk_level: IdempotencyRiskLevel
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "idempotency_surfaces": list(self.idempotency_surfaces),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskIdempotencyReadinessPlan:
    """Task-level idempotency readiness recommendations."""

    plan_id: str | None = None
    recommendations: tuple[TaskIdempotencyReadinessRecommendation, ...] = field(
        default_factory=tuple
    )
    sensitive_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "sensitive_task_ids": list(self.sensitive_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    @property
    def records(self) -> tuple[TaskIdempotencyReadinessRecommendation, ...]:
        """Compatibility view matching planners that name extracted items records."""
        return self.recommendations


def build_task_idempotency_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskIdempotencyReadinessPlan:
    """Build idempotency readiness recommendations for retryable task surfaces."""
    plan_id, tasks = _source_payload(source)
    recommendations = [
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index)) is not None
    ]
    recommendations.sort(
        key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id, item.title.casefold())
    )
    result = tuple(recommendations)
    surface_counts = {
        surface: sum(1 for item in result if surface in item.idempotency_surfaces)
        for surface in _SURFACE_ORDER
    }
    risk_counts = {
        risk: sum(1 for item in result if item.risk_level == risk) for risk in _RISK_ORDER
    }

    return TaskIdempotencyReadinessPlan(
        plan_id=plan_id,
        recommendations=result,
        sensitive_task_ids=tuple(item.task_id for item in result),
        summary={
            "task_count": len(tasks),
            "sensitive_task_count": len(result),
            "recommendation_count": len(result),
            "high_risk_count": risk_counts["high"],
            "medium_risk_count": risk_counts["medium"],
            "low_risk_count": risk_counts["low"],
            "missing_acceptance_criteria_count": sum(
                len(item.missing_acceptance_criteria) for item in result
            ),
            "surface_counts": surface_counts,
            "sensitive_task_ids": [item.task_id for item in result],
        },
    )


def extract_task_idempotency_readiness_recommendations(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskIdempotencyReadinessRecommendation, ...]:
    """Return idempotency readiness recommendations from task-shaped input."""
    return build_task_idempotency_readiness_plan(source).recommendations


def task_idempotency_readiness_plan_to_dict(
    result: TaskIdempotencyReadinessPlan,
) -> dict[str, Any]:
    """Serialize an idempotency readiness plan to a plain dictionary."""
    return result.to_dict()


task_idempotency_readiness_plan_to_dict.__test__ = False


def _recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskIdempotencyReadinessRecommendation | None:
    surfaces, evidence = _surface_signals(task)
    if not surfaces:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    acceptance_context = " ".join(_strings(task.get("acceptance_criteria")))
    present = _present_acceptance_criteria(acceptance_context)
    missing = tuple(criterion for criterion in _ACCEPTANCE_ORDER if criterion not in present)

    return TaskIdempotencyReadinessRecommendation(
        task_id=task_id,
        title=title,
        idempotency_surfaces=surfaces,
        missing_acceptance_criteria=missing,
        risk_level=_risk_level(surfaces, missing),
        evidence=tuple(_dedupe(evidence)),
    )


def _surface_signals(task: Mapping[str, Any]) -> tuple[tuple[IdempotencySurface, ...], list[str]]:
    surfaces: set[IdempotencySurface] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for surface in _SURFACE_ORDER:
            if _PATH_SURFACE_PATTERNS[surface].search(normalized) or _SURFACE_PATTERNS[
                surface
            ].search(path_text):
                surfaces.add(surface)
                evidence.append(f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        matched = False
        for surface in _SURFACE_ORDER:
            if _SURFACE_PATTERNS[surface].search(text):
                surfaces.add(surface)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return tuple(surface for surface in _SURFACE_ORDER if surface in surfaces), evidence


def _present_acceptance_criteria(context: str) -> set[IdempotencyAcceptanceCriterion]:
    return {
        criterion for criterion, pattern in _ACCEPTANCE_PATTERNS.items() if pattern.search(context)
    }


def _risk_level(
    surfaces: tuple[IdempotencySurface, ...],
    missing: tuple[IdempotencyAcceptanceCriterion, ...],
) -> IdempotencyRiskLevel:
    surface_set = set(surfaces)
    missing_set = set(missing)
    if surface_set & {"payment_flow", "migration"} and (
        "idempotency_key_or_dedupe_key" in missing_set
        or "side_effect_deduplication" in missing_set
        or "safe_rerun_or_compensation" in missing_set
    ):
        return "high"
    if surface_set & {"webhook_receiver", "queue_consumer", "external_callback"} and (
        "idempotency_key_or_dedupe_key" in missing_set or "duplicate_delivery_tests" in missing_set
    ):
        return "high"
    if len(missing) >= 4:
        return "medium"
    if surface_set & {"retryable_workflow", "import_job"} and len(missing) >= 2:
        return "medium"
    return "low"


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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in _SURFACE_PATTERNS.values()):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _SURFACE_PATTERNS.values()):
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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    "IdempotencyAcceptanceCriterion",
    "IdempotencyRiskLevel",
    "IdempotencySurface",
    "TaskIdempotencyReadinessPlan",
    "TaskIdempotencyReadinessRecommendation",
    "build_task_idempotency_readiness_plan",
    "extract_task_idempotency_readiness_recommendations",
    "task_idempotency_readiness_plan_to_dict",
]
