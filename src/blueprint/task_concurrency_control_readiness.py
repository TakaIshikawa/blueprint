"""Plan concurrency-control readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ConcurrencyRiskLevel = Literal["low", "medium", "high"]
ConcurrencySurface = Literal[
    "concurrent_write",
    "parallel_worker",
    "shared_queue",
    "lock_contention",
    "optimistic_concurrency",
    "race_condition",
    "duplicate_submission",
]
ConcurrencyControl = Literal[
    "lock_strategy",
    "conflict_detection",
    "retry_or_backoff",
    "duplicate_request_guard",
    "transactional_boundary",
    "observability_metric",
    "rollback_or_repair_path",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[ConcurrencyRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[ConcurrencySurface, int] = {
    "concurrent_write": 0,
    "parallel_worker": 1,
    "shared_queue": 2,
    "lock_contention": 3,
    "optimistic_concurrency": 4,
    "race_condition": 5,
    "duplicate_submission": 6,
}
_CONTROL_ORDER: dict[ConcurrencyControl, int] = {
    "lock_strategy": 0,
    "conflict_detection": 1,
    "retry_or_backoff": 2,
    "duplicate_request_guard": 3,
    "transactional_boundary": 4,
    "observability_metric": 5,
    "rollback_or_repair_path": 6,
}
_SURFACE_PATTERNS: dict[ConcurrencySurface, re.Pattern[str]] = {
    "concurrent_write": re.compile(
        r"\b(?:concurrent writes?|parallel writes?|simultaneous writes?|shared state|"
        r"write contention|lost updates?|multi[- ]writer|multiple writers?|write path)\b",
        re.I,
    ),
    "parallel_worker": re.compile(
        r"\b(?:parallel workers?|worker pool|background workers?|job workers?|consumer workers?|"
        r"concurrent workers?|multi[- ]worker|thread pool|async workers?|celery|sidekiq|queue consumer)\b",
        re.I,
    ),
    "shared_queue": re.compile(
        r"\b(?:shared queue|work queue|job queue|task queue|message queue|kafka|sqs|rabbitmq|"
        r"pub/sub|pubsub|consumer group|outbox)\b",
        re.I,
    ),
    "lock_contention": re.compile(
        r"\b(?:lock|locking|mutex|semaphore|advisory lock|distributed lock|pessimistic lock|"
        r"row lock|lock contention|lock timeout|deadlock|critical section)\b",
        re.I,
    ),
    "optimistic_concurrency": re.compile(
        r"\b(?:optimistic concurrenc(?:y|ies)|optimistic lock|version check|versioned writes?|"
        r"compare[- ]and[- ]swap|cas|etag|if[- ]match|revision token|write version)\b",
        re.I,
    ),
    "race_condition": re.compile(
        r"\b(?:race condition|racey|racy|lost update|write skew|read[- ]modify[- ]write|"
        r"time[- ]of[- ]check|toctou|interleaving|ordering hazard)\b",
        re.I,
    ),
    "duplicate_submission": re.compile(
        r"\b(?:duplicate submissions?|double submit|double click|duplicate requests?|duplicate jobs?|"
        r"replayed requests?|idempotency|idempotency key|dedupe|deduplicate|exactly once)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[ConcurrencySurface, re.Pattern[str]] = {
    "concurrent_write": re.compile(r"concurrent|parallel|multi[_-]?writer|shared[_-]?state|write[_-]?path", re.I),
    "parallel_worker": re.compile(r"(?:^|/)(?:workers?|jobs?|consumers?|threads?|tasks?)(?:/|$)|worker|consumer", re.I),
    "shared_queue": re.compile(r"(?:^|/)(?:queues?|messaging|kafka|sqs|rabbitmq|outbox)(?:/|$)|queue|consumer", re.I),
    "lock_contention": re.compile(r"locks?|mutex|semaphore|advisory[_-]?lock|critical[_-]?section", re.I),
    "optimistic_concurrency": re.compile(r"optimistic|version(?:ed)?[_-]?write|etag|revision|cas", re.I),
    "race_condition": re.compile(r"race|lost[_-]?update|toctou|read[_-]?modify[_-]?write", re.I),
    "duplicate_submission": re.compile(r"duplicate|dedupe|idempotenc|double[_-]?submit|replay", re.I),
}
_CONTROL_PATTERNS: dict[ConcurrencyControl, re.Pattern[str]] = {
    "lock_strategy": re.compile(
        r"\b(?:lock strategy|locking strategy|explicit locking|locking control|mutex|semaphore|advisory lock|distributed lock|"
        r"critical section|lock ordering|lease|fencing token|pessimistic lock)\b",
        re.I,
    ),
    "conflict_detection": re.compile(
        r"\b(?:conflict detection|conflict handling|detect conflicts?|version check|optimistic lock|etag|if[- ]match|"
        r"compare[- ]and[- ]swap|cas|stale write|write conflict|unique constraint)\b",
        re.I,
    ),
    "retry_or_backoff": re.compile(
        r"\b(?:retry|retries|backoff|exponential backoff|jitter|retry budget|retry[- ]safe|"
        r"deadlock retry|lock timeout retry)\b",
        re.I,
    ),
    "duplicate_request_guard": re.compile(
        r"\b(?:duplicate request guard|idempotency key|idempotency|dedupe key|deduplicate|"
        r"duplicate guards?|single flight|once only|exactly once|unique request)\b",
        re.I,
    ),
    "transactional_boundary": re.compile(
        r"\b(?:transactional boundaries|transactional boundary|transaction boundaries|transaction boundary|transaction|atomic|commit boundary|"
        r"unit of work|select for update|outbox transaction)\b",
        re.I,
    ),
    "observability_metric": re.compile(
        r"\b(?:observability|metrics?|metric|dashboard|alert|logging|trace|contention metric|"
        r"retry count|conflict rate|duplicate rate|lock wait|deadlock rate)\b",
        re.I,
    ),
    "rollback_or_repair_path": re.compile(
        r"\b(?:rollback|roll back|repair paths?|reconciliation|reconcile|compensating action|"
        r"compensation|manual repair|requeue|dead letter|dlq|restore)\b",
        re.I,
    ),
}
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only|storybook)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskConcurrencyControlReadinessRecommendation:
    """Concurrency-control guidance for one affected execution task."""

    task_id: str
    title: str
    concurrency_surfaces: tuple[ConcurrencySurface, ...] = field(default_factory=tuple)
    missing_controls: tuple[ConcurrencyControl, ...] = field(default_factory=tuple)
    risk_level: ConcurrencyRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "concurrency_surfaces": list(self.concurrency_surfaces),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskConcurrencyControlReadinessPlan:
    """Plan-level concurrency-control readiness summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskConcurrencyControlReadinessRecommendation, ...] = field(default_factory=tuple)
    concurrency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "concurrency_task_ids": list(self.concurrency_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return concurrency-control recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]

    @property
    def records(self) -> tuple[TaskConcurrencyControlReadinessRecommendation, ...]:
        """Compatibility view matching planners that name task rows records."""
        return self.recommendations

    def to_markdown(self) -> str:
        """Render the concurrency-control readiness plan as deterministic Markdown."""
        title = "# Task Concurrency Control Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('concurrency_task_count', 0)} concurrency-sensitive tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(suppressed: {self.summary.get('suppressed_task_count', 0)})."
            ),
        ]
        if not self.recommendations:
            lines.extend(["", "No concurrency-control readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Concurrency Surfaces | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.concurrency_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_concurrency_control_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskConcurrencyControlReadinessPlan:
    """Build task-level concurrency-control readiness recommendations."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record_for_task(task, index)) is not None
            ),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                len(record.missing_controls),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    concurrency_task_ids = tuple(record.task_id for record in records)
    concurrency_task_id_set = set(concurrency_task_ids)
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in concurrency_task_id_set
    )
    return TaskConcurrencyControlReadinessPlan(
        plan_id=plan_id,
        recommendations=records,
        concurrency_task_ids=concurrency_task_ids,
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, total_task_count=len(tasks), suppressed_task_count=len(suppressed_task_ids)),
    )


def generate_task_concurrency_control_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskConcurrencyControlReadinessRecommendation, ...]:
    """Return concurrency-control recommendations for relevant execution tasks."""
    return build_task_concurrency_control_readiness_plan(source).recommendations


def summarize_task_concurrency_control_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskConcurrencyControlReadinessPlan:
    """Compatibility alias for building concurrency-control readiness plans."""
    return build_task_concurrency_control_readiness_plan(source)


def task_concurrency_control_readiness_plan_to_dict(
    result: TaskConcurrencyControlReadinessPlan,
) -> dict[str, Any]:
    """Serialize a concurrency-control readiness plan to a plain dictionary."""
    return result.to_dict()


task_concurrency_control_readiness_plan_to_dict.__test__ = False


def task_concurrency_control_readiness_to_dicts(
    records: (
        tuple[TaskConcurrencyControlReadinessRecommendation, ...]
        | list[TaskConcurrencyControlReadinessRecommendation]
        | TaskConcurrencyControlReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize concurrency-control readiness recommendations to dictionaries."""
    if isinstance(records, TaskConcurrencyControlReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_concurrency_control_readiness_to_dicts.__test__ = False


def task_concurrency_control_readiness_plan_to_markdown(
    result: TaskConcurrencyControlReadinessPlan,
) -> str:
    """Render a concurrency-control readiness plan as Markdown."""
    return result.to_markdown()


task_concurrency_control_readiness_plan_to_markdown.__test__ = False


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskConcurrencyControlReadinessRecommendation | None:
    surfaces: dict[ConcurrencySurface, list[str]] = {}
    controls: set[ConcurrencyControl] = set()
    text_items = _candidate_texts(task)

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces)
    for source_field, text in text_items:
        _inspect_text(source_field, text, surfaces, controls)

    if not surfaces or _only_low_signal_documentation(text_items):
        return None

    concurrency_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    task_id = _task_id(task, index)
    return TaskConcurrencyControlReadinessRecommendation(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        concurrency_surfaces=concurrency_surfaces,
        missing_controls=missing_controls,
        risk_level=_risk_level(concurrency_surfaces, missing_controls),
        evidence=tuple(
            _dedupe(
                evidence
                for surface in concurrency_surfaces
                for evidence in surfaces.get(surface, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[ConcurrencySurface, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            surfaces.setdefault(surface, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[ConcurrencySurface, list[str]],
    controls: set[ConcurrencyControl],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for control, pattern in _CONTROL_PATTERNS.items():
        if pattern.search(text):
            controls.add(control)


def _risk_level(
    concurrency_surfaces: tuple[ConcurrencySurface, ...],
    missing_controls: tuple[ConcurrencyControl, ...],
) -> ConcurrencyRiskLevel:
    if not missing_controls or len(missing_controls) <= 1:
        return "low"
    high_impact = {
        "concurrent_write",
        "parallel_worker",
        "shared_queue",
        "race_condition",
        "duplicate_submission",
    }
    if high_impact.intersection(concurrency_surfaces) and len(missing_controls) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskConcurrencyControlReadinessRecommendation, ...],
    *,
    total_task_count: int,
    suppressed_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "concurrency_task_count": len(records),
        "suppressed_task_count": suppressed_task_count,
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in _RISK_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.concurrency_surfaces)
            for surface in _SURFACE_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for record in records if control in record.missing_controls)
            for control in _CONTROL_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if source is None or isinstance(source, (str, bytes)):
        return None, []
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "model_dump"):
        payload = source.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _source_payload(payload)

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
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
    for field_name in ("acceptance_criteria", "depends_on", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SURFACE_PATTERNS.values(), *_CONTROL_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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


def _only_low_signal_documentation(text_items: list[tuple[str, str]]) -> bool:
    combined = " ".join(text for _, text in text_items)
    return bool(_NEGATIVE_RE.search(combined)) and not re.search(
        r"\b(?:implement|add|update|write|worker|queue|lock|race|concurrent|parallel|"
        r"idempotency|duplicate|retry|transaction|shared state)\b",
        combined,
        re.I,
    )


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
    "ConcurrencyControl",
    "ConcurrencyRiskLevel",
    "ConcurrencySurface",
    "TaskConcurrencyControlReadinessPlan",
    "TaskConcurrencyControlReadinessRecommendation",
    "build_task_concurrency_control_readiness_plan",
    "generate_task_concurrency_control_readiness",
    "summarize_task_concurrency_control_readiness",
    "task_concurrency_control_readiness_plan_to_dict",
    "task_concurrency_control_readiness_plan_to_markdown",
    "task_concurrency_control_readiness_to_dicts",
]
