"""Plan cache warming readiness controls for cache-heavy execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CacheWarmingRiskLevel = Literal["low", "medium", "high"]
CacheSurface = Literal[
    "cache",
    "cdn",
    "materialized_view",
    "precomputed_aggregate",
    "search_warmer",
    "cold_start_mitigation",
]
WarmingTrigger = Literal[
    "deploy_or_release",
    "launch",
    "scheduled",
    "traffic_based",
    "manual",
    "backfill",
    "cold_start",
    "user_facing",
    "high_traffic",
]
CacheWarmingControl = Literal[
    "warmup_trigger",
    "cold_start_fallback",
    "invalidation_coordination",
    "capacity_limit",
    "stale_data_guard",
    "monitoring_metric",
    "rollback_or_disable_path",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[CacheWarmingRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[CacheSurface, int] = {
    "cache": 0,
    "cdn": 1,
    "materialized_view": 2,
    "precomputed_aggregate": 3,
    "search_warmer": 4,
    "cold_start_mitigation": 5,
}
_TRIGGER_ORDER: dict[WarmingTrigger, int] = {
    "deploy_or_release": 0,
    "launch": 1,
    "scheduled": 2,
    "traffic_based": 3,
    "manual": 4,
    "backfill": 5,
    "cold_start": 6,
    "user_facing": 7,
    "high_traffic": 8,
}
_CONTROL_ORDER: dict[CacheWarmingControl, int] = {
    "warmup_trigger": 0,
    "cold_start_fallback": 1,
    "invalidation_coordination": 2,
    "capacity_limit": 3,
    "stale_data_guard": 4,
    "monitoring_metric": 5,
    "rollback_or_disable_path": 6,
}
_SURFACE_PATTERNS: dict[CacheSurface, re.Pattern[str]] = {
    "cache": re.compile(r"\b(?:caches?|caching|cached|redis|memcached|ttl|cache key|edge cache)\b", re.I),
    "cdn": re.compile(r"\b(?:cdn|cloudfront|fastly|akamai|edge network|edge asset|static asset cache)\b", re.I),
    "materialized_view": re.compile(r"\b(?:materialized views?|materialised views?|mv refresh|view refresh)\b", re.I),
    "precomputed_aggregate": re.compile(
        r"\b(?:precomputed aggregates?|pre[- ]?compute|precalculated|rollups?|summary table|aggregate table)\b",
        re.I,
    ),
    "search_warmer": re.compile(
        r"\b(?:search warmer|search warmers|warm search|search cache|index warmer|elasticsearch warmer|opensearch warmer)\b",
        re.I,
    ),
    "cold_start_mitigation": re.compile(
        r"\b(?:cold[- ]?start|cold cache|warmup|warm up|warming|prewarm|pre-warm|startup latency|first request latency)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[CacheSurface, re.Pattern[str]] = {
    "cache": re.compile(r"(?:^|/)(?:caches?|cache|redis|memcached)(?:/|$)|cache|cached|ttl", re.I),
    "cdn": re.compile(r"(?:^|/)(?:cdn|edge|cloudfront|fastly|akamai)(?:/|$)|cdn|cloudfront|fastly", re.I),
    "materialized_view": re.compile(r"materiali[sz]ed[_-]?views?|(?:^|/)mviews?(?:/|$)|mv_refresh", re.I),
    "precomputed_aggregate": re.compile(r"precomputed|precompute|aggregates?|rollups?|summary_table", re.I),
    "search_warmer": re.compile(r"search[_-]?warmer|index[_-]?warmer|(?:^|/)search(?:/|$)", re.I),
    "cold_start_mitigation": re.compile(r"cold[_-]?start|warmups?|prewarm|warming", re.I),
}
_TRIGGER_PATTERNS: dict[WarmingTrigger, re.Pattern[str]] = {
    "deploy_or_release": re.compile(r"\b(?:deploy|deployment|release|rollout|post[- ]?deploy|pre[- ]?deploy)\b", re.I),
    "launch": re.compile(r"\b(?:launch|go[- ]?live|cutover|launch[- ]?critical|release day)\b", re.I),
    "scheduled": re.compile(r"\b(?:scheduled|cron|nightly|hourly|periodic|timer)\b", re.I),
    "traffic_based": re.compile(r"\b(?:traffic[- ]?based|on demand|on[- ]?request|first request|lazy warm|request path)\b", re.I),
    "manual": re.compile(r"\b(?:manual warm|manual trigger|runbook|operator trigger|admin trigger)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|hydrate|rehydrate|seed cache|prime cache|bulk warm)\b", re.I),
    "cold_start": re.compile(r"\b(?:cold[- ]?start|cold cache|first request latency|startup latency)\b", re.I),
    "user_facing": re.compile(r"\b(?:user[- ]facing|customer[- ]facing|public|checkout|signup|homepage|production users?)\b", re.I),
    "high_traffic": re.compile(r"\b(?:high[- ]traffic|high volume|hot path|peak traffic|qps|rps|millions?|scale)\b", re.I),
}
_CONTROL_PATTERNS: dict[CacheWarmingControl, re.Pattern[str]] = {
    "warmup_trigger": re.compile(r"\b(?:warmup trigger|warm up trigger|prewarm trigger|scheduled warmup|post[- ]?deploy warm|cache priming trigger)\b", re.I),
    "cold_start_fallback": re.compile(r"\b(?:cold[- ]?start fallback|fallback path|origin fallback|degraded mode|lazy fallback|cache miss fallback)\b", re.I),
    "invalidation_coordination": re.compile(r"\b(?:invalidation coordination|coordinate invalidation|cache invalidation|purge coordination|ttl coordination|refresh coordination)\b", re.I),
    "capacity_limit": re.compile(r"\b(?:capacity limit|rate limit|throttle|concurrency limit|batch size|warmup budget|load cap)\b", re.I),
    "stale_data_guard": re.compile(r"\b(?:stale data guard|staleness guard|freshness check|max age|version check|etag|stale[- ]while[- ]revalidate)\b", re.I),
    "monitoring_metric": re.compile(r"\b(?:monitoring metric|metrics?|cache hit rate|warmup success|miss rate|latency metric|alert|dashboard)\b", re.I),
    "rollback_or_disable_path": re.compile(r"\b(?:rollback|disable path|kill switch|feature flag|turn off|disable warm|revert path)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskCacheWarmingReadinessRecommendation:
    """Cache warming readiness guidance for one affected execution task."""

    task_id: str
    title: str
    cache_surfaces: tuple[CacheSurface, ...] = field(default_factory=tuple)
    warming_triggers: tuple[WarmingTrigger, ...] = field(default_factory=tuple)
    missing_controls: tuple[CacheWarmingControl, ...] = field(default_factory=tuple)
    risk_level: CacheWarmingRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "cache_surfaces": list(self.cache_surfaces),
            "warming_triggers": list(self.warming_triggers),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCacheWarmingReadinessPlan:
    """Plan-level cache warming readiness summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskCacheWarmingReadinessRecommendation, ...] = field(default_factory=tuple)
    cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "cache_task_ids": list(self.cache_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cache warming recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]

    @property
    def records(self) -> tuple[TaskCacheWarmingReadinessRecommendation, ...]:
        """Compatibility view matching planners that name task rows records."""
        return self.recommendations

    def to_markdown(self) -> str:
        """Render the cache warming readiness plan as deterministic Markdown."""
        title = "# Task Cache Warming Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('cache_task_count', 0)} cache-warming tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(suppressed: {self.summary.get('suppressed_task_count', 0)})."
            ),
        ]
        if not self.recommendations:
            lines.extend(["", "No cache warming readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Cache Surfaces | Warming Triggers | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.cache_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.warming_triggers) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_cache_warming_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCacheWarmingReadinessPlan:
    """Build task-level cache warming readiness recommendations."""
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
    cache_task_ids = tuple(record.task_id for record in records)
    cache_task_id_set = set(cache_task_ids)
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in cache_task_id_set
    )
    return TaskCacheWarmingReadinessPlan(
        plan_id=plan_id,
        recommendations=records,
        cache_task_ids=cache_task_ids,
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, total_task_count=len(tasks), suppressed_task_count=len(suppressed_task_ids)),
    )


def generate_task_cache_warming_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskCacheWarmingReadinessRecommendation, ...]:
    """Return cache warming recommendations for relevant execution tasks."""
    return build_task_cache_warming_readiness_plan(source).recommendations


def summarize_task_cache_warming_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCacheWarmingReadinessPlan:
    """Compatibility alias for building cache warming readiness plans."""
    return build_task_cache_warming_readiness_plan(source)


def task_cache_warming_readiness_plan_to_dict(
    result: TaskCacheWarmingReadinessPlan,
) -> dict[str, Any]:
    """Serialize a cache warming readiness plan to a plain dictionary."""
    return result.to_dict()


task_cache_warming_readiness_plan_to_dict.__test__ = False


def task_cache_warming_readiness_to_dicts(
    records: (
        tuple[TaskCacheWarmingReadinessRecommendation, ...]
        | list[TaskCacheWarmingReadinessRecommendation]
        | TaskCacheWarmingReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize cache warming readiness recommendations to dictionaries."""
    if isinstance(records, TaskCacheWarmingReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_cache_warming_readiness_to_dicts.__test__ = False


def task_cache_warming_readiness_plan_to_markdown(
    result: TaskCacheWarmingReadinessPlan,
) -> str:
    """Render a cache warming readiness plan as Markdown."""
    return result.to_markdown()


task_cache_warming_readiness_plan_to_markdown.__test__ = False


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskCacheWarmingReadinessRecommendation | None:
    surfaces: dict[CacheSurface, list[str]] = {}
    triggers: dict[WarmingTrigger, list[str]] = {}
    controls: set[CacheWarmingControl] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces, triggers)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, triggers, controls)

    if not surfaces:
        return None

    cache_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    warming_triggers = tuple(trigger for trigger in _TRIGGER_ORDER if trigger in triggers)
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    task_id = _task_id(task, index)
    return TaskCacheWarmingReadinessRecommendation(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        cache_surfaces=cache_surfaces,
        warming_triggers=warming_triggers,
        missing_controls=missing_controls,
        risk_level=_risk_level(cache_surfaces, warming_triggers, missing_controls),
        evidence=tuple(
            _dedupe(
                evidence
                for key in (*cache_surfaces, *warming_triggers)
                for evidence in (surfaces.get(key, []) if key in surfaces else triggers.get(key, []))
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[CacheSurface, list[str]],
    triggers: dict[WarmingTrigger, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            surfaces.setdefault(surface, []).append(evidence)
    for trigger, pattern in _TRIGGER_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            triggers.setdefault(trigger, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[CacheSurface, list[str]],
    triggers: dict[WarmingTrigger, list[str]],
    controls: set[CacheWarmingControl],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for trigger, pattern in _TRIGGER_PATTERNS.items():
        if pattern.search(text):
            triggers.setdefault(trigger, []).append(evidence)
    for control, pattern in _CONTROL_PATTERNS.items():
        if pattern.search(text):
            controls.add(control)


def _risk_level(
    cache_surfaces: tuple[CacheSurface, ...],
    warming_triggers: tuple[WarmingTrigger, ...],
    missing_controls: tuple[CacheWarmingControl, ...],
) -> CacheWarmingRiskLevel:
    if not missing_controls:
        return "low"
    if (
        "cold_start_mitigation" in cache_surfaces
        or any(trigger in warming_triggers for trigger in ("launch", "user_facing", "high_traffic", "cold_start"))
    ):
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskCacheWarmingReadinessRecommendation, ...],
    *,
    total_task_count: int,
    suppressed_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "cache_task_count": len(records),
        "suppressed_task_count": suppressed_task_count,
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in ("high", "medium", "low")
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.cache_surfaces)
            for surface in _SURFACE_ORDER
        },
        "warming_trigger_counts": {
            trigger: sum(1 for record in records if trigger in record.warming_triggers)
            for trigger in _TRIGGER_ORDER
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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SURFACE_PATTERNS.values(), *_TRIGGER_PATTERNS.values(), *_CONTROL_PATTERNS.values())
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
    "CacheSurface",
    "CacheWarmingControl",
    "CacheWarmingRiskLevel",
    "TaskCacheWarmingReadinessPlan",
    "TaskCacheWarmingReadinessRecommendation",
    "WarmingTrigger",
    "build_task_cache_warming_readiness_plan",
    "generate_task_cache_warming_readiness",
    "summarize_task_cache_warming_readiness",
    "task_cache_warming_readiness_plan_to_dict",
    "task_cache_warming_readiness_plan_to_markdown",
    "task_cache_warming_readiness_to_dicts",
]
