"""Infer cache invalidation requirements for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CacheLayer = Literal[
    "cdn",
    "browser_cache",
    "redis",
    "memcached",
    "orm_query_cache",
    "build_artifact_cache",
    "feature_flag_cache",
    "api_response_cache",
]
StaleDataRisk = Literal["none", "low", "medium", "high"]
_T = TypeVar("_T")

_LAYER_ORDER: tuple[CacheLayer, ...] = (
    "cdn",
    "browser_cache",
    "redis",
    "memcached",
    "orm_query_cache",
    "build_artifact_cache",
    "feature_flag_cache",
    "api_response_cache",
)
_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_LAYER_PATTERNS: dict[CacheLayer, tuple[re.Pattern[str], ...]] = {
    "cdn": (
        re.compile(r"\b(?:cdn|edge cache|cloudfront|fastly|akamai|varnish)\b", re.IGNORECASE),
    ),
    "browser_cache": (
        re.compile(
            r"\b(?:browser cache|cache-control|etag|service worker|localstorage|"
            r"sessionstorage|web cache)\b",
            re.IGNORECASE,
        ),
    ),
    "redis": (re.compile(r"\bredis\b", re.IGNORECASE),),
    "memcached": (re.compile(r"\b(?:memcached|memcache)\b", re.IGNORECASE),),
    "orm_query_cache": (
        re.compile(
            r"\b(?:orm|query cache|queryset cache|database-backed read|db-backed read|"
            r"read model|materialized view|select query|repository read|sql read)\b",
            re.IGNORECASE,
        ),
    ),
    "build_artifact_cache": (
        re.compile(
            r"\b(?:build cache|artifact cache|build artifact|webpack|vite|next\.js|"
            r"static asset|asset manifest|dist/|bundle|compiled asset)\b",
            re.IGNORECASE,
        ),
    ),
    "feature_flag_cache": (
        re.compile(
            r"\b(?:feature flag|feature flags|flag cache|launchdarkly|configcat|split\.io|"
            r"flagsmith|flipper)\b",
            re.IGNORECASE,
        ),
    ),
    "api_response_cache": (
        re.compile(
            r"\b(?:api response cache|response cache|http cache|rest endpoint|graphql|"
            r"openapi|api endpoint|endpoint|controller|route handler)\b",
            re.IGNORECASE,
        ),
    ),
}
_PATH_HINTS: tuple[tuple[CacheLayer, re.Pattern[str]], ...] = (
    ("cdn", re.compile(r"(?:^|/)(?:cdn|edge|cloudfront|fastly|akamai)(?:/|$)", re.IGNORECASE)),
    ("browser_cache", re.compile(r"(?:^|/)(?:service-worker|sw|pwa|public)(?:[./]|/|$)", re.IGNORECASE)),
    ("redis", re.compile(r"(?:^|/)(?:redis|cache)(?:[._/-]|$)", re.IGNORECASE)),
    ("memcached", re.compile(r"(?:^|/)(?:memcache|memcached)(?:[._/-]|$)", re.IGNORECASE)),
    ("orm_query_cache", re.compile(r"(?:^|/)(?:models|queries|repositories|dao)(?:/|$)", re.IGNORECASE)),
    (
        "build_artifact_cache",
        re.compile(r"(?:^|/)(?:assets|static|dist|build|public)(?:/|$)", re.IGNORECASE),
    ),
    ("feature_flag_cache", re.compile(r"(?:^|/)(?:flags|feature_flags|features)(?:[._/-]|$)", re.IGNORECASE)),
    ("api_response_cache", re.compile(r"(?:^|/)(?:api|routes|controllers|graphql)(?:/|$)", re.IGNORECASE)),
)
_ASSET_RE = re.compile(
    r"\b(?:asset|assets|static file|image|css|javascript|js bundle|font|manifest)\b",
    re.IGNORECASE,
)
_API_RE = re.compile(r"\b(?:api|endpoint|route|controller|graphql|rest|openapi)\b", re.IGNORECASE)
_DB_READ_RE = re.compile(
    r"\b(?:database-backed read|db-backed read|query|read model|repository|select|orm)\b",
    re.IGNORECASE,
)
_FLAG_RE = re.compile(
    r"\b(?:feature flag|feature flags|flag cache|feature gate|rollout rule|launchdarkly)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskCacheInvalidationGuidance:
    """Cache invalidation guidance for one task."""

    task_id: str
    title: str
    cache_layers: tuple[CacheLayer, ...] = field(default_factory=tuple)
    invalidation_strategy: str = "No cache invalidation required."
    stale_data_risk: StaleDataRisk = "none"
    risk_reasons: tuple[str, ...] = field(default_factory=tuple)
    rollback_considerations: tuple[str, ...] = field(default_factory=tuple)
    validation_hints: tuple[str, ...] = field(default_factory=tuple)
    evidence_hints: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "cache_layers": list(self.cache_layers),
            "invalidation_strategy": self.invalidation_strategy,
            "stale_data_risk": self.stale_data_risk,
            "risk_reasons": list(self.risk_reasons),
            "rollback_considerations": list(self.rollback_considerations),
            "validation_hints": list(self.validation_hints),
            "evidence_hints": list(self.evidence_hints),
        }


@dataclass(frozen=True, slots=True)
class TaskCacheInvalidationPlan:
    """Per-task cache invalidation requirements for an execution plan."""

    plan_id: str | None = None
    tasks: tuple[TaskCacheInvalidationGuidance, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "tasks": [task.to_dict() for task in self.tasks],
        }

    def to_markdown(self) -> str:
        """Render cache invalidation guidance as deterministic Markdown."""
        title = "# Task Cache Invalidation"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.tasks:
            lines.extend(["", "No tasks were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Guidance",
                "",
                "| Task | Layers | Risk | Strategy | Validation |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for task in self.tasks:
            lines.append(
                "| "
                f"{_markdown_cell(task.task_id)} | "
                f"{_markdown_cell(', '.join(task.cache_layers) or 'none')} | "
                f"{task.stale_data_risk} | "
                f"{_markdown_cell(task.invalidation_strategy)} | "
                f"{_markdown_cell('; '.join(task.validation_hints) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_cache_invalidation_plan(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskCacheInvalidationPlan:
    """Infer cache invalidation guidance for every task in an execution plan."""
    plan_id, tasks = _source_payload(source)
    records = _task_records(tasks)
    return TaskCacheInvalidationPlan(
        plan_id=plan_id,
        tasks=tuple(_guidance_for_record(record) for record in records),
    )


def derive_task_cache_invalidation_plan(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskCacheInvalidationPlan:
    """Compatibility alias for building task cache invalidation guidance."""
    return build_task_cache_invalidation_plan(source)


def task_cache_invalidation_to_dict(plan: TaskCacheInvalidationPlan) -> dict[str, Any]:
    """Serialize task cache invalidation guidance to a plain dictionary."""
    return plan.to_dict()


task_cache_invalidation_to_dict.__test__ = False


def task_cache_invalidation_to_markdown(plan: TaskCacheInvalidationPlan) -> str:
    """Render task cache invalidation guidance as Markdown."""
    return plan.to_markdown()


task_cache_invalidation_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    risk_level: str
    validation_commands: tuple[str, ...]
    context: str


def _guidance_for_record(record: _TaskRecord) -> TaskCacheInvalidationGuidance:
    layers = _cache_layers(record)
    risk_reasons = tuple(_risk_reasons(record, layers))
    stale_data_risk = _stale_data_risk(record, layers, risk_reasons)
    return TaskCacheInvalidationGuidance(
        task_id=record.task_id,
        title=record.title,
        cache_layers=layers,
        invalidation_strategy=_invalidation_strategy(layers),
        stale_data_risk=stale_data_risk,
        risk_reasons=risk_reasons,
        rollback_considerations=tuple(_rollback_considerations(layers, stale_data_risk)),
        validation_hints=tuple(_validation_hints(record, layers)),
        evidence_hints=tuple(_evidence_hints(layers, stale_data_risk)),
    )


def _cache_layers(record: _TaskRecord) -> tuple[CacheLayer, ...]:
    context = record.context
    layers: list[CacheLayer] = []
    for layer in _LAYER_ORDER:
        if any(pattern.search(context) for pattern in _LAYER_PATTERNS[layer]):
            layers.append(layer)

    paths = " ".join(_strings(record.task.get("files_or_modules") or record.task.get("files")))
    for layer, pattern in _PATH_HINTS:
        if pattern.search(paths):
            layers.append(layer)

    if _ASSET_RE.search(context):
        layers.extend(["cdn", "browser_cache", "build_artifact_cache"])
    if _API_RE.search(context):
        layers.append("api_response_cache")
    if _DB_READ_RE.search(context):
        layers.append("orm_query_cache")
    if _FLAG_RE.search(context):
        layers.append("feature_flag_cache")

    return tuple(_dedupe(layer for layer in _LAYER_ORDER if layer in set(layers)))


def _risk_reasons(record: _TaskRecord, layers: tuple[CacheLayer, ...]) -> list[str]:
    reasons: list[str] = []
    context = record.context
    if not layers:
        return reasons
    if "cdn" in layers or "browser_cache" in layers:
        reasons.append("user-visible stale assets or edge responses")
    if "api_response_cache" in layers:
        reasons.append("cached API responses can outlive contract or payload changes")
    if "feature_flag_cache" in layers:
        reasons.append("cached flag state can delay rollout or rollback")
    if "redis" in layers or "memcached" in layers:
        reasons.append("shared in-memory cache may serve stale computed data")
    if "orm_query_cache" in layers:
        reasons.append("database-backed reads may retain stale query results")
    if "build_artifact_cache" in layers:
        reasons.append("build artifact cache can preserve old bundles or manifests")
    if record.risk_level in _HIGH_RISK_VALUES:
        reasons.append(f"{record.risk_level} task risk")
    if re.search(r"\b(?:schema|migration|backfill|data model|contract|payload)\b", context, re.IGNORECASE):
        reasons.append("data or contract shape change")
    return _dedupe(reasons)


def _stale_data_risk(
    record: _TaskRecord,
    layers: tuple[CacheLayer, ...],
    risk_reasons: tuple[str, ...],
) -> StaleDataRisk:
    if not layers:
        return "none"
    if record.risk_level in _HIGH_RISK_VALUES:
        return "high"
    if any(layer in layers for layer in ("cdn", "redis", "memcached", "feature_flag_cache")):
        return "high" if "data or contract shape change" in risk_reasons else "medium"
    if any(layer in layers for layer in ("api_response_cache", "orm_query_cache")):
        return "medium"
    return "low"


def _invalidation_strategy(layers: tuple[CacheLayer, ...]) -> str:
    if not layers:
        return "No cache invalidation required; keep normal validation evidence."
    strategies = {
        "cdn": "purge affected CDN keys or surrogate tags",
        "browser_cache": "version asset URLs and review Cache-Control, ETag, or service-worker behavior",
        "redis": "delete or rotate affected Redis keys with namespace-scoped commands",
        "memcached": "delete affected memcached keys or bump the namespace version",
        "orm_query_cache": "clear query cache entries tied to changed models, filters, or read paths",
        "build_artifact_cache": "clear CI/build artifact cache for changed bundles and manifests",
        "feature_flag_cache": "refresh flag provider caches and confirm SDK polling/streaming propagation",
        "api_response_cache": "expire API response keys for changed routes, payloads, and vary headers",
    }
    return "; ".join(strategies[layer] for layer in layers) + "."


def _rollback_considerations(
    layers: tuple[CacheLayer, ...],
    stale_data_risk: StaleDataRisk,
) -> list[str]:
    if not layers:
        return ["No cache-specific rollback step expected."]
    considerations = ["Document the cache keys, tags, or namespaces touched by rollback."]
    if "cdn" in layers or "browser_cache" in layers:
        considerations.append("Keep prior assets addressable until CDN and browser TTLs have expired.")
    if "feature_flag_cache" in layers:
        considerations.append("Confirm flag rollback propagates through provider and SDK caches.")
    if "redis" in layers or "memcached" in layers or "orm_query_cache" in layers:
        considerations.append("Make rollback idempotent if old and new cache entries coexist.")
    if stale_data_risk == "high":
        considerations.append("Prepare an emergency purge command before rollout.")
    return considerations


def _validation_hints(record: _TaskRecord, layers: tuple[CacheLayer, ...]) -> list[str]:
    hints = list(record.validation_commands)
    if not layers:
        return _dedupe(hints)
    if "cdn" in layers:
        hints.append("Capture CDN purge request or surrogate-key invalidation evidence.")
    if "browser_cache" in layers:
        hints.append("Verify fresh load with cache disabled and a normal cached reload.")
    if "redis" in layers:
        hints.append("Inspect affected Redis keys before and after invalidation.")
    if "memcached" in layers:
        hints.append("Inspect affected memcached keys or namespace version after invalidation.")
    if "orm_query_cache" in layers:
        hints.append("Run stale-read regression checks against changed database-backed reads.")
    if "build_artifact_cache" in layers:
        hints.append("Confirm rebuilt artifact digest or manifest version changed.")
    if "feature_flag_cache" in layers:
        hints.append("Confirm flag value refresh across rollout and rollback cohorts.")
    if "api_response_cache" in layers:
        hints.append("Compare cached and uncached API responses for changed routes.")
    return _dedupe(hints)


def _evidence_hints(
    layers: tuple[CacheLayer, ...],
    stale_data_risk: StaleDataRisk,
) -> list[str]:
    if not layers:
        return ["Normal task validation is sufficient; no cache evidence needed."]
    hints = ["List cache layers, keys/tags/namespaces, TTLs, and purge timing in handoff notes."]
    if stale_data_risk in {"medium", "high"}:
        hints.append("Attach before/after evidence that stale data was not served after deployment.")
    if "api_response_cache" in layers:
        hints.append("Record response headers or cache-status evidence for changed API routes.")
    return hints


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    for item in source:
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


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk_level"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk"))
                    or "unspecified"
                ).casefold(),
                validation_commands=tuple(_validation_commands(task)),
                context=_task_context(task),
            )
        )
    return tuple(records)


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
    ):
        if text := _optional_text(task.get(field_name)):
            values.append(text)
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(_strings(task.get("acceptance_criteria")))
    values.extend(_metadata_texts(task.get("metadata")))
    return " ".join(values)


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.append(str(key))
            texts.extend(_metadata_texts(value[key]))
        return texts
    return _strings(value)


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "CacheLayer",
    "StaleDataRisk",
    "TaskCacheInvalidationGuidance",
    "TaskCacheInvalidationPlan",
    "build_task_cache_invalidation_plan",
    "derive_task_cache_invalidation_plan",
    "task_cache_invalidation_to_dict",
    "task_cache_invalidation_to_markdown",
]
