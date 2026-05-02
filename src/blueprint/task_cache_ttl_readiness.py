"""Plan cache TTL and expiration readiness for cache-touching execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CacheTTLSurface = Literal[
    "redis",
    "memcached",
    "cdn_cache",
    "browser_cache",
    "http_cache_headers",
    "object_cache",
    "computed_result_cache",
    "generic_cache",
]
CacheTTLSafeguard = Literal[
    "explicit_ttl",
    "invalidation_trigger",
    "stale_while_revalidate",
    "tenant_user_key_scope",
    "purge_tooling",
    "observability",
    "fallback_behavior",
    "test_coverage",
]
CacheTTLReadiness = Literal["strong", "moderate", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CacheTTLReadiness, int] = {"weak": 0, "moderate": 1, "strong": 2}
_SURFACE_ORDER: tuple[CacheTTLSurface, ...] = (
    "redis",
    "memcached",
    "cdn_cache",
    "browser_cache",
    "http_cache_headers",
    "object_cache",
    "computed_result_cache",
    "generic_cache",
)
_SAFEGUARD_ORDER: tuple[CacheTTLSafeguard, ...] = (
    "explicit_ttl",
    "invalidation_trigger",
    "stale_while_revalidate",
    "tenant_user_key_scope",
    "purge_tooling",
    "observability",
    "fallback_behavior",
    "test_coverage",
)
_SURFACE_PATTERNS: dict[CacheTTLSurface, re.Pattern[str]] = {
    "redis": re.compile(r"\bredis\b", re.I),
    "memcached": re.compile(r"\b(?:memcached|memcache)\b", re.I),
    "cdn_cache": re.compile(r"\b(?:cdn cache|cdn|edge cache|cloudfront|fastly|akamai|varnish)\b", re.I),
    "browser_cache": re.compile(
        r"\b(?:browser cache|service worker cache|pwa cache|localstorage|sessionstorage)\b",
        re.I,
    ),
    "http_cache_headers": re.compile(
        r"\b(?:http cache|cache-control|cache control|etag|expires header|surrogate-control|"
        r"surrogate control|max-age|s-maxage|age header)\b",
        re.I,
    ),
    "object_cache": re.compile(r"\b(?:object cache|object-cache|fragment cache|model cache|orm cache)\b", re.I),
    "computed_result_cache": re.compile(
        r"\b(?:computed result cache|computed cache|result cache|query cache|memoized|memoization|"
        r"precomputed|derived cache|cached calculation|cached aggregate)\b",
        re.I,
    ),
    "generic_cache": re.compile(r"\b(?:cache|caches|cached|caching|ttl|expiration|expiry|expire)\b", re.I),
}
_PATH_PATTERNS: dict[CacheTTLSurface, re.Pattern[str]] = {
    "redis": re.compile(r"(?:^|/)(?:redis|redis_cache)(?:[._/-]|$)", re.I),
    "memcached": re.compile(r"(?:^|/)(?:memcache|memcached)(?:[._/-]|$)", re.I),
    "cdn_cache": re.compile(r"(?:^|/)(?:cdn|edge|cloudfront|fastly|akamai|varnish)(?:[._/-]|$)", re.I),
    "browser_cache": re.compile(r"(?:^|/)(?:browser-cache|service-worker|sw|pwa|public)(?:[._/-]|$)", re.I),
    "http_cache_headers": re.compile(r"cache[_-]?control|http[_-]?cache|etag|expires|headers?", re.I),
    "object_cache": re.compile(r"object[_-]?cache|fragment[_-]?cache|model[_-]?cache|orm[_-]?cache", re.I),
    "computed_result_cache": re.compile(r"result[_-]?cache|query[_-]?cache|memoiz|computed|precomputed|aggregate", re.I),
    "generic_cache": re.compile(r"(?:^|/)(?:caches?|cache)(?:[._/-]|$)|ttl|expir", re.I),
}
_SAFEGUARD_PATTERNS: dict[CacheTTLSafeguard, re.Pattern[str]] = {
    "explicit_ttl": re.compile(
        r"\b(?:explicit ttl|ttl|time[- ]?to[- ]?live|expires? after|expiration|expiry|"
        r"max[- ]?age|s-maxage|cache duration|retention window)\b",
        re.I,
    ),
    "invalidation_trigger": re.compile(
        r"\b(?:invalidation trigger|invalidate|cache invalidation|expire keys?|bump namespace|"
        r"refresh trigger|delete cached|evict)\b",
        re.I,
    ),
    "stale_while_revalidate": re.compile(
        r"\b(?:stale[- ]while[- ]revalidate|swr|serve stale|stale fallback|background refresh|"
        r"refresh in background)\b",
        re.I,
    ),
    "tenant_user_key_scope": re.compile(
        r"\b(?:(?:tenant|user|account|organization|org|workspace)[- ]?(?:scoped|specific|aware)|"
        r"(?:tenant|user|account|organization|org|workspace).{0,40}(?:cache key|namespace)|"
        r"cache key.{0,40}(?:tenant|user|account|organization|org|workspace))\b",
        re.I,
    ),
    "purge_tooling": re.compile(
        r"\b(?:purge tool|purge command|manual purge|admin purge|emergency purge|cache purge|"
        r"flush command|runbook)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|monitoring|alerts?|dashboard|cache hit rate|miss rate|"
        r"stale rate|age metric|cache status)\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback|cache miss fallback|origin fallback|degraded mode|bypass cache|uncached path|"
        r"fail open|fail closed)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:test coverage|tests?|pytest|unit test|integration test|e2e|smoke test|regression test|"
        r"cache header test|ttl test)\b",
        re.I,
    ),
}
_SAFEGUARD_CHECKS: dict[CacheTTLSafeguard, str] = {
    "explicit_ttl": "Define the TTL or cache-header lifetime for every affected cache surface.",
    "invalidation_trigger": "Verify the event, deploy, or data-change trigger that expires stale entries.",
    "stale_while_revalidate": "Confirm stale-while-revalidate or stale-serving behavior is intentional and bounded.",
    "tenant_user_key_scope": "Review cache keys for tenant, user, account, or workspace scoping.",
    "purge_tooling": "Document purge tooling or runbook steps for emergency expiration.",
    "observability": "Add metrics, logs, or alerts for hit rate, miss rate, stale age, and purge outcomes.",
    "fallback_behavior": "Exercise cache miss, origin fallback, bypass, or degraded-mode behavior.",
    "test_coverage": "Add tests or validation evidence for TTL expiry and stale-data behavior.",
}


@dataclass(frozen=True, slots=True)
class TaskCacheTTLReadinessRecord:
    """TTL-readiness guidance for one cache-touching task."""

    task_id: str
    title: str
    cache_surfaces: tuple[CacheTTLSurface, ...]
    present_safeguards: tuple[CacheTTLSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CacheTTLSafeguard, ...] = field(default_factory=tuple)
    readiness: CacheTTLReadiness = "moderate"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    @property
    def risk_level(self) -> CacheTTLReadiness:
        """Compatibility view for planners that expose risk/readiness as one field."""
        return self.readiness

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "cache_surfaces": list(self.cache_surfaces),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskCacheTTLReadinessPlan:
    """Plan-level TTL-readiness review for cache-touching tasks."""

    plan_id: str | None = None
    records: tuple[TaskCacheTTLReadinessRecord, ...] = field(default_factory=tuple)
    cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCacheTTLReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskCacheTTLReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "cache_task_ids": list(self.cache_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return TTL-readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render TTL-readiness guidance as deterministic Markdown."""
        title = "# Task Cache TTL Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        surface_counts = self.summary.get("surface_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Cache task count: {self.summary.get('cache_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Surface counts: "
            + ", ".join(f"{surface} {surface_counts.get(surface, 0)}" for surface in _SURFACE_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task cache TTL-readiness records were inferred."])
            if self.suppressed_task_ids:
                lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Cache Surfaces | Present Safeguards | Missing Safeguards | Evidence | Recommended Checks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.cache_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} |"
            )
        if self.suppressed_task_ids:
            lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
        return "\n".join(lines)


def build_task_cache_ttl_readiness_plan(source: Any) -> TaskCacheTTLReadinessPlan:
    """Build TTL-readiness records for cache-touching execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    record_ids = {record.task_id for record in records}
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in record_ids
    )
    return TaskCacheTTLReadinessPlan(
        plan_id=plan_id,
        records=records,
        cache_task_ids=tuple(record.task_id for record in records),
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, task_count=len(tasks), suppressed_task_ids=suppressed_task_ids),
    )


def analyze_task_cache_ttl_readiness(source: Any) -> TaskCacheTTLReadinessPlan:
    """Compatibility alias for building cache TTL-readiness plans."""
    return build_task_cache_ttl_readiness_plan(source)


def summarize_task_cache_ttl_readiness(source: Any) -> TaskCacheTTLReadinessPlan:
    """Compatibility alias for building cache TTL-readiness plans."""
    return build_task_cache_ttl_readiness_plan(source)


def generate_task_cache_ttl_readiness(source: Any) -> TaskCacheTTLReadinessPlan:
    """Compatibility alias for generating cache TTL-readiness plans."""
    return build_task_cache_ttl_readiness_plan(source)


def derive_task_cache_ttl_readiness(source: Any) -> TaskCacheTTLReadinessPlan:
    """Compatibility alias for deriving cache TTL-readiness plans."""
    return build_task_cache_ttl_readiness_plan(source)


def recommend_task_cache_ttl_readiness(source: Any) -> TaskCacheTTLReadinessPlan:
    """Compatibility alias for recommending cache TTL safeguards."""
    return build_task_cache_ttl_readiness_plan(source)


def task_cache_ttl_readiness_plan_to_dict(result: TaskCacheTTLReadinessPlan) -> dict[str, Any]:
    """Serialize a cache TTL-readiness plan to a plain dictionary."""
    return result.to_dict()


task_cache_ttl_readiness_plan_to_dict.__test__ = False


def task_cache_ttl_readiness_plan_to_dicts(
    result: TaskCacheTTLReadinessPlan | Iterable[TaskCacheTTLReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize cache TTL-readiness records to plain dictionaries."""
    if isinstance(result, TaskCacheTTLReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_cache_ttl_readiness_plan_to_dicts.__test__ = False


def task_cache_ttl_readiness_plan_to_markdown(result: TaskCacheTTLReadinessPlan) -> str:
    """Render a cache TTL-readiness plan as Markdown."""
    return result.to_markdown()


task_cache_ttl_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[CacheTTLSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    safeguards: tuple[CacheTTLSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskCacheTTLReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    task_id = _task_id(task, index)
    return TaskCacheTTLReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        cache_surfaces=signals.surfaces,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.safeguards, missing),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.safeguard_evidence])),
        recommended_checks=tuple(_SAFEGUARD_CHECKS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[CacheTTLSurface] = set()
    safeguard_hits: set[CacheTTLSafeguard] = set()
    surface_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_surface = False
        matched_safeguard = False
        for surface, pattern in _PATH_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                surface_hits.add(surface)
                matched_surface = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_surface:
            surface_evidence.append(f"files_or_modules: {path}")
        if matched_safeguard:
            safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_surface = False
        matched_safeguard = False
        for surface, pattern in _SURFACE_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                surface_hits.add(surface)
                matched_surface = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_surface:
            surface_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    if surface_hits - {"generic_cache"}:
        surface_hits.discard("generic_cache")
    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _readiness(
    present: tuple[CacheTTLSafeguard, ...],
    missing: tuple[CacheTTLSafeguard, ...],
) -> CacheTTLReadiness:
    if not missing:
        return "strong"
    if "explicit_ttl" not in present:
        return "weak"
    return "moderate"


def _summary(
    records: tuple[TaskCacheTTLReadinessRecord, ...],
    *,
    task_count: int,
    suppressed_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "cache_task_count": len(records),
        "cache_task_ids": [record.task_id for record in records],
        "suppressed_task_count": len(suppressed_task_ids),
        "suppressed_task_ids": list(suppressed_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.cache_surfaces)
            for surface in _SURFACE_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
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

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
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
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
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
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    "CacheTTLReadiness",
    "CacheTTLSafeguard",
    "CacheTTLSurface",
    "TaskCacheTTLReadinessPlan",
    "TaskCacheTTLReadinessRecord",
    "analyze_task_cache_ttl_readiness",
    "build_task_cache_ttl_readiness_plan",
    "derive_task_cache_ttl_readiness",
    "generate_task_cache_ttl_readiness",
    "recommend_task_cache_ttl_readiness",
    "summarize_task_cache_ttl_readiness",
    "task_cache_ttl_readiness_plan_to_dict",
    "task_cache_ttl_readiness_plan_to_dicts",
    "task_cache_ttl_readiness_plan_to_markdown",
]
