"""Assess task readiness for CDN and edge-cache implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CdnCacheBehavior = Literal[
    "cdn_cache",
    "edge_cache",
    "http_cache_headers",
    "surrogate_keys",
    "stale_while_revalidate",
    "purge_invalidation",
    "signed_urls",
    "asset_versioning",
    "regional_edge_rollout",
]
CdnCacheSafeguard = Literal[
    "ttl_policy",
    "invalidation_paths",
    "private_data_protection",
    "signed_url_policy",
    "rollout_monitoring",
    "cache_key_design",
    "fallback_behavior",
]
CdnCacheReadiness = Literal["strong", "moderate", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CdnCacheReadiness, int] = {"weak": 0, "moderate": 1, "strong": 2}
_BEHAVIOR_ORDER: tuple[CdnCacheBehavior, ...] = (
    "cdn_cache",
    "edge_cache",
    "http_cache_headers",
    "surrogate_keys",
    "stale_while_revalidate",
    "purge_invalidation",
    "signed_urls",
    "asset_versioning",
    "regional_edge_rollout",
)
_SAFEGUARD_ORDER: tuple[CdnCacheSafeguard, ...] = (
    "ttl_policy",
    "invalidation_paths",
    "private_data_protection",
    "signed_url_policy",
    "rollout_monitoring",
    "cache_key_design",
    "fallback_behavior",
)
_BEHAVIOR_PATTERNS: dict[CdnCacheBehavior, re.Pattern[str]] = {
    "cdn_cache": re.compile(r"\b(?:cdn|cloudfront|fastly|akamai|varnish|cloudflare)\b", re.I),
    "edge_cache": re.compile(r"\b(?:edge cache|edge caching|edge worker|edge function|edge location|pop)\b", re.I),
    "http_cache_headers": re.compile(
        r"\b(?:cache-control|cache control|surrogate-control|surrogate control|etag|expires header|"
        r"max-age|s-maxage|public cache|private cache|vary header|cache header)\b",
        re.I,
    ),
    "surrogate_keys": re.compile(r"\b(?:surrogate key|surrogate-key|surrogate keys|cache tag|cache tags)\b", re.I),
    "stale_while_revalidate": re.compile(
        r"\b(?:stale[- ]while[- ]revalidate|swr|serve stale|stale-if-error|background revalidation)\b",
        re.I,
    ),
    "purge_invalidation": re.compile(
        r"\b(?:purge|cache purge|invalidate|invalidation|ban request|soft purge|hard purge|edge flush)\b",
        re.I,
    ),
    "signed_urls": re.compile(
        r"\b(?:signed url|signed urls|signed cookie|signed cookies|private cdn|tokenized url|"
        r"tokenized link|presigned url|pre-signed url)\b",
        re.I,
    ),
    "asset_versioning": re.compile(
        r"\b(?:asset versioning|versioned asset|versioned url|fingerprint(?:ed)? asset|"
        r"content hash|hashed filename|cache bust(?:ing)?|asset manifest)\b",
        re.I,
    ),
    "regional_edge_rollout": re.compile(
        r"\b(?:regional edge|edge region|region rollout|regional rollout|canary region|"
        r"edge rollout|multi-region edge|pop rollout|regional cache)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[CdnCacheBehavior, re.Pattern[str]] = {
    "cdn_cache": re.compile(r"(?:^|/)(?:cdn|cloudfront|fastly|akamai|cloudflare|varnish)(?:[._/-]|$)", re.I),
    "edge_cache": re.compile(r"(?:^|/)(?:edge|edge-cache|workers?)(?:[._/-]|$)", re.I),
    "http_cache_headers": re.compile(r"cache[_-]?control|surrogate[_-]?control|etag|cache[_-]?headers?", re.I),
    "surrogate_keys": re.compile(r"surrogate[_-]?keys?|cache[_-]?tags?", re.I),
    "stale_while_revalidate": re.compile(r"stale[_-]?while[_-]?revalidate|swr|stale[_-]?if[_-]?error", re.I),
    "purge_invalidation": re.compile(r"purge|invalidat|edge[_-]?flush", re.I),
    "signed_urls": re.compile(r"signed[_-]?(?:urls?|cookies?)|presigned|private[_-]?cdn", re.I),
    "asset_versioning": re.compile(r"(?:^|/)(?:assets?|static|public|manifest)(?:[._/-]|$)|fingerprint|hash|version", re.I),
    "regional_edge_rollout": re.compile(r"regional[_-]?edge|edge[_-]?regions?|pop[_-]?rollout|regional[_-]?rollout", re.I),
}
_SAFEGUARD_PATTERNS: dict[CdnCacheSafeguard, re.Pattern[str]] = {
    "ttl_policy": re.compile(
        r"\b(?:ttl|time[- ]?to[- ]?live|max-age|s-maxage|cache duration|expiration|expiry|"
        r"surrogate-control|cache-control lifetime)\b",
        re.I,
    ),
    "invalidation_paths": re.compile(
        r"\b(?:purge|invalidate|invalidation|surrogate key|cache tag|cache tags|ban request|"
        r"soft purge|hard purge|deploy purge|publish purge)\b",
        re.I,
    ),
    "private_data_protection": re.compile(
        r"\b(?:private data|personal data|pii|authenticated|authorization|cookie|session|tenant|user-specific|"
        r"account-specific|private cache|no-store|no-cache|leakage|data leak|vary authorization|vary cookie)\b",
        re.I,
    ),
    "signed_url_policy": re.compile(
        r"\b(?:signed url|signed urls|signed cookie|signed cookies|tokenized url|presigned url|"
        r"expiry token|url signature|signature expiry)\b",
        re.I,
    ),
    "rollout_monitoring": re.compile(
        r"\b(?:monitoring|metrics?|alerts?|dashboard|cache hit rate|cache status|origin error|"
        r"purge outcome|edge logs?|canary|regional rollout|rollback|synthetic check)\b",
        re.I,
    ),
    "cache_key_design": re.compile(
        r"\b(?:cache key|cache keys|vary header|vary by|query string|header normalization|cookie normalization|"
        r"tenant scope|user scope|device variant|locale variant|key design)\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback|origin fallback|stale-if-error|serve stale|bypass cache|cache miss|"
        r"uncached path|fail open|fail closed|degraded mode)\b",
        re.I,
    ),
}
_SAFEGUARD_RECOMMENDATIONS: dict[CdnCacheSafeguard, str] = {
    "ttl_policy": "Define explicit browser, CDN, and surrogate TTLs for each cached response class.",
    "invalidation_paths": "Document purge or invalidation paths, affected keys/tags, and emergency runbook steps.",
    "private_data_protection": "Verify private, authenticated, tenant, and user-specific data cannot be stored in shared edge caches.",
    "signed_url_policy": "Define signed URL or cookie expiry, key rotation, and unauthorized-access validation.",
    "rollout_monitoring": "Add rollout monitoring for hit rate, cache status, origin errors, purge outcomes, and regional canaries.",
    "cache_key_design": "Specify cache key inputs, Vary behavior, query/header normalization, and tenant or locale scoping.",
    "fallback_behavior": "Exercise origin fallback, bypass, cache-miss, and stale-if-error behavior before rollout.",
}


@dataclass(frozen=True, slots=True)
class TaskCdnCacheReadinessRecord:
    """CDN and edge-cache readiness guidance for one task."""

    task_id: str
    title: str
    cache_behaviors: tuple[CdnCacheBehavior, ...]
    present_safeguards: tuple[CdnCacheSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CdnCacheSafeguard, ...] = field(default_factory=tuple)
    readiness: CdnCacheReadiness = "moderate"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def risk_level(self) -> CdnCacheReadiness:
        """Compatibility view for callers that expect a risk/readiness field."""
        return self.readiness

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "cache_behaviors": list(self.cache_behaviors),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommendations": list(self.recommendations),
        }


@dataclass(frozen=True, slots=True)
class TaskCdnCacheReadinessPlan:
    """Plan-level CDN cache readiness review."""

    plan_id: str | None = None
    records: tuple[TaskCdnCacheReadinessRecord, ...] = field(default_factory=tuple)
    cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCdnCacheReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskCdnCacheReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose recommendations."""
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
        """Return CDN-readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render CDN-readiness guidance as deterministic Markdown."""
        title = "# Task CDN Cache Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        behavior_counts = self.summary.get("behavior_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- CDN cache task count: {self.summary.get('cache_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Behavior counts: "
            + ", ".join(f"{behavior} {behavior_counts.get(behavior, 0)}" for behavior in _BEHAVIOR_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No CDN or edge-cache readiness records were inferred."])
            if self.suppressed_task_ids:
                lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Cache Behaviors | Present Safeguards | Missing Safeguards | Recommendations |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.cache_behaviors) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommendations) or 'none')} |"
            )
        if self.suppressed_task_ids:
            lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
        return "\n".join(lines)


def build_task_cdn_cache_readiness_plan(source: Any) -> TaskCdnCacheReadinessPlan:
    """Build CDN and edge-cache readiness records for relevant execution tasks."""
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
    return TaskCdnCacheReadinessPlan(
        plan_id=plan_id,
        records=records,
        cache_task_ids=tuple(record.task_id for record in records),
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, task_count=len(tasks), suppressed_task_ids=suppressed_task_ids),
    )


def analyze_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for building CDN cache readiness plans."""
    return build_task_cdn_cache_readiness_plan(source)


def extract_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for extracting CDN cache readiness plans."""
    return build_task_cdn_cache_readiness_plan(source)


def generate_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for generating CDN cache readiness plans."""
    return build_task_cdn_cache_readiness_plan(source)


def recommend_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for recommending CDN cache safeguards."""
    return build_task_cdn_cache_readiness_plan(source)


def summarize_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for summarizing CDN cache readiness plans."""
    return build_task_cdn_cache_readiness_plan(source)


def derive_task_cdn_cache_readiness(source: Any) -> TaskCdnCacheReadinessPlan:
    """Compatibility alias for deriving CDN cache readiness plans."""
    return build_task_cdn_cache_readiness_plan(source)


def task_cdn_cache_readiness_plan_to_dict(result: TaskCdnCacheReadinessPlan) -> dict[str, Any]:
    """Serialize a CDN cache readiness plan to a plain dictionary."""
    return result.to_dict()


task_cdn_cache_readiness_plan_to_dict.__test__ = False


def task_cdn_cache_readiness_plan_to_dicts(
    result: TaskCdnCacheReadinessPlan | Iterable[TaskCdnCacheReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize CDN cache readiness records to plain dictionaries."""
    if isinstance(result, TaskCdnCacheReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_cdn_cache_readiness_plan_to_dicts.__test__ = False


def task_cdn_cache_readiness_plan_to_markdown(result: TaskCdnCacheReadinessPlan) -> str:
    """Render a CDN cache readiness plan as Markdown."""
    return result.to_markdown()


task_cdn_cache_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    behaviors: tuple[CdnCacheBehavior, ...] = field(default_factory=tuple)
    behavior_evidence: tuple[str, ...] = field(default_factory=tuple)
    safeguards: tuple[CdnCacheSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskCdnCacheReadinessRecord | None:
    signals = _signals(task)
    if not signals.behaviors:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    task_id = _task_id(task, index)
    return TaskCdnCacheReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        cache_behaviors=signals.behaviors,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.safeguards, missing),
        evidence=tuple(_dedupe([*signals.behavior_evidence, *signals.safeguard_evidence])),
        recommendations=tuple(_SAFEGUARD_RECOMMENDATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    behavior_hits: set[CdnCacheBehavior] = set()
    safeguard_hits: set[CdnCacheSafeguard] = set()
    behavior_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_behavior = False
        matched_safeguard = False
        for behavior, pattern in _PATH_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                behavior_hits.add(behavior)
                matched_behavior = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_behavior:
            behavior_evidence.append(f"files_or_modules: {path}")
        if matched_safeguard:
            safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_behavior = False
        matched_safeguard = False
        for behavior, pattern in _BEHAVIOR_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                behavior_hits.add(behavior)
                matched_behavior = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_behavior:
            behavior_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        behaviors=tuple(behavior for behavior in _BEHAVIOR_ORDER if behavior in behavior_hits),
        behavior_evidence=tuple(_dedupe(behavior_evidence)),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _readiness(
    present: tuple[CdnCacheSafeguard, ...],
    missing: tuple[CdnCacheSafeguard, ...],
) -> CdnCacheReadiness:
    if not missing:
        return "strong"
    required = {"ttl_policy", "invalidation_paths", "private_data_protection", "cache_key_design"}
    if required - set(present):
        return "weak"
    return "moderate"


def _summary(
    records: tuple[TaskCdnCacheReadinessRecord, ...],
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
        "behavior_counts": {
            behavior: sum(1 for record in records if behavior in record.cache_behaviors)
            for behavior in _BEHAVIOR_ORDER
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
    return any(pattern.search(value) for pattern in [*_BEHAVIOR_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "CdnCacheBehavior",
    "CdnCacheReadiness",
    "CdnCacheSafeguard",
    "TaskCdnCacheReadinessPlan",
    "TaskCdnCacheReadinessRecord",
    "analyze_task_cdn_cache_readiness",
    "build_task_cdn_cache_readiness_plan",
    "derive_task_cdn_cache_readiness",
    "extract_task_cdn_cache_readiness",
    "generate_task_cdn_cache_readiness",
    "recommend_task_cdn_cache_readiness",
    "summarize_task_cdn_cache_readiness",
    "task_cdn_cache_readiness_plan_to_dict",
    "task_cdn_cache_readiness_plan_to_dicts",
    "task_cdn_cache_readiness_plan_to_markdown",
]
