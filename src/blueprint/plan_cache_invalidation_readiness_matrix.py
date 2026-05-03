"""Build plan-level cache invalidation readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CacheInvalidationReadiness = Literal["ready", "partial", "blocked"]
CacheInvalidationSeverity = Literal["high", "medium", "low"]

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CacheInvalidationReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[CacheInvalidationSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CACHE_RE = re.compile(
    r"\b(?:cache|cached|caching|redis|memcached|cdn|edge cache|browser cache|http cache|"
    r"object cache|fragment cache|query cache|result cache|surrogate key|cache key|ttl)\b",
    re.I,
)
_INVALIDATION_RE = re.compile(
    r"\b(?:invalidate|invalidation|evict|eviction|expire|purge|flush|bust|cache bust|"
    r"delete cached|refresh cache|revalidate|bump namespace|version(?:ed)? key|clear cache)\b",
    re.I,
)
_OWNER_RE = re.compile(r"\b(?:owner|owners|dri|responsible|assignee|team|lead|sre|platform|on[- ]?call)\b", re.I)
_TRIGGER_RE = re.compile(
    r"\b(?:trigger|event|hook|webhook|publish|deploy|data change|write path|mutation|update|delete|"
    r"create|commit|after save|source of truth|invalidation rule|purge path)\b",
    re.I,
)
_TTL_RE = re.compile(
    r"\b(?:ttl|time[- ]?to[- ]?live|staleness|stale|expiration|expiry|expire after|max[- ]?age|"
    r"s-maxage|stale[- ]while[- ]revalidate|swr|freshness window|cache duration)\b",
    re.I,
)
_DEPENDENCY_RE = re.compile(
    r"\b(?:depend(?:s|ency|encies)|ordering|order|sequence|before|after|upstream|downstream|"
    r"cascade|fanout|consumer|producer|topolog(?:y|ical)|source first)\b",
    re.I,
)
_WARMING_RE = re.compile(
    r"\b(?:backfill|warm(?:ing)?|warmup|prewarm|pre-warm|prime|preload|hydrate|rebuild|"
    r"recompute|regenerate|migration replay|cold start|cache fill)\b",
    re.I,
)
_OBSERVABILITY_RE = re.compile(
    r"\b(?:observability|metrics?|monitoring|alerts?|dashboard|logs?|telemetry|hit rate|miss rate|"
    r"stale rate|purge outcome|invalidation count|cache age|synthetic)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|fallback|fall back|revert|restore|bypass cache|uncached path|"
    r"disable cache|feature flag|kill[- ]switch|manual purge|runbook|fail open|fail closed)\b",
    re.I,
)
_CUSTOMER_IMPACT_RE = re.compile(
    r"\b(?:customer|user|tenant|account|visible|impact|stale data|incorrect data|consistency|"
    r"freshness|downtime|degraded|sla|slo|support|notification)\b",
    re.I,
)
_EXPLICIT_GAP_RE = re.compile(r"\b(?:gap|missing|unknown|unresolved|tbd|todo|not documented|not defined)\b", re.I)
_SURFACE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:cache surface|surface|keyspace|namespace|cache keyspace|cache namespace)\s*[:=]\s*[`'\"]?([a-z0-9][\w./:-]{1,})", re.I),
    re.compile(r"\b(?:cache|redis|memcached|cdn|edge|browser|http|object|fragment|query|result)\s+(?:keyspace|namespace|keys?|surface)\s+[`'\"]?([a-z0-9][\w./:-]{1,})", re.I),
    re.compile(r"\b[`'\"]?([a-z0-9][\w./:-]{1,})[`'\"]?\s+(?:cache|keyspace|namespace|surrogate key|cache key)", re.I),
    re.compile(r"\b(?:invalidate|purge|evict|flush|expire|bust|revalidate)\s+(?:the\s+)?[`'\"]?([a-z0-9][\w./:-]{1,})", re.I),
)
_SURFACE_STOPWORDS = {
    "cache",
    "cached",
    "cdn",
    "edge",
    "browser",
    "http",
    "redis",
    "memcached",
    "key",
    "keys",
    "keyspace",
    "namespace",
    "surface",
    "the",
    "all",
    "stale",
    "after",
    "event",
    "when",
    "with",
    "and",
}


@dataclass(frozen=True, slots=True)
class PlanCacheInvalidationReadinessRow:
    """One grouped cache invalidation readiness row."""

    cache_surface: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    owner: str = "missing"
    invalidation_trigger: str = "missing"
    ttl_staleness_policy: str = "missing"
    dependency_ordering: str = "missing"
    backfill_warming: str = "missing"
    observability: str = "missing"
    rollback_fallback: str = "missing"
    customer_impact: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: CacheInvalidationReadiness = "partial"
    severity: CacheInvalidationSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def cache_keyspace(self) -> str:
        """Compatibility alias for callers that name grouped surfaces keyspaces."""
        return self.cache_surface

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "cache_surface": self.cache_surface,
            "cache_keyspace": self.cache_keyspace,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "owner": self.owner,
            "invalidation_trigger": self.invalidation_trigger,
            "ttl_staleness_policy": self.ttl_staleness_policy,
            "dependency_ordering": self.dependency_ordering,
            "backfill_warming": self.backfill_warming,
            "observability": self.observability,
            "rollback_fallback": self.rollback_fallback,
            "customer_impact": self.customer_impact,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCacheInvalidationReadinessMatrix:
    """Plan-level cache invalidation readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanCacheInvalidationReadinessRow, ...] = field(default_factory=tuple)
    cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanCacheInvalidationReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "cache_task_ids": list(self.cache_task_ids),
            "no_cache_task_ids": list(self.no_cache_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the cache invalidation readiness matrix as deterministic Markdown."""
        title = "# Plan Cache Invalidation Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('cache_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require cache invalidation readiness "
                f"(blocked: {readiness_counts.get('blocked', 0)}, "
                f"partial: {readiness_counts.get('partial', 0)}, "
                f"ready: {readiness_counts.get('ready', 0)}; "
                f"high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No cache invalidation readiness rows were inferred."])
            if self.no_cache_task_ids:
                lines.extend(["", f"No cache invalidation signals: {_markdown_cell(', '.join(self.no_cache_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Cache Surface | Tasks | Titles | Owner | Trigger | TTL/Staleness | Dependencies | "
                    "Backfill/Warming | Observability | Rollback/Fallback | Customer Impact | "
                    "Readiness | Severity | Gaps | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.cache_surface)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{_markdown_cell('; '.join(row.titles))} | "
                f"{row.owner} | {row.invalidation_trigger} | {row.ttl_staleness_policy} | "
                f"{row.dependency_ordering} | {row.backfill_warming} | {row.observability} | "
                f"{row.rollback_fallback} | {row.customer_impact} | {row.readiness} | {row.severity} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_cache_task_ids:
            lines.extend(["", f"No cache invalidation signals: {_markdown_cell(', '.join(self.no_cache_task_ids))}"])
        return "\n".join(lines)


def build_plan_cache_invalidation_readiness_matrix(source: Any) -> PlanCacheInvalidationReadinessMatrix:
    """Build grouped cache invalidation readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[str, list[_TaskCacheInvalidationSignals]] = {}
    no_cache_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_cache_invalidation:
            no_cache_task_ids.append(signals.task_id)
            continue
        grouped.setdefault(signals.cache_surface, []).append(signals)

    rows = tuple(sorted((_row_from_group(surface, values) for surface, values in grouped.items()), key=_row_sort_key))
    cache_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return PlanCacheInvalidationReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        cache_task_ids=cache_task_ids,
        no_cache_task_ids=tuple(no_cache_task_ids),
        summary=_summary(len(tasks), rows, no_cache_task_ids),
    )


def generate_plan_cache_invalidation_readiness_matrix(source: Any) -> PlanCacheInvalidationReadinessMatrix:
    """Generate a cache invalidation readiness matrix from a plan-like source."""
    return build_plan_cache_invalidation_readiness_matrix(source)


def analyze_plan_cache_invalidation_readiness_matrix(source: Any) -> PlanCacheInvalidationReadinessMatrix:
    """Analyze an execution plan for cache invalidation readiness."""
    if isinstance(source, PlanCacheInvalidationReadinessMatrix):
        return source
    return build_plan_cache_invalidation_readiness_matrix(source)


def derive_plan_cache_invalidation_readiness_matrix(source: Any) -> PlanCacheInvalidationReadinessMatrix:
    """Derive a cache invalidation readiness matrix from a plan-like source."""
    return analyze_plan_cache_invalidation_readiness_matrix(source)


def extract_plan_cache_invalidation_readiness_matrix(source: Any) -> PlanCacheInvalidationReadinessMatrix:
    """Extract a cache invalidation readiness matrix from a plan-like source."""
    return derive_plan_cache_invalidation_readiness_matrix(source)


def summarize_plan_cache_invalidation_readiness_matrix(
    source: PlanCacheInvalidationReadinessMatrix | Iterable[PlanCacheInvalidationReadinessRow] | Any,
) -> dict[str, Any] | PlanCacheInvalidationReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanCacheInvalidationReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_cache_invalidation_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_cache_invalidation_readiness_matrix_to_dict(
    matrix: PlanCacheInvalidationReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a cache invalidation readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_cache_invalidation_readiness_matrix_to_dict.__test__ = False


def plan_cache_invalidation_readiness_matrix_to_dicts(
    matrix: PlanCacheInvalidationReadinessMatrix | Iterable[PlanCacheInvalidationReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize cache invalidation rows to plain dictionaries."""
    if isinstance(matrix, PlanCacheInvalidationReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_cache_invalidation_readiness_matrix_to_dicts.__test__ = False


def plan_cache_invalidation_readiness_matrix_to_markdown(
    matrix: PlanCacheInvalidationReadinessMatrix,
) -> str:
    """Render a cache invalidation readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_cache_invalidation_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskCacheInvalidationSignals:
    task_id: str
    title: str
    cache_surface: str
    statuses: dict[str, str]
    gaps: tuple[str, ...]
    evidence: tuple[str, ...]
    has_cache_invalidation: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskCacheInvalidationSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    has_cache_invalidation = bool(_CACHE_RE.search(context) and _INVALIDATION_RE.search(context))
    statuses = {
        "owner": _status(_OWNER_RE, texts, skip_fields=("id",)),
        "invalidation_trigger": _status(_TRIGGER_RE, texts, skip_fields=("id",)),
        "ttl_staleness_policy": _status(_TTL_RE, texts),
        "dependency_ordering": _status(_DEPENDENCY_RE, texts),
        "backfill_warming": _status(_WARMING_RE, texts),
        "observability": _status(_OBSERVABILITY_RE, texts, skip_fields=("id",)),
        "rollback_fallback": _status(_ROLLBACK_RE, texts),
        "customer_impact": _status(_CUSTOMER_IMPACT_RE, texts),
    }
    gaps = [
        f"Missing {label}."
        for field_name, label in (
            ("owner", "cache invalidation owner"),
            ("invalidation_trigger", "invalidation trigger"),
            ("ttl_staleness_policy", "TTL or staleness policy"),
            ("dependency_ordering", "dependency ordering"),
            ("backfill_warming", "backfill or warming plan"),
            ("observability", "observability or alerting"),
            ("rollback_fallback", "rollback or fallback plan"),
            ("customer_impact", "customer-visible impact assessment"),
        )
        if statuses[field_name] == "missing"
    ]
    gaps.extend(_evidence_snippet(field, text) for field, text in texts if field != "id" and _EXPLICIT_GAP_RE.search(text))
    return _TaskCacheInvalidationSignals(
        task_id=task_id,
        title=title,
        cache_surface=_cache_surface(task, texts) or "unspecified_cache_keyspace",
        statuses=statuses,
        gaps=tuple(_dedupe(gaps)),
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _cache_evidence_match(text))),
        has_cache_invalidation=has_cache_invalidation,
    )


def _row_from_group(surface: str, signals: list[_TaskCacheInvalidationSignals]) -> PlanCacheInvalidationReadinessRow:
    fields = (
        "owner",
        "invalidation_trigger",
        "ttl_staleness_policy",
        "dependency_ordering",
        "backfill_warming",
        "observability",
        "rollback_fallback",
        "customer_impact",
    )
    statuses = {
        field_name: "present" if any(signal.statuses[field_name] == "present" for signal in signals) else "missing"
        for field_name in fields
    }
    gaps = tuple(
        _dedupe(
            gap
            for signal in signals
            for gap in signal.gaps
            if not gap.startswith("Missing ") or statuses[_gap_field(gap)] == "missing"
        )
    )
    readiness = _readiness(statuses)
    return PlanCacheInvalidationReadinessRow(
        cache_surface=surface,
        task_ids=tuple(_dedupe(signal.task_id for signal in signals)),
        titles=tuple(_dedupe(signal.title for signal in signals)),
        gaps=gaps,
        readiness=readiness,
        severity=_severity(readiness),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
        **statuses,
    )


def _gap_field(gap: str) -> str:
    if "owner" in gap:
        return "owner"
    if "trigger" in gap:
        return "invalidation_trigger"
    if "ttl" in gap.lower() or "staleness" in gap:
        return "ttl_staleness_policy"
    if "dependency" in gap or "ordering" in gap:
        return "dependency_ordering"
    if "backfill" in gap or "warming" in gap:
        return "backfill_warming"
    if "observability" in gap or "alert" in gap:
        return "observability"
    if "rollback" in gap or "fallback" in gap:
        return "rollback_fallback"
    if "customer" in gap or "impact" in gap:
        return "customer_impact"
    return "owner"


def _readiness(statuses: Mapping[str, str]) -> CacheInvalidationReadiness:
    if any(statuses[field_name] == "missing" for field_name in ("owner", "invalidation_trigger", "rollback_fallback")):
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _severity(readiness: CacheInvalidationReadiness) -> CacheInvalidationSeverity:
    return {"blocked": "high", "partial": "medium", "ready": "low"}[readiness]


def _summary(
    task_count: int,
    rows: Iterable[PlanCacheInvalidationReadinessRow],
    no_cache_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    no_cache_ids = tuple(no_cache_task_ids)
    cache_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "cache_task_count": len(cache_task_ids),
        "no_cache_task_count": len(no_cache_ids),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "gap_counts": {
            gap: sum(1 for row in row_list if gap in row.gaps)
            for gap in sorted({gap for row in row_list for gap in row.gaps})
        },
        "surface_counts": {
            surface: sum(1 for row in row_list if row.cache_surface == surface)
            for surface in sorted({row.cache_surface for row in row_list})
        },
    }


def _row_sort_key(row: PlanCacheInvalidationReadinessRow) -> tuple[int, int, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.cache_surface,
        ",".join(row.task_ids),
    )


def _status(
    pattern: re.Pattern[str],
    texts: Iterable[tuple[str, str]],
    *,
    skip_fields: tuple[str, ...] = (),
) -> str:
    return "present" if any(field not in skip_fields and pattern.search(text) for field, text in texts) else "missing"


def _cache_surface(task: Mapping[str, Any], texts: Iterable[tuple[str, str]]) -> str | None:
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("cache_surface", "cache_keyspace", "keyspace", "cache_namespace", "namespace", "surrogate_key"):
            candidate = _optional_text(metadata.get(key))
            if candidate:
                return _normalise_surface(candidate)
    for field, text in texts:
        for pattern in _SURFACE_PATTERNS:
            match = pattern.search(text)
            if match:
                candidate = _normalise_surface(match.group(1))
                if candidate and candidate not in _SURFACE_STOPWORDS:
                    return candidate
        if field.startswith("files"):
            for part in re.split(r"[/\\]", text):
                if _CACHE_RE.search(part) or _INVALIDATION_RE.search(part):
                    candidate = _normalise_surface(part)
                    if candidate and candidate not in _SURFACE_STOPWORDS:
                        return candidate
    return None


def _normalise_surface(value: str) -> str:
    text = _text(value).strip("`'\".,;:()[]{}")
    text = re.sub(r"[^a-zA-Z0-9./:-]+", "_", text)
    return text.strip("_").lower()


def _cache_evidence_match(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (
            _CACHE_RE,
            _INVALIDATION_RE,
            _OWNER_RE,
            _TRIGGER_RE,
            _TTL_RE,
            _DEPENDENCY_RE,
            _WARMING_RE,
            _OBSERVABILITY_RE,
            _ROLLBACK_RE,
            _CUSTOMER_IMPACT_RE,
        )
    )


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
    return None, _task_payloads(iterator)


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    if value is None:
        return tasks
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            payload = item.model_dump(mode="python")
            if isinstance(payload, Mapping):
                tasks.append(dict(payload))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for key in ("id", "title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        value = _optional_text(task.get(key))
        if value:
            texts.append((key, value))
    for key in ("depends_on", "dependencies", "files_or_modules", "acceptance_criteria", "tags", "validation_commands"):
        for idx, value in enumerate(_strings(task.get(key))):
            texts.append((f"{key}[{idx}]", value))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in sorted(metadata.items()):
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _CACHE_RE.search(key_text) or _INVALIDATION_RE.search(key_text):
                texts.append((f"metadata.{key}", key_text))
            for idx, item in enumerate(_strings(value)):
                texts.append((f"metadata.{key}" if idx == 0 else f"metadata.{key}[{idx}]", item))
    return tuple(texts)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_text(value),) if _text(value) else ()
    if isinstance(value, Mapping):
        return tuple(_text(f"{key}: {item}") for key, item in value.items() if _text(item))
    if isinstance(value, Iterable):
        return tuple(_text(item) for item in value if _text(item))
    text = _text(value)
    return (text,) if text else ()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value).strip())


def _evidence_snippet(field: str, text: str) -> str:
    return f"{field}: {_text(text)[:220]}"


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def _object_payload(value: object) -> dict[str, Any]:
    return {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key))
    }


def _looks_like_plan(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: Any) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )
