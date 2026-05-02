"""Plan cache stampede readiness for cache-heavy implementation tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CacheStampedeSignal = Literal[
    "cache_miss_storm",
    "hot_key",
    "ttl_refresh",
    "memoization",
    "expensive_recomputation",
    "database_fanout",
    "traffic_spike",
]
CacheStampedeSafeguard = Literal[
    "request_coalescing",
    "jittered_ttls",
    "stale_while_revalidate",
    "prewarming",
    "rate_limiting",
    "observability",
]
CacheStampedeRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[CacheStampedeSignal, ...] = (
    "cache_miss_storm",
    "hot_key",
    "ttl_refresh",
    "memoization",
    "expensive_recomputation",
    "database_fanout",
    "traffic_spike",
)
_SAFEGUARD_ORDER: tuple[CacheStampedeSafeguard, ...] = (
    "request_coalescing",
    "jittered_ttls",
    "stale_while_revalidate",
    "prewarming",
    "rate_limiting",
    "observability",
)
_RISK_ORDER: dict[CacheStampedeRisk, int] = {"high": 0, "medium": 1, "low": 2}

_SIGNAL_PATTERNS: dict[CacheStampedeSignal, re.Pattern[str]] = {
    "cache_miss_storm": re.compile(
        r"\b(?:cache miss storm|cache stampede|thundering herd|dogpile|miss storm|"
        r"cold cache|cache avalanche|simultaneous cache miss(?:es)?)\b",
        re.I,
    ),
    "hot_key": re.compile(
        r"\b(?:hot key|hot cache key|popular key|celebrity key|skewed key|high[- ]traffic key|"
        r"single cache key|shared cache key)\b",
        re.I,
    ),
    "ttl_refresh": re.compile(
        r"\b(?:ttl refresh|ttl expiry|ttl expiration|cache expiry|cache expiration|expire all|"
        r"refresh cache|refresh window|background refresh|revalidate)\b",
        re.I,
    ),
    "memoization": re.compile(
        r"\b(?:memoiz(?:e|ed|ation)|computed cache|result cache|query cache|cached aggregate|"
        r"derived cache|in[- ]memory cache|function cache)\b",
        re.I,
    ),
    "expensive_recomputation": re.compile(
        r"\b(?:expensive recomputation|expensive recalculation|costly computation|heavy computation|"
        r"recompute|recalculation|regenerate report|render expensive|slow query)\b",
        re.I,
    ),
    "database_fanout": re.compile(
        r"\b(?:database fanout|db fanout|fan[- ]out quer(?:y|ies)|n\+1|many queries|"
        r"origin fanout|database load|read replica pressure|downstream fanout)\b",
        re.I,
    ),
    "traffic_spike": re.compile(
        r"\b(?:traffic spike|traffic surge|burst traffic|high traffic|peak traffic|flash sale|"
        r"launch traffic|viral traffic|load spike|qps spike|request spike)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[CacheStampedeSafeguard, re.Pattern[str]] = {
    "request_coalescing": re.compile(
        r"\b(?:request coalescing|single[- ]?flight|singleflight|request collapsing|"
        r"promise cache|in[- ]flight dedupe|lock per key|mutex per key|dogpile lock)\b",
        re.I,
    ),
    "jittered_ttls": re.compile(
        r"\b(?:jittered ttl|ttl jitter|randomized ttl|random ttl|spread expir(?:y|ation)|"
        r"staggered expir(?:y|ation)|probabilistic expir(?:y|ation))\b",
        re.I,
    ),
    "stale_while_revalidate": re.compile(
        r"\b(?:stale[- ]while[- ]revalidate|swr|serve stale|stale fallback|stale cache|"
        r"background refresh|refresh in background)\b",
        re.I,
    ),
    "prewarming": re.compile(
        r"\b(?:prewarm(?:ing)?|pre-warm(?:ing)?|cache warm(?:ing)?|warm cache|warmup|"
        r"precompute|preload cache|prime cache)\b",
        re.I,
    ),
    "rate_limiting": re.compile(
        r"\b(?:rate limit(?:ing)?|throttl(?:e|ing)|backpressure|shed load|load shedding|"
        r"concurrency limit|queue requests|circuit breaker)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|monitoring|alerts?|dashboard|cache hit rate|miss rate|"
        r"origin qps|cache wait time|coalescing rate|hot key metric|stampede alert)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[CacheStampedeSafeguard, str] = {
    "request_coalescing": "Add request coalescing or a per-key single-flight lock so one miss refreshes each hot value.",
    "jittered_ttls": "Use jittered or staggered TTLs so many keys do not expire at the same instant.",
    "stale_while_revalidate": "Serve bounded stale data while one background refresh repopulates the cache.",
    "prewarming": "Prewarm or prime hot cache entries before launches, deploys, migrations, or scheduled expirations.",
    "rate_limiting": "Protect the origin with rate limits, concurrency caps, backpressure, or load shedding.",
    "observability": "Track hit rate, miss spikes, hot keys, refresh latency, coalescing, origin load, and alerts.",
}


@dataclass(frozen=True, slots=True)
class TaskCacheStampedeReadinessRecord:
    """Stampede-readiness guidance for one cache-heavy task."""

    task_id: str
    title: str
    detected_signals: tuple[CacheStampedeSignal, ...]
    present_safeguards: tuple[CacheStampedeSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CacheStampedeSafeguard, ...] = field(default_factory=tuple)
    risk_level: CacheStampedeRisk = "medium"
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[CacheStampedeSignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommendations(self) -> tuple[str, ...]:
        """Compatibility view for planners that name recommended actions recommendations."""
        return self.recommended_actions

    @property
    def recommended_checks(self) -> tuple[str, ...]:
        """Compatibility view for planners that name recommended actions checks."""
        return self.recommended_actions

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_actions": list(self.recommended_actions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCacheStampedeReadinessPlan:
    """Plan-level stampede-readiness review for cache-heavy tasks."""

    plan_id: str | None = None
    records: tuple[TaskCacheStampedeReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCacheStampedeReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskCacheStampedeReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return stampede-readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render stampede-readiness guidance as deterministic Markdown."""
        title = "# Task Cache Stampede Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task cache stampede-readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Actions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_actions) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_cache_stampede_readiness_plan(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Build cache stampede-readiness records for cache-heavy implementation tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskCacheStampedeReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for building cache stampede-readiness plans."""
    return build_task_cache_stampede_readiness_plan(source)


def summarize_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for building cache stampede-readiness plans."""
    return build_task_cache_stampede_readiness_plan(source)


def extract_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for extracting cache stampede-readiness plans."""
    return build_task_cache_stampede_readiness_plan(source)


def generate_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for generating cache stampede-readiness plans."""
    return build_task_cache_stampede_readiness_plan(source)


def derive_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for deriving cache stampede-readiness plans."""
    return build_task_cache_stampede_readiness_plan(source)


def recommend_task_cache_stampede_readiness(source: Any) -> TaskCacheStampedeReadinessPlan:
    """Compatibility alias for recommending cache stampede safeguards."""
    return build_task_cache_stampede_readiness_plan(source)


def task_cache_stampede_readiness_plan_to_dict(result: TaskCacheStampedeReadinessPlan) -> dict[str, Any]:
    """Serialize a cache stampede-readiness plan to a plain dictionary."""
    return result.to_dict()


task_cache_stampede_readiness_plan_to_dict.__test__ = False


def task_cache_stampede_readiness_plan_to_dicts(
    result: TaskCacheStampedeReadinessPlan | Iterable[TaskCacheStampedeReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize cache stampede-readiness records to plain dictionaries."""
    if isinstance(result, TaskCacheStampedeReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_cache_stampede_readiness_plan_to_dicts.__test__ = False


def task_cache_stampede_readiness_plan_to_markdown(result: TaskCacheStampedeReadinessPlan) -> str:
    """Render a cache stampede-readiness plan as Markdown."""
    return result.to_markdown()


task_cache_stampede_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[CacheStampedeSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CacheStampedeSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskCacheStampedeReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskCacheStampedeReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, signals.present_safeguards, missing),
        recommended_actions=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[CacheStampedeSignal] = set()
    safeguard_hits: set[CacheStampedeSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[CacheStampedeSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[CacheStampedeSignal] = set()
    if "stampede" in text or "dogpile" in text or "thundering herd" in text or "miss storm" in text:
        signals.add("cache_miss_storm")
    if "hot key" in text or "hotkey" in text or "popular key" in text:
        signals.add("hot_key")
    if {"ttl", "expiry", "expiration", "refresh", "revalidate"} & parts or any(
        token in name for token in ("ttl", "expiry", "expiration", "refresh", "revalidate")
    ):
        signals.add("ttl_refresh")
    if any(token in text for token in ("memoiz", "result cache", "query cache", "computed cache", "aggregate")):
        signals.add("memoization")
    if any(token in text for token in ("recompute", "recalculation", "expensive", "slow query")):
        signals.add("expensive_recomputation")
    if any(token in text for token in ("fanout", "fan out", "n+1", "database load", "origin fanout")):
        signals.add("database_fanout")
    if any(token in text for token in ("traffic spike", "traffic surge", "high traffic", "qps spike", "flash sale")):
        signals.add("traffic_spike")
    return signals


def _risk_level(
    signals: tuple[CacheStampedeSignal, ...],
    present: tuple[CacheStampedeSafeguard, ...],
    missing: tuple[CacheStampedeSafeguard, ...],
) -> CacheStampedeRisk:
    if not missing:
        return "low"
    signal_set = set(signals)
    present_set = set(present)
    high_pressure = bool({"cache_miss_storm", "hot_key", "database_fanout", "traffic_spike"} & signal_set)
    lacks_collapse = "request_coalescing" not in present_set
    lacks_origin_protection = "rate_limiting" not in present_set and "stale_while_revalidate" not in present_set
    if high_pressure and (lacks_collapse or lacks_origin_protection):
        return "high"
    if len(missing) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskCacheStampedeReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "CacheStampedeRisk",
    "CacheStampedeSafeguard",
    "CacheStampedeSignal",
    "TaskCacheStampedeReadinessPlan",
    "TaskCacheStampedeReadinessRecord",
    "analyze_task_cache_stampede_readiness",
    "build_task_cache_stampede_readiness_plan",
    "derive_task_cache_stampede_readiness",
    "extract_task_cache_stampede_readiness",
    "generate_task_cache_stampede_readiness",
    "recommend_task_cache_stampede_readiness",
    "summarize_task_cache_stampede_readiness",
    "task_cache_stampede_readiness_plan_to_dict",
    "task_cache_stampede_readiness_plan_to_dicts",
    "task_cache_stampede_readiness_plan_to_markdown",
]
