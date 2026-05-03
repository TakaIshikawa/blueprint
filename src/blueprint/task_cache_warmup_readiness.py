"""Assess readiness safeguards for cache warmup and precomputation tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CacheWarmupSignal = Literal[
    "warming_job",
    "precomputed_projection",
    "materialized_view",
    "primed_keys",
    "startup_cache_hydration",
]
CacheWarmupSafeguard = Literal[
    "warmup_backfill_plan",
    "stale_data_guard",
    "load_shedding",
    "cache_miss_fallback",
    "observability",
    "rollback_or_disable_switch",
    "owner_evidence",
]
CacheWarmupRiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[CacheWarmupRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[CacheWarmupSignal, ...] = (
    "warming_job",
    "precomputed_projection",
    "materialized_view",
    "primed_keys",
    "startup_cache_hydration",
)
_SAFEGUARD_ORDER: tuple[CacheWarmupSafeguard, ...] = (
    "warmup_backfill_plan",
    "stale_data_guard",
    "load_shedding",
    "cache_miss_fallback",
    "observability",
    "rollback_or_disable_switch",
    "owner_evidence",
)
_SIGNAL_PATTERNS: dict[CacheWarmupSignal, re.Pattern[str]] = {
    "warming_job": re.compile(
        r"\b(?:cache warmup|cache warm up|cache warming|prewarm|pre-warm|warmer job|warming job|"
        r"warmup job|warm-up job|scheduled warm|post[- ]?deploy warm|cache hydration job)\b",
        re.I,
    ),
    "precomputed_projection": re.compile(
        r"\b(?:precomputed projection|precomputed projections|precompute(?:d)? read model|"
        r"precomputed aggregate|precomputed rollup|projection table|summary table|derived projection|"
        r"precalculated|pre-calculated|precompute|precomputes|precomputing)\b",
        re.I,
    ),
    "materialized_view": re.compile(
        r"\b(?:materialized view|materialized views|materialised view|materialised views|"
        r"mv refresh|refresh materialized|refresh materialised)\b",
        re.I,
    ),
    "primed_keys": re.compile(
        r"\b(?:prime cache|primed cache|prime keys|primed keys|seed cache|seeded cache|"
        r"populate cache keys|cache priming|redis key priming|hot keys)\b",
        re.I,
    ),
    "startup_cache_hydration": re.compile(
        r"\b(?:startup cache hydration|startup hydration|boot(?:strap)? cache|hydrate cache on startup|"
        r"startup warmup|startup warm up|cold[- ]?start hydration|cold cache hydration|"
        r"load cache during boot)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[CacheWarmupSignal, re.Pattern[str]] = {
    "warming_job": re.compile(r"warmups?|warm[-_]?up|warming|prewarm|warmer", re.I),
    "precomputed_projection": re.compile(r"precomput|projection|rollups?|summary[_-]?table|read[_-]?model", re.I),
    "materialized_view": re.compile(r"materiali[sz]ed[_-]?views?|mv[_-]?refresh|mviews?", re.I),
    "primed_keys": re.compile(r"prim(?:e|ing)|seed[_-]?cache|hot[_-]?keys", re.I),
    "startup_cache_hydration": re.compile(r"startup|bootstrap|cold[_-]?start|hydration|hydrate", re.I),
}
_SAFEGUARD_PATTERNS: dict[CacheWarmupSafeguard, re.Pattern[str]] = {
    "warmup_backfill_plan": re.compile(
        r"\b(?:warmup backfill plan|backfill plan|backfill window|bulk backfill|migration plan|"
        r"rebuild plan|rehydration plan|resume backfill|checkpoint(?:ed)? backfill)\b",
        re.I,
    ),
    "stale_data_guard": re.compile(
        r"\b(?:stale data guard|staleness guard|freshness check|freshness guard|max age|version check|"
        r"etag|watermark|source version|serve stale|stale[- ]while[- ]revalidate)\b",
        re.I,
    ),
    "load_shedding": re.compile(
        r"\b(?:load shedding|shed load|rate limit|throttle|throttling|concurrency limit|batch size|"
        r"queue limit|warmup budget|load cap|capacity limit|circuit breaker|pause warmup)\b",
        re.I,
    ),
    "cache_miss_fallback": re.compile(
        r"\b(?:cache miss fallback|miss fallback|fallback path|origin fallback|database fallback|"
        r"degraded mode|lazy recompute|on[- ]demand compute|uncached path|bypass cache)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|monitoring|alerts?|dashboard|logs?|tracing|cache hit rate|"
        r"miss rate|warmup success|warmup failure|stale rate|queue depth|latency)\b",
        re.I,
    ),
    "rollback_or_disable_switch": re.compile(
        r"\b(?:rollback|disable switch|disable path|kill switch|feature flag|turn off|disable warm|"
        r"disable warmup|pause warmup|revert path|roll back)\b",
        re.I,
    ),
    "owner_evidence": re.compile(
        r"\b(?:owner|owned by|ownership|on[- ]call|dr[iy]|responsible team|runbook owner|"
        r"service owner|team owns)\b",
        re.I,
    ),
}
_SAFEGUARD_CHECKS: dict[CacheWarmupSafeguard, str] = {
    "warmup_backfill_plan": "Document how warmup or precomputation backfills are chunked, resumed, and completed.",
    "stale_data_guard": "Add freshness, version, watermark, or max-age checks before serving warmed data.",
    "load_shedding": "Bound warmup load with rate limits, concurrency caps, batch sizes, budgets, or circuit breakers.",
    "cache_miss_fallback": "Exercise fallback behavior when warmed entries are absent or precomputation fails.",
    "observability": "Track warmup success, failures, stale rates, queue depth, latency, and cache hit or miss rates.",
    "rollback_or_disable_switch": "Provide a rollback, feature flag, kill switch, or pause path for the warmup mechanism.",
    "owner_evidence": "Identify the owning team or on-call path for operating the warmup task.",
}


@dataclass(frozen=True, slots=True)
class TaskCacheWarmupReadinessRecord:
    """Readiness guidance for one cache warmup or precomputation task."""

    task_id: str
    title: str
    signals: tuple[CacheWarmupSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[CacheWarmupSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CacheWarmupSafeguard, ...] = field(default_factory=tuple)
    risk_level: CacheWarmupRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    @property
    def present_safeguards(self) -> tuple[CacheWarmupSafeguard, ...]:
        """Compatibility view matching analyzers that name covered controls present_safeguards."""
        return self.safeguards

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "signals": list(self.signals),
            "safeguards": list(self.safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskCacheWarmupReadinessPlan:
    """Plan-level cache warmup readiness summary."""

    plan_id: str | None = None
    records: tuple[TaskCacheWarmupReadinessRecord, ...] = field(default_factory=tuple)
    cache_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCacheWarmupReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskCacheWarmupReadinessRecord, ...]:
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
        """Return cache warmup records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render cache warmup readiness guidance as deterministic Markdown."""
        title = "# Task Cache Warmup Readiness"
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
            f"- Cache warmup task count: {self.summary.get('cache_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task cache warmup readiness records were inferred."])
            if self.suppressed_task_ids:
                lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Safeguards | Missing Safeguards | Evidence | Recommended Checks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} |"
            )
        if self.suppressed_task_ids:
            lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
        return "\n".join(lines)


def build_task_cache_warmup_readiness_plan(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Build readiness records for cache warmup and precomputation tasks."""
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
                len(record.missing_safeguards),
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
    return TaskCacheWarmupReadinessPlan(
        plan_id=plan_id,
        records=records,
        cache_task_ids=cache_task_ids,
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, task_count=len(tasks), suppressed_task_ids=suppressed_task_ids),
    )


def analyze_task_cache_warmup_readiness(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Compatibility alias for building cache warmup readiness plans."""
    return build_task_cache_warmup_readiness_plan(source)


def derive_task_cache_warmup_readiness(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Compatibility alias for deriving cache warmup readiness plans."""
    return build_task_cache_warmup_readiness_plan(source)


def generate_task_cache_warmup_readiness(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Compatibility alias for generating cache warmup readiness plans."""
    return build_task_cache_warmup_readiness_plan(source)


def recommend_task_cache_warmup_readiness(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Compatibility alias for recommending cache warmup safeguards."""
    return build_task_cache_warmup_readiness_plan(source)


def summarize_task_cache_warmup_readiness(source: Any) -> TaskCacheWarmupReadinessPlan:
    """Compatibility alias for summarizing cache warmup readiness."""
    return build_task_cache_warmup_readiness_plan(source)


def task_cache_warmup_readiness_plan_to_dict(result: TaskCacheWarmupReadinessPlan) -> dict[str, Any]:
    """Serialize a cache warmup readiness plan to a plain dictionary."""
    return result.to_dict()


task_cache_warmup_readiness_plan_to_dict.__test__ = False


def task_cache_warmup_readiness_plan_to_dicts(
    result: TaskCacheWarmupReadinessPlan | Iterable[TaskCacheWarmupReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize cache warmup readiness records to plain dictionaries."""
    if isinstance(result, TaskCacheWarmupReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_cache_warmup_readiness_plan_to_dicts.__test__ = False


def task_cache_warmup_readiness_to_dicts(
    result: TaskCacheWarmupReadinessPlan | Iterable[TaskCacheWarmupReadinessRecord],
) -> list[dict[str, Any]]:
    """Compatibility alias for serializing cache warmup readiness records."""
    return task_cache_warmup_readiness_plan_to_dicts(result)


task_cache_warmup_readiness_to_dicts.__test__ = False


def task_cache_warmup_readiness_plan_to_markdown(result: TaskCacheWarmupReadinessPlan) -> str:
    """Render a cache warmup readiness plan as Markdown."""
    return result.to_markdown()


task_cache_warmup_readiness_plan_to_markdown.__test__ = False


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskCacheWarmupReadinessRecord | None:
    signal_hits: set[CacheWarmupSignal] = set()
    safeguard_hits: set[CacheWarmupSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _PATH_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(f"files_or_modules: {path}")
        if matched_safeguard:
            safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
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
        snippet = _evidence_snippet(source_field, text)
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    if not signal_hits:
        return None

    signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits)
    safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits)
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguard_hits)
    task_id = _task_id(task, index)
    return TaskCacheWarmupReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        signals=signals,
        safeguards=safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals, missing),
        evidence=tuple(_dedupe([*signal_evidence, *safeguard_evidence])),
        recommended_checks=tuple(_SAFEGUARD_CHECKS[safeguard] for safeguard in missing),
    )


def _risk_level(
    signals: tuple[CacheWarmupSignal, ...],
    missing: tuple[CacheWarmupSafeguard, ...],
) -> CacheWarmupRiskLevel:
    if not missing:
        return "low"
    if "load_shedding" in missing or "cache_miss_fallback" in missing:
        return "high"
    if any(signal in signals for signal in ("materialized_view", "startup_cache_hydration", "primed_keys")):
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskCacheWarmupReadinessRecord, ...],
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
        "risk_counts": {
            risk_level: sum(1 for record in records if record.risk_level == risk_level)
            for risk_level in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.signals)
            for signal in _SIGNAL_ORDER
        },
        "safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.safeguards)
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


def _validation_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for index, command in enumerate(flatten_validation_commands(task)):
        if text := _optional_text(command):
            texts.append((f"validation_commands[{index}]", text))
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


def _metadata_key_is_signal(key_text: str) -> bool:
    return any(
        pattern.search(key_text)
        for pattern in (*_SIGNAL_PATTERNS.values(), *_PATH_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
    )


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
    "CacheWarmupRiskLevel",
    "CacheWarmupSafeguard",
    "CacheWarmupSignal",
    "TaskCacheWarmupReadinessPlan",
    "TaskCacheWarmupReadinessRecord",
    "analyze_task_cache_warmup_readiness",
    "build_task_cache_warmup_readiness_plan",
    "derive_task_cache_warmup_readiness",
    "generate_task_cache_warmup_readiness",
    "recommend_task_cache_warmup_readiness",
    "summarize_task_cache_warmup_readiness",
    "task_cache_warmup_readiness_plan_to_dict",
    "task_cache_warmup_readiness_plan_to_dicts",
    "task_cache_warmup_readiness_plan_to_markdown",
    "task_cache_warmup_readiness_to_dicts",
]
