"""Assess task readiness for API caching implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


CachingSignal = Literal[
    "cache_implementation",
    "cache_invalidation_logic",
    "cache_key_design",
    "cache_ttl_configuration",
    "cache_header_configuration",
    "cache_monitoring_metrics",
    "cache_warming_strategy",
    "cache_hit_ratio_tracking",
    "performance_baseline",
    "cache_middleware",
    "distributed_cache",
]
CachingSafeguard = Literal[
    "cache_invalidation_tests",
    "cache_key_collision_tests",
    "cache_consistency_tests",
    "performance_benchmarks",
    "monitoring_dashboards",
    "cache_documentation",
]
CachingReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CachingReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[CachingSignal, ...] = (
    "cache_implementation",
    "cache_invalidation_logic",
    "cache_key_design",
    "cache_ttl_configuration",
    "cache_header_configuration",
    "cache_monitoring_metrics",
    "cache_warming_strategy",
    "cache_hit_ratio_tracking",
    "performance_baseline",
    "cache_middleware",
    "distributed_cache",
)
_SAFEGUARD_ORDER: tuple[CachingSafeguard, ...] = (
    "cache_invalidation_tests",
    "cache_key_collision_tests",
    "cache_consistency_tests",
    "performance_benchmarks",
    "monitoring_dashboards",
    "cache_documentation",
)
_SIGNAL_PATTERNS: dict[CachingSignal, re.Pattern[str]] = {
    "cache_implementation": re.compile(
        r"\b(?:implement.{0,20}cach(?:e|ing)|add.{0,20}cach(?:e|ing)|cach(?:e|ing).{0,20}implementation|cache layer|caching layer)\b",
        re.I,
    ),
    "cache_invalidation_logic": re.compile(
        r"\b(?:cache.{0,40}invalidat(?:e|ion)|invalidat(?:e|ion).{0,40}cache|purge.{0,20}cache|cache.{0,20}purge|cache.{0,20}clear(?:ing)?|cache.{0,20}bust(?:ing)?)\b",
        re.I,
    ),
    "cache_key_design": re.compile(
        r"\b(?:cache.{0,20}key|cache.{0,20}hash|cache.{0,20}identifier|cache.{0,20}namespace|key.{0,20}design|key.{0,20}strategy)\b",
        re.I,
    ),
    "cache_ttl_configuration": re.compile(
        r"\b(?:cache.{0,20}ttl|ttl|time.to.live|cache.{0,20}expir(?:e|ation|y)|expir(?:e|ation|y).{0,20}time|cache.{0,20}duration)\b",
        re.I,
    ),
    "cache_header_configuration": re.compile(
        r"\b(?:cache.{0,20}header|cache-control|etag|last-modified|max-age|s-maxage)\b",
        re.I,
    ),
    "cache_monitoring_metrics": re.compile(
        r"\b(?:cache.{0,20}metric|cache.{0,20}monitor(?:ing)?|cache.{0,20}observability|cache.{0,20}telemetry|cache.{0,20}stat(?:s|istics)?)\b",
        re.I,
    ),
    "cache_warming_strategy": re.compile(
        r"\b(?:cache.{0,20}warm(?:ing)?|warm.{0,20}cache|pre-?populate.{0,20}cache|cache.{0,20}pre-?load)\b",
        re.I,
    ),
    "cache_hit_ratio_tracking": re.compile(
        r"\b(?:cache.{0,20}hit.{0,20}(?:rate|ratio)|hit.{0,20}rate|miss.{0,20}rate|cache.{0,20}effectiveness)\b",
        re.I,
    ),
    "performance_baseline": re.compile(
        r"\b(?:performance.{0,20}baseline|baseline.{0,20}performance|benchmark|performance.{0,20}metric|response.{0,20}time|latency.{0,20}baseline)\b",
        re.I,
    ),
    "cache_middleware": re.compile(
        r"\b(?:cache.{0,20}middleware|caching.{0,20}middleware|middleware.{0,20}cach(?:e|ing))\b",
        re.I,
    ),
    "distributed_cache": re.compile(
        r"\b(?:redis|memcached|distributed.{0,20}cache|cache.{0,20}cluster|remote.{0,20}cache)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CachingSignal, re.Pattern[str]] = {
    "cache_implementation": re.compile(r"cach(?:e|ing)", re.I),
    "cache_invalidation_logic": re.compile(r"invalidat|purge", re.I),
    "cache_key_design": re.compile(r"cache[_-]?key", re.I),
    "cache_ttl_configuration": re.compile(r"ttl|expir", re.I),
    "cache_header_configuration": re.compile(r"cache[_-]?header", re.I),
    "cache_monitoring_metrics": re.compile(r"cache[_-]?metric|cache[_-]?monitor", re.I),
    "cache_warming_strategy": re.compile(r"warm|pre[_-]?load", re.I),
    "cache_hit_ratio_tracking": re.compile(r"hit[_-]?rate|miss[_-]?rate", re.I),
    "performance_baseline": re.compile(r"baseline|benchmark|performance", re.I),
    "cache_middleware": re.compile(r"middleware", re.I),
    "distributed_cache": re.compile(r"redis|memcached", re.I),
}
_SAFEGUARD_PATTERNS: dict[CachingSafeguard, re.Pattern[str]] = {
    "cache_invalidation_tests": re.compile(
        r"\b(?:(?:cache.{0,80}(?:invalidat|purg|clear)).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache.{0,80}(?:invalidat|purg|clear))|"
        r"test.{0,40}invalidat|test.{0,40}purge)\b",
        re.I,
    ),
    "cache_key_collision_tests": re.compile(
        r"\b(?:(?:cache.{0,80}key).{0,80}(?:collision|conflict|uniqueness|tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache.{0,80}key)|"
        r"key.{0,40}collision|key.{0,40}uniqueness)\b",
        re.I,
    ),
    "cache_consistency_tests": re.compile(
        r"\b(?:(?:cache.{0,80}consistency).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache.{0,80}consistency)|"
        r"stale.{0,40}cache.{0,40}test|cache.{0,40}staleness)\b",
        re.I,
    ),
    "performance_benchmarks": re.compile(
        r"\b(?:performance.{0,80}(?:benchmark|test|baseline|metric)|"
        r"benchmark.{0,80}(?:performance|cache|caching)|"
        r"latency.{0,40}test|response.{0,40}time.{0,40}test)\b",
        re.I,
    ),
    "monitoring_dashboards": re.compile(
        r"\b(?:monitor.{0,80}(?:dashboard|metric|alert|observability)|"
        r"dashboard.{0,80}(?:cache|monitor|metric)|"
        r"cache.{0,80}(?:dashboard|metric|monitor|alert|observability)|"
        r"prometheus|grafana|datadog)\b",
        re.I,
    ),
    "cache_documentation": re.compile(
        r"\b(?:cache.{0,80}(?:docs|documentation|guide|readme|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|document|usage|examples?).{0,80}cache|"
        r"caching.{0,80}strategy|cache.{0,80}policy)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:cache|caching)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[CachingSafeguard, str] = {
    "cache_invalidation_tests": "Add tests verifying cache invalidation logic, purge mechanisms, and cache clearing on data updates to prevent stale data.",
    "cache_key_collision_tests": "Add tests for cache key uniqueness, collision scenarios, and proper namespace isolation to prevent key conflicts.",
    "cache_consistency_tests": "Add tests for cache consistency, stale data detection, and race condition handling during concurrent updates.",
    "performance_benchmarks": "Establish performance baselines and benchmarks to measure cache effectiveness, response time improvements, and hit ratios.",
    "monitoring_dashboards": "Set up monitoring dashboards with cache hit/miss rates, eviction metrics, memory usage, and performance alerts.",
    "cache_documentation": "Document caching strategy, TTL policies, invalidation hooks, key design, and operational runbooks for cache troubleshooting.",
}


@dataclass(frozen=True, slots=True)
class TaskApiCachingReadiness:
    """API caching readiness assessment for one execution task."""

    task_id: str
    cache_implementation_status: str = "not_started"
    invalidation_strategy: str = "not_defined"
    monitoring_plan: str = "not_defined"
    performance_baseline: str = "not_established"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "cache_implementation_status": self.cache_implementation_status,
            "invalidation_strategy": self.invalidation_strategy,
            "monitoring_plan": self.monitoring_plan,
            "performance_baseline": self.performance_baseline,
        }


@dataclass(frozen=True, slots=True)
class TaskApiCachingReadinessFinding:
    """API caching readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[CachingSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CachingSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CachingSafeguard, ...] = field(default_factory=tuple)
    readiness: CachingReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskApiCachingReadinessPlan:
    """Plan-level API caching readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiCachingReadinessFinding, ...] = field(default_factory=tuple)
    caching_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiCachingReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "caching_task_ids": list(self.caching_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return caching readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render caching readiness guidance as deterministic Markdown."""
        title = "# Task API Caching Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Caching task count: {self.summary.get('caching_task_count', 0)}",
            f"- Not applicable task count: {self.summary.get('not_applicable_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER[:5]),
        ]
        if not self.findings:
            lines.extend(["", "No API caching readiness findings were identified."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Detected Signals | Present Safeguards | Missing Safeguards |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} |"
            )
        return "\n".join(lines)


def analyze_task_api_caching_readiness(
    task: Mapping[str, Any],
) -> TaskApiCachingReadiness:
    """Analyze a task for API caching implementation readiness."""
    task_id = task.get("id", "")

    # Determine cache implementation status
    cache_impl_status = "not_started"
    task_text = _searchable_task_text(task)

    if _SIGNAL_PATTERNS["cache_implementation"].search(task_text):
        cache_impl_status = "planned"
        if any(pattern.search(task_text) for pattern in [
            _SIGNAL_PATTERNS["cache_middleware"],
            _SIGNAL_PATTERNS["distributed_cache"],
        ]):
            cache_impl_status = "in_progress"

    # Determine invalidation strategy
    invalidation_strat = "not_defined"
    if _SIGNAL_PATTERNS["cache_invalidation_logic"].search(task_text):
        invalidation_strat = "defined"
        if _SAFEGUARD_PATTERNS["cache_invalidation_tests"].search(task_text):
            invalidation_strat = "tested"

    # Determine monitoring plan
    monitor_plan = "not_defined"
    if _SIGNAL_PATTERNS["cache_monitoring_metrics"].search(task_text):
        monitor_plan = "planned"
        if _SAFEGUARD_PATTERNS["monitoring_dashboards"].search(task_text):
            monitor_plan = "implemented"

    # Determine performance baseline
    perf_baseline = "not_established"
    if _SIGNAL_PATTERNS["performance_baseline"].search(task_text):
        perf_baseline = "planned"
        if _SAFEGUARD_PATTERNS["performance_benchmarks"].search(task_text):
            perf_baseline = "established"

    return TaskApiCachingReadiness(
        task_id=task_id,
        cache_implementation_status=cache_impl_status,
        invalidation_strategy=invalidation_strat,
        monitoring_plan=monitor_plan,
        performance_baseline=perf_baseline,
    )


def build_task_api_caching_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCachingReadinessPlan:
    """Build a caching readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    caching_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in caching_ids
    )
    return TaskApiCachingReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        caching_task_ids=caching_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, caching_ids, not_applicable_ids),
    )


def derive_task_api_caching_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCachingReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_caching_readiness_plan(plan)


def generate_task_api_caching_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCachingReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_caching_readiness_plan(plan)


def extract_task_api_caching_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiCachingReadinessFinding, ...]:
    """Return caching readiness findings for applicable tasks."""
    return build_task_api_caching_readiness_plan(plan).findings


def summarize_task_api_caching_readiness(
    result: TaskApiCachingReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for caching readiness review."""
    if isinstance(result, TaskApiCachingReadinessPlan):
        return dict(result.summary)
    return build_task_api_caching_readiness_plan(result).summary


def _load_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(plan, str):
        return None, []
    if isinstance(plan, ExecutionPlan):
        payload = dict(plan.model_dump(mode="python"))
        return payload.get("id"), payload.get("tasks", [])
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return payload.get("id"), payload.get("tasks", [])
    if isinstance(plan, Mapping):
        try:
            payload = dict(ExecutionPlan.model_validate(plan).model_dump(mode="python"))
            return payload.get("id"), payload.get("tasks", [])
        except (TypeError, ValueError, ValidationError):
            return plan.get("id"), plan.get("tasks", [])
    # Handle SimpleNamespace or other objects with tasks attribute
    if hasattr(plan, "tasks"):
        plan_id = getattr(plan, "id", None)
        tasks = getattr(plan, "tasks", [])
        # Convert tasks to dicts
        task_dicts = []
        for task in tasks:
            if isinstance(task, Mapping):
                task_dicts.append(dict(task))
            elif hasattr(task, "__dict__"):
                task_dicts.append(vars(task))
            else:
                task_dicts.append({})
        return plan_id, task_dicts
    return None, []


def _analyze_task(task: Mapping[str, Any]) -> TaskApiCachingReadinessFinding | None:
    task_id = task.get("id", "")
    title = task.get("title", "")

    if _NO_IMPACT_RE.search(_searchable_task_text(task)):
        return None

    detected_signals = _detect_signals(task)
    if not detected_signals:
        return None

    present_safeguards = _detect_safeguards(task)
    missing_safeguards: tuple[CachingSafeguard, ...] = tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if safeguard not in present_safeguards
    )

    readiness = _compute_readiness(detected_signals, present_safeguards, missing_safeguards)
    evidence = _collect_evidence(task, detected_signals)
    actionable_remediations = tuple(
        _REMEDIATIONS[safeguard] for safeguard in missing_safeguards
    )

    return TaskApiCachingReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[CachingSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    validation_commands = _validation_commands(task)
    detected: list[CachingSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)
        elif any(_SIGNAL_PATTERNS[signal].search(cmd) for cmd in validation_commands):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[CachingSafeguard, ...]:
    searchable = _searchable_task_text(task)
    validation_commands = _validation_commands(task)
    detected: list[CachingSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)
        elif any(_SAFEGUARD_PATTERNS[safeguard].search(cmd) for cmd in validation_commands):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[CachingSignal, ...],
    present: tuple[CachingSafeguard, ...],
    missing: tuple[CachingSafeguard, ...],
) -> CachingReadiness:
    if not signals:
        return "weak"

    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[CachingSignal, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    searchable = _searchable_task_text(task)

    for signal in signals[:3]:
        pattern = _SIGNAL_PATTERNS[signal]
        match = pattern.search(searchable)
        if match:
            start = max(0, match.start() - 40)
            end = min(len(searchable), match.end() + 40)
            snippet = _clean_text(searchable[start:end])
            if len(snippet) > 100:
                snippet = f"{snippet[:97]}..."
            evidence.append(f"{signal}: ...{snippet}...")

    return tuple(evidence)


def _summary(
    findings: tuple[TaskApiCachingReadinessFinding, ...],
    caching_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "caching_task_count": len(caching_ids),
        "not_applicable_task_count": len(not_applicable_ids),
        "readiness_counts": {
            readiness: sum(1 for finding in findings if finding.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "overall_readiness": _overall_readiness(findings),
    }


def _overall_readiness(findings: tuple[TaskApiCachingReadinessFinding, ...]) -> CachingReadiness:
    if not findings:
        return "weak"
    readiness_values = [_READINESS_ORDER[finding.readiness] for finding in findings]
    avg = sum(readiness_values) / len(readiness_values)
    if avg >= 1.5:
        return "strong"
    if avg >= 0.5:
        return "partial"
    return "weak"


def _searchable_task_text(task: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field in ("title", "body", "description", "prompt", "acceptance_criteria", "definition_of_done"):
        value = task.get(field)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
    return " ".join(parts)


def _task_paths(task: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for field in ("expected_files", "output_files", "related_files", "files_or_modules", "files"):
        value = task.get(field)
        if isinstance(value, str):
            paths.append(value)
        elif isinstance(value, (list, tuple)):
            paths.extend(str(item) for item in value if item)
    return paths


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for field in ("validation_command", "validation_commands", "test_command", "test_commands"):
        value = task.get(field)
        if isinstance(value, str):
            commands.append(value)
        elif isinstance(value, (list, tuple)):
            commands.extend(str(item) for item in value if item)
        elif isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
    return commands


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


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
    "CachingReadiness",
    "CachingSafeguard",
    "CachingSignal",
    "TaskApiCachingReadiness",
    "TaskApiCachingReadinessFinding",
    "TaskApiCachingReadinessPlan",
    "analyze_task_api_caching_readiness",
    "build_task_api_caching_readiness_plan",
    "derive_task_api_caching_readiness_plan",
    "extract_task_api_caching_readiness_findings",
    "generate_task_api_caching_readiness_plan",
    "summarize_task_api_caching_readiness",
]
