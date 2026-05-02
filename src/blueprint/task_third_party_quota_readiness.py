"""Plan third-party provider quota readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ThirdPartyQuotaSignal = Literal[
    "provider_quota",
    "burst_limit",
    "monthly_cap",
    "paid_tier",
    "provider_throttling",
    "fallback_behavior",
    "quota_alert",
]
ThirdPartyQuotaSafeguard = Literal[
    "budget_alarms",
    "retry_backoff",
    "degradation_behavior",
    "cache_strategy",
    "provider_limit_tests",
]
ThirdPartyQuotaImpactLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_IMPACT_ORDER: dict[ThirdPartyQuotaImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[ThirdPartyQuotaSignal, ...] = (
    "provider_quota",
    "burst_limit",
    "monthly_cap",
    "paid_tier",
    "provider_throttling",
    "fallback_behavior",
    "quota_alert",
)
_SAFEGUARD_ORDER: tuple[ThirdPartyQuotaSafeguard, ...] = (
    "budget_alarms",
    "retry_backoff",
    "degradation_behavior",
    "cache_strategy",
    "provider_limit_tests",
)
_PROVIDER_PATTERNS: dict[str, re.Pattern[str]] = {
    "Anthropic": re.compile(r"\banthropic\b", re.I),
    "AWS": re.compile(r"\b(?:aws|amazon web services|ses|s3|bedrock)\b", re.I),
    "GitHub": re.compile(r"\bgithub\b", re.I),
    "Google Maps": re.compile(r"\bgoogle maps\b", re.I),
    "Mapbox": re.compile(r"\bmapbox\b", re.I),
    "OpenAI": re.compile(r"\bopenai\b", re.I),
    "Salesforce": re.compile(r"\bsalesforce\b", re.I),
    "SendGrid": re.compile(r"\bsendgrid\b", re.I),
    "Shopify": re.compile(r"\bshopify\b", re.I),
    "Slack": re.compile(r"\bslack\b", re.I),
    "Stripe": re.compile(r"\bstripe\b", re.I),
    "Twilio": re.compile(r"\btwilio\b", re.I),
    "Zendesk": re.compile(r"\bzendesk\b", re.I),
}
_SIGNAL_PATTERNS: dict[ThirdPartyQuotaSignal, re.Pattern[str]] = {
    "provider_quota": re.compile(
        r"\b(?:provider quota|vendor quota|third[- ]party quota|api quota|external api quota|"
        r"usage quota|quota exhaustion|quota limit|quota exceeded|provider cap|usage cap|"
        r"external provider limit)\b",
        re.I,
    ),
    "burst_limit": re.compile(
        r"\b(?:burst limit|burst cap|burst quota|requests? per second|rps|requests? per minute|rpm|"
        r"per[- ]minute cap|per[- ]second cap|concurrency cap|spike limit)\b",
        re.I,
    ),
    "monthly_cap": re.compile(
        r"\b(?:monthly cap|monthly quota|monthly limit|monthly usage|billing month|per month|"
        r"month(?:ly)? allowance|usage allowance|credit cap|token cap)\b",
        re.I,
    ),
    "paid_tier": re.compile(
        r"\b(?:paid tier|free tier|billing tier|plan tier|upgrade tier|usage based billing|"
        r"metered billing|subscription limit|enterprise plan|billing limit)\b",
        re.I,
    ),
    "provider_throttling": re.compile(
        r"\b(?:provider throttling|vendor throttling|throttled by provider|429|too many requests|"
        r"retry-after|rate limited by|provider rate limit|quota throttle)\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback|degraded mode|degradation|graceful degradation|fail open|fail closed|"
        r"queue for later|serve cached|alternate provider|secondary provider)\b",
        re.I,
    ),
    "quota_alert": re.compile(
        r"\b(?:quota alert|quota alarm|budget alert|budget alarm|usage alert|billing alert|"
        r"quota monitor|usage monitor|threshold alert|spend alert)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ThirdPartyQuotaSignal, re.Pattern[str]] = {
    "provider_quota": re.compile(r"(?:quota|usage[_-]?cap|provider[_-]?limit|vendor[_-]?limit)", re.I),
    "burst_limit": re.compile(r"(?:burst|rpm|rps|concurrency[_-]?cap)", re.I),
    "monthly_cap": re.compile(r"(?:monthly|billing|usage[_-]?allowance|credit[_-]?cap)", re.I),
    "paid_tier": re.compile(r"(?:paid[_-]?tier|free[_-]?tier|billing[_-]?tier|plan[_-]?tier)", re.I),
    "provider_throttling": re.compile(r"(?:throttl|rate[_-]?limit|retry[_-]?after|429)", re.I),
    "fallback_behavior": re.compile(r"(?:fallback|degrad|secondary[_-]?provider|serve[_-]?cached)", re.I),
    "quota_alert": re.compile(r"(?:quota[_-]?alert|budget[_-]?alarm|usage[_-]?alert|spend[_-]?alert)", re.I),
}
_SAFEGUARD_PATTERNS: dict[ThirdPartyQuotaSafeguard, re.Pattern[str]] = {
    "budget_alarms": re.compile(
        r"\b(?:budget alarms?|budget alerts?|billing alerts?|spend alerts?|quota alerts?|"
        r"usage alerts?|threshold alerts?|quota monitoring|usage monitoring)\b",
        re.I,
    ),
    "retry_backoff": re.compile(
        r"\b(?:retry/backoff|retry backoff|exponential backoff|backoff|jitter|retry-after|"
        r"retry budget|client throttling|token bucket|leaky bucket)\b",
        re.I,
    ),
    "degradation_behavior": re.compile(
        r"\b(?:degradation behavior|degraded mode|graceful degradation|fallback behavior|"
        r"fallback|fail open|fail closed|queue for later|alternate provider|secondary provider)\b",
        re.I,
    ),
    "cache_strategy": re.compile(
        r"\b(?:cache strategy|caching strategy|serve cached|cached response|reuse cached|"
        r"cache ttl|request cache|response cache|memoize|dedupe requests?)\b",
        re.I,
    ),
    "provider_limit_tests": re.compile(
        r"\b(?:provider[- ]limit tests?|quota tests?|quota exhaustion tests?|billing cap tests?|"
        r"throttling tests?|429 tests?|retry-after tests?|limit simulation|quota simulation|"
        r"(?:test|tests|pytest|simulate|simulation).{0,80}(?:quota exhaustion|billing cap|429)|"
        r"(?:quota exhaustion|billing cap|429).{0,80}(?:test|tests|simulate|simulation))\b",
        re.I,
    ),
}
_RECOMMENDATIONS: dict[ThirdPartyQuotaSafeguard, str] = {
    "budget_alarms": "Add budget or quota alarms before provider usage reaches billing or service caps.",
    "retry_backoff": "Define retry, backoff, jitter, and client-side throttling behavior for provider throttles.",
    "degradation_behavior": "Specify degradation or fallback behavior when the provider quota is exhausted.",
    "cache_strategy": "Use caching, request deduplication, or reuse of provider responses to reduce quota burn.",
    "provider_limit_tests": "Add provider-limit tests or simulations for quota exhaustion, 429s, and billing caps.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:third[- ]party|provider|vendor|external api|quota|billing cap|"
    r"usage cap|throttling)\b.{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)
_PROVIDER_KEYS = ("provider", "providers", "provider_name", "vendor", "vendors", "service")


@dataclass(frozen=True, slots=True)
class TaskThirdPartyQuotaReadinessRecord:
    """Third-party quota readiness guidance for one execution task."""

    task_id: str
    title: str
    impact_level: ThirdPartyQuotaImpactLevel
    matched_signals: tuple[ThirdPartyQuotaSignal, ...]
    provider_name: str | None = None
    present_safeguards: tuple[ThirdPartyQuotaSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ThirdPartyQuotaSafeguard, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def risk_level(self) -> ThirdPartyQuotaImpactLevel:
        """Compatibility view for planners that expose risk as the impact field."""
        return self.impact_level

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impact_level": self.impact_level,
            "matched_signals": list(self.matched_signals),
            "provider_name": self.provider_name,
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskThirdPartyQuotaReadinessPlan:
    """Plan-level third-party quota readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskThirdPartyQuotaReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskThirdPartyQuotaReadinessRecord, ...]:
        """Compatibility view matching planners that call records findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskThirdPartyQuotaReadinessRecord, ...]:
        """Compatibility view matching planners that call records recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return quota readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render third-party quota readiness as deterministic Markdown."""
        title = "# Task Third-Party Quota Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        impact_counts = self.summary.get("impact_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguards count: {self.summary.get('missing_safeguards_count', 0)}",
            "- Impact counts: "
            + ", ".join(f"{impact} {impact_counts.get(impact, 0)}" for impact in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No third-party quota readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Impact | Signals | Provider | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(record.provider_name or 'unspecified')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_third_party_quota_readiness_plan(source: Any) -> TaskThirdPartyQuotaReadinessPlan:
    """Build third-party quota readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    impacted_set = set(impacted_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted_set
    )
    return TaskThirdPartyQuotaReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_third_party_quota_readiness(source: Any) -> TaskThirdPartyQuotaReadinessPlan:
    """Compatibility alias for building third-party quota readiness plans."""
    return build_task_third_party_quota_readiness_plan(source)


def recommend_task_third_party_quota_readiness(source: Any) -> TaskThirdPartyQuotaReadinessPlan:
    """Compatibility alias for recommending third-party quota readiness plans."""
    return build_task_third_party_quota_readiness_plan(source)


def extract_task_third_party_quota_readiness(source: Any) -> TaskThirdPartyQuotaReadinessPlan:
    """Compatibility alias for extracting third-party quota readiness plans."""
    return build_task_third_party_quota_readiness_plan(source)


def generate_task_third_party_quota_readiness(source: Any) -> TaskThirdPartyQuotaReadinessPlan:
    """Compatibility alias for generating third-party quota readiness plans."""
    return build_task_third_party_quota_readiness_plan(source)


def task_third_party_quota_readiness_plan_to_dict(
    result: TaskThirdPartyQuotaReadinessPlan,
) -> dict[str, Any]:
    """Serialize a third-party quota readiness plan to a plain dictionary."""
    return result.to_dict()


task_third_party_quota_readiness_plan_to_dict.__test__ = False


def task_third_party_quota_readiness_plan_to_dicts(
    result: TaskThirdPartyQuotaReadinessPlan | Iterable[TaskThirdPartyQuotaReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize third-party quota readiness records to plain dictionaries."""
    if isinstance(result, TaskThirdPartyQuotaReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_third_party_quota_readiness_plan_to_dicts.__test__ = False


def task_third_party_quota_readiness_plan_to_markdown(
    result: TaskThirdPartyQuotaReadinessPlan,
) -> str:
    """Render a third-party quota readiness plan as Markdown."""
    return result.to_markdown()


task_third_party_quota_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    matched_signals: tuple[ThirdPartyQuotaSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ThirdPartyQuotaSafeguard, ...] = field(default_factory=tuple)
    provider_name: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _record(task: Mapping[str, Any], index: int) -> TaskThirdPartyQuotaReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.matched_signals:
        return None

    missing = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskThirdPartyQuotaReadinessRecord(
        task_id=task_id,
        title=title,
        impact_level=_impact_level(set(signals.matched_signals), set(signals.present_safeguards), missing),
        matched_signals=signals.matched_signals,
        provider_name=signals.provider_name,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        recommended_checks=tuple(_RECOMMENDATIONS[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ThirdPartyQuotaSignal] = set()
    safeguard_hits: set[ThirdPartyQuotaSafeguard] = set()
    providers: list[str] = []
    evidence: list[str] = []
    explicitly_no_impact = False

    metadata = task.get("metadata")
    providers.extend(_strings_from_keys(metadata, _PROVIDER_KEYS))

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        providers.extend(_providers_from_text(searchable))
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        providers.extend([*_providers_from_text(text), *_providers_from_text(searchable)])
        if matched:
            evidence.append(snippet)

    provider_name = _dedupe(providers)[0] if _dedupe(providers) else None
    if provider_name and any(signal in signal_hits for signal in ("monthly_cap", "paid_tier", "provider_throttling")):
        signal_hits.add("provider_quota")

    return _Signals(
        matched_signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits
        ),
        provider_name=provider_name,
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _impact_level(
    signals: set[ThirdPartyQuotaSignal],
    present: set[ThirdPartyQuotaSafeguard],
    missing: tuple[ThirdPartyQuotaSafeguard, ...],
) -> ThirdPartyQuotaImpactLevel:
    exhaustion_signals = signals & {"provider_quota", "monthly_cap", "paid_tier", "provider_throttling"}
    if {"monthly_cap", "paid_tier"} <= signals and "budget_alarms" in missing:
        return "high"
    if "provider_throttling" in signals and "retry_backoff" in missing:
        return "high"
    if exhaustion_signals and "degradation_behavior" in missing and "fallback_behavior" not in signals:
        return "high"
    if len(signals) >= 3 and len(missing) >= 3:
        return "high"
    if "burst_limit" in signals and "retry_backoff" in missing:
        return "medium"
    if len(missing) >= 3:
        return "medium"
    if signals and not present:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskThirdPartyQuotaReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "no_impact_task_count": len(no_impact_task_ids),
        "missing_safeguards_count": sum(len(record.missing_safeguards) for record in records),
        "impact_counts": {
            impact: sum(1 for record in records if record.impact_level == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
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
    return bool(
        any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])
        or value in {key.replace("_", " ") for key in _PROVIDER_KEYS}
    )


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


def _providers_from_text(text: str) -> list[str]:
    return [provider for provider, pattern in _PROVIDER_PATTERNS.items() if pattern.search(text)]


def _strings_from_keys(value: Any, keys: tuple[str, ...]) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    strings: list[str] = []
    for key in keys:
        strings.extend(_strings(value.get(key)))
    return strings


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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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
    "TaskThirdPartyQuotaReadinessPlan",
    "TaskThirdPartyQuotaReadinessRecord",
    "ThirdPartyQuotaImpactLevel",
    "ThirdPartyQuotaSafeguard",
    "ThirdPartyQuotaSignal",
    "analyze_task_third_party_quota_readiness",
    "build_task_third_party_quota_readiness_plan",
    "extract_task_third_party_quota_readiness",
    "generate_task_third_party_quota_readiness",
    "recommend_task_third_party_quota_readiness",
    "task_third_party_quota_readiness_plan_to_dict",
    "task_third_party_quota_readiness_plan_to_dicts",
    "task_third_party_quota_readiness_plan_to_markdown",
]
