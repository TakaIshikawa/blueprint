"""Identify task-level API rate limit and quota risks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ApiRateLimitImpactCategory = Literal[
    "inbound_endpoint",
    "outbound_vendor",
    "batch_burst",
    "retry_amplification",
    "pagination_backoff",
    "customer_quota_messaging",
]
ApiRateLimitSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[ApiRateLimitImpactCategory, int] = {
    "inbound_endpoint": 0,
    "outbound_vendor": 1,
    "batch_burst": 2,
    "retry_amplification": 3,
    "pagination_backoff": 4,
    "customer_quota_messaging": 5,
}
_SEVERITY_ORDER: dict[ApiRateLimitSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_PATTERNS: dict[ApiRateLimitImpactCategory, re.Pattern[str]] = {
    "inbound_endpoint": re.compile(
        r"\b(?:api endpoint|endpoint|route|controller|rest api|graphql|public api|"
        r"webhook receiver|incoming webhook|throttle|rate limit(?:ing)?|quota)\b",
        re.I,
    ),
    "outbound_vendor": re.compile(
        r"\b(?:vendor api|third[- ]party api|external api|provider api|stripe|adyen|paypal|"
        r"twilio|sendgrid|mailgun|postmark|openai|github|slack|salesforce|shopify|zendesk|"
        r"google maps|mapbox|aws|ses|s3|rate limit(?:ing)?|quota)\b",
        re.I,
    ),
    "batch_burst": re.compile(
        r"\b(?:batch|bulk|backfill|cron|scheduled|nightly|hourly|job|worker|queue|import|"
        r"export|sync|burst|fan[- ]?out|campaign|many tenants|thousands|millions)\b",
        re.I,
    ),
    "retry_amplification": re.compile(
        r"\b(?:retry|retries|retry storm|amplification|exponential backoff|backoff|"
        r"429|too many requests|timeout|transient failure|circuit breaker|idempotenc)\b",
        re.I,
    ),
    "pagination_backoff": re.compile(
        r"\b(?:pagination|paginate|cursor|page size|page token|offset|limit|retry-after|"
        r"backoff|429|too many requests)\b",
        re.I,
    ),
    "customer_quota_messaging": re.compile(
        r"\b(?:quota exceeded|limit exceeded|limit reached|usage limit|plan limit|rate limit message|"
        r"429 message|customer[- ]facing|customer messaging|upgrade prompt|support copy)\b",
        re.I,
    ),
}
_RATE_LIMIT_RE = re.compile(r"\b(?:rate limit(?:ing|ed)?|throttle|quota|429|too many requests|retry-after)\b", re.I)
_API_RE = re.compile(r"\b(?:api|endpoint|route|graphql|rest|webhook|vendor|provider|integration|http)\b", re.I)
_PROVIDER_PATTERNS: dict[str, re.Pattern[str]] = {
    "Adyen": re.compile(r"\badyen\b", re.I),
    "AWS": re.compile(r"\b(?:aws|amazon web services|s3|ses)\b", re.I),
    "GitHub": re.compile(r"\bgithub\b", re.I),
    "Google Maps": re.compile(r"\bgoogle maps\b", re.I),
    "Mailgun": re.compile(r"\bmailgun\b", re.I),
    "Mapbox": re.compile(r"\bmapbox\b", re.I),
    "OpenAI": re.compile(r"\bopenai\b", re.I),
    "PayPal": re.compile(r"\bpaypal\b", re.I),
    "Postmark": re.compile(r"\bpostmark\b", re.I),
    "Salesforce": re.compile(r"\bsalesforce\b", re.I),
    "SendGrid": re.compile(r"\bsendgrid\b", re.I),
    "Shopify": re.compile(r"\bshopify\b", re.I),
    "Slack": re.compile(r"\bslack\b", re.I),
    "Stripe": re.compile(r"\bstripe\b", re.I),
    "Twilio": re.compile(r"\btwilio\b", re.I),
    "Zendesk": re.compile(r"\bzendesk\b", re.I),
}
_LIMIT_VALUE_RE = re.compile(
    r"\b(?:\d+(?:\.\d+)?\s*(?:req(?:uests)?|calls|messages|emails|jobs|tokens|rpm|rps)"
    r"(?:\s*/\s*|\s+per\s+)(?:second|minute|hour|day|month)|\d+\s*(?:rpm|rps))\b",
    re.I,
)
_OWNER_KEYS = (
    "quota_owner",
    "rate_limit_owner",
    "limit_owner",
    "owner",
    "owners",
    "assignee",
    "assignees",
    "dri",
    "oncall",
    "on_call",
)
_LIMIT_KEYS = (
    "limit_value",
    "quota_limit",
    "rate_limit",
    "rate_limit_value",
    "quota",
    "limits",
)
_PROVIDER_KEYS = ("provider_name", "provider", "providers", "vendor", "vendors", "service")
_ARTIFACT_KEYS = (
    "mitigation_artifacts",
    "rate_limit_artifacts",
    "quota_artifacts",
    "mitigations",
    "safeguards",
    "runbooks",
)
_CATEGORY_KEYS = (
    "rate_limit_categories",
    "api_rate_limit_categories",
    "quota_categories",
    "impact_categories",
    "categories",
)


@dataclass(frozen=True, slots=True)
class TaskApiRateLimitImpactRecord:
    """API rate limit and quota guidance for one execution task category."""

    task_id: str
    title: str
    category: ApiRateLimitImpactCategory
    severity: ApiRateLimitSeverity
    quota_owner: str | None = None
    limit_value: str | None = None
    provider_name: str | None = None
    mitigation_artifacts: tuple[str, ...] = field(default_factory=tuple)
    context_signals: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "category": self.category,
            "severity": self.severity,
            "quota_owner": self.quota_owner,
            "limit_value": self.limit_value,
            "provider_name": self.provider_name,
            "mitigation_artifacts": list(self.mitigation_artifacts),
            "context_signals": list(self.context_signals),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskApiRateLimitImpactPlan:
    """Plan-level API rate limit and quota impact review."""

    plan_id: str | None = None
    records: tuple[TaskApiRateLimitImpactRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

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
        """Return API rate limit impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[TaskApiRateLimitImpactRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the API rate limit impact plan as deterministic Markdown."""
        title = "# Task API Rate Limit Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        category_counts = self.summary.get("category_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('impacted_task_count', 0)} impacted tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no impact: {self.summary.get('no_impact_task_count', 0)})."
            ),
            (
                "Categories: "
                f"inbound_endpoint {category_counts.get('inbound_endpoint', 0)}, "
                f"outbound_vendor {category_counts.get('outbound_vendor', 0)}, "
                f"batch_burst {category_counts.get('batch_burst', 0)}, "
                f"retry_amplification {category_counts.get('retry_amplification', 0)}, "
                f"pagination_backoff {category_counts.get('pagination_backoff', 0)}, "
                f"customer_quota_messaging {category_counts.get('customer_quota_messaging', 0)}."
            ),
            (
                "Severity: "
                f"high {severity_counts.get('high', 0)}, "
                f"medium {severity_counts.get('medium', 0)}, "
                f"low {severity_counts.get('low', 0)}."
            ),
        ]
        if not self.records:
            lines.extend(["", "No API rate limit impact records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Category | Severity | Owner | Limit | Provider | Mitigation Artifacts | Context | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.category} | "
                f"{record.severity} | "
                f"{_markdown_cell(record.quota_owner or 'unspecified')} | "
                f"{_markdown_cell(record.limit_value or 'unspecified')} | "
                f"{_markdown_cell(record.provider_name or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.mitigation_artifacts) or 'none')} | "
                f"{_markdown_cell(', '.join(record.context_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_api_rate_limit_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiRateLimitImpactPlan:
    """Build task-level API rate limit and quota impact guidance."""
    plan_id, tasks = _source_payload(source)
    task_records = [_task_records(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for records_for_task in task_records for record in records_for_task),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                record.task_id,
                _CATEGORY_ORDER[record.category],
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if task_records[index - 1]
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if not task_records[index - 1]
    )
    category_counts = {
        category: sum(1 for record in records if record.category == category)
        for category in _CATEGORY_ORDER
    }
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    return TaskApiRateLimitImpactPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "record_count": len(records),
            "impacted_task_count": len(impacted_task_ids),
            "no_impact_task_count": len(no_impact_task_ids),
            "category_counts": category_counts,
            "severity_counts": severity_counts,
            "impacted_task_ids": list(impacted_task_ids),
            "no_impact_task_ids": list(no_impact_task_ids),
        },
    )


def derive_task_api_rate_limit_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiRateLimitImpactPlan:
    """Compatibility alias for building API rate limit impact plans."""
    return build_task_api_rate_limit_impact_plan(source)


def summarize_task_api_rate_limit_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiRateLimitImpactPlan:
    """Compatibility alias matching other task-level planners."""
    return build_task_api_rate_limit_impact_plan(source)


def summarize_task_api_rate_limit_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiRateLimitImpactPlan:
    """Compatibility alias matching plural task impact helper names."""
    return build_task_api_rate_limit_impact_plan(source)


def task_api_rate_limit_impact_plan_to_dict(
    result: TaskApiRateLimitImpactPlan,
) -> dict[str, Any]:
    """Serialize an API rate limit impact plan to a plain dictionary."""
    return result.to_dict()


task_api_rate_limit_impact_plan_to_dict.__test__ = False


def task_api_rate_limit_impact_plan_to_markdown(
    result: TaskApiRateLimitImpactPlan,
) -> str:
    """Render an API rate limit impact plan as Markdown."""
    return result.to_markdown()


task_api_rate_limit_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskSignals:
    categories: tuple[ApiRateLimitImpactCategory, ...]
    quota_owner: str | None
    limit_value: str | None
    provider_name: str | None
    mitigation_artifacts: tuple[str, ...]
    context_signals: tuple[str, ...]
    evidence_by_category: Mapping[ApiRateLimitImpactCategory, tuple[str, ...]]
    general_evidence: tuple[str, ...]


def _task_records(task: Mapping[str, Any], index: int) -> tuple[TaskApiRateLimitImpactRecord, ...]:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    records: list[TaskApiRateLimitImpactRecord] = []
    for category in signals.categories:
        evidence = tuple(
            _dedupe(
                [
                    *signals.evidence_by_category.get(category, ()),
                    *signals.general_evidence,
                ]
            )
        )
        records.append(
            TaskApiRateLimitImpactRecord(
                task_id=task_id,
                title=title,
                category=category,
                severity=_severity(category, signals),
                quota_owner=signals.quota_owner,
                limit_value=signals.limit_value,
                provider_name=signals.provider_name,
                mitigation_artifacts=_mitigation_artifacts(category, signals),
                context_signals=signals.context_signals,
                evidence=evidence,
            )
        )
    return tuple(records)


def _signals(task: Mapping[str, Any]) -> _TaskSignals:
    categories: set[ApiRateLimitImpactCategory] = set()
    evidence_by_category: dict[ApiRateLimitImpactCategory, list[str]] = {
        category: [] for category in _CATEGORY_ORDER
    }
    general_evidence: list[str] = []
    context_signals: list[str] = []
    providers: list[str] = []
    limit_values: list[str] = []

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for category in _metadata_categories(metadata):
            categories.add(category)
            evidence_by_category[category].append(f"metadata.rate_limit_categories: {category}")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        snippet = f"files_or_modules: {path}"
        for category in _path_categories(normalized, path_text):
            categories.add(category)
            evidence_by_category[category].append(snippet)
        providers.extend(_providers_from_text(path_text))
        context_signals.extend(_context_signals(path_text))

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for category, pattern in _CATEGORY_PATTERNS.items():
            if pattern.search(text):
                if _category_is_relevant(category, text):
                    categories.add(category)
                    evidence_by_category[category].append(snippet)
                    matched = True
        if _RATE_LIMIT_RE.search(text) and _API_RE.search(text):
            general_evidence.append(snippet)
            matched = True
        providers.extend(_providers_from_text(text))
        limit_values.extend(match.group(0) for match in _LIMIT_VALUE_RE.finditer(text))
        found_contexts = _context_signals(text)
        if found_contexts:
            context_signals.extend(found_contexts)
            matched = True
        if matched and source_field.startswith("metadata."):
            general_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for category, pattern in _CATEGORY_PATTERNS.items():
            if (pattern.search(command) or pattern.search(command_text)) and (
                _category_is_relevant(category, command) or _category_is_relevant(category, command_text)
            ):
                categories.add(category)
                evidence_by_category[category].append(snippet)
        providers.extend([*_providers_from_text(command), *_providers_from_text(command_text)])
        limit_values.extend(match.group(0) for match in _LIMIT_VALUE_RE.finditer(command))

    if "outbound_vendor" in categories and not providers:
        providers.extend(_strings_from_keys(metadata, _PROVIDER_KEYS))
    providers = [*_strings_from_keys(metadata, _PROVIDER_KEYS), *providers]
    limit_values = [*_strings_from_keys(metadata, _LIMIT_KEYS), *limit_values]
    quota_owners = _strings_from_keys(metadata, _OWNER_KEYS)
    artifacts = _strings_from_keys(metadata, _ARTIFACT_KEYS)

    return _TaskSignals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in categories),
        quota_owner=_dedupe(quota_owners)[0] if _dedupe(quota_owners) else None,
        limit_value=_dedupe(limit_values)[0] if _dedupe(limit_values) else None,
        provider_name=_dedupe(providers)[0] if _dedupe(providers) else None,
        mitigation_artifacts=tuple(_dedupe(artifacts)),
        context_signals=tuple(_dedupe(context_signals)),
        evidence_by_category={
            category: tuple(_dedupe(values)) for category, values in evidence_by_category.items()
        },
        general_evidence=tuple(_dedupe(general_evidence)),
    )


def _path_categories(
    normalized: str,
    path_text: str,
) -> tuple[ApiRateLimitImpactCategory, ...]:
    posix = PurePosixPath(normalized.casefold())
    parts = set(posix.parts)
    categories: set[ApiRateLimitImpactCategory] = set()
    if {"routes", "controllers", "endpoints", "graphql"} & parts or any(
        token in path_text for token in ("endpoint", "route", "controller", "graphql")
    ):
        categories.add("inbound_endpoint")
    if {"integrations", "vendors", "providers", "clients"} & parts or _providers_from_text(path_text):
        categories.add("outbound_vendor")
    if any(token in path_text for token in ("batch", "bulk", "backfill", "cron", "scheduled", "worker", "queue")):
        categories.add("batch_burst")
    if any(token in path_text for token in ("retry", "backoff", "circuit_breaker", "idempotenc")):
        categories.add("retry_amplification")
    if any(token in path_text for token in ("pagination", "paginate", "cursor", "page_size", "retry_after")):
        categories.add("pagination_backoff")
    if any(token in path_text for token in ("quota", "usage_limit", "rate_limit_message", "upgrade_prompt")):
        categories.add("customer_quota_messaging")
    return tuple(category for category in _CATEGORY_ORDER if category in categories)


def _category_is_relevant(category: ApiRateLimitImpactCategory, text: str) -> bool:
    if category == "inbound_endpoint":
        return bool(
            re.search(r"\b(?:api endpoint|endpoint|route|controller|rest api|graphql|public api|incoming webhook|webhook receiver)\b", text, re.I)
        )
    if category == "outbound_vendor":
        return bool(
            re.search(r"\b(?:vendor api|third[- ]party api|external api|provider api|integration client)\b", text, re.I)
            or _providers_from_text(text)
        )
    if category == "batch_burst":
        return bool(
            re.search(
                r"\b(?:batch|bulk|backfill|cron|scheduled|nightly|hourly|job|worker|queue|import|export|burst|fan[- ]?out|campaign|thousands|millions)\b",
                text,
                re.I,
            )
            and (_RATE_LIMIT_RE.search(text) or _API_RE.search(text))
        )
    if category == "retry_amplification":
        return bool(
            re.search(
                r"\b(?:retries|retry (?:storm|amplification|failed|policy|budget|rate|calls?)|amplification|429|too many requests|transient failure|circuit breaker|idempotenc)\b",
                text,
                re.I,
            )
        )
    if category == "pagination_backoff":
        return bool(re.search(r"\b(?:pagination|paginate|cursor|page size|page token|offset)\b", text, re.I))
    return True


def _metadata_categories(metadata: Mapping[str, Any]) -> tuple[ApiRateLimitImpactCategory, ...]:
    categories: list[ApiRateLimitImpactCategory] = []
    for key in _CATEGORY_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = value.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
            aliases = {
                "inbound": "inbound_endpoint",
                "endpoint": "inbound_endpoint",
                "outbound": "outbound_vendor",
                "vendor": "outbound_vendor",
                "batch": "batch_burst",
                "burst": "batch_burst",
                "retry": "retry_amplification",
                "pagination": "pagination_backoff",
                "customer": "customer_quota_messaging",
                "messaging": "customer_quota_messaging",
            }
            normalized = aliases.get(normalized, normalized)
            if normalized in _CATEGORY_ORDER:
                categories.append(normalized)  # type: ignore[arg-type]
    return tuple(_dedupe(categories))


def _severity(
    category: ApiRateLimitImpactCategory,
    signals: _TaskSignals,
) -> ApiRateLimitSeverity:
    contexts = set(signals.context_signals)
    if contexts & {"payment", "auth", "scheduled_bulk"}:
        return "high"
    if category == "customer_quota_messaging" and "customer_facing" in contexts:
        return "high"
    if category in {"batch_burst", "retry_amplification", "pagination_backoff"}:
        return "medium"
    if category == "outbound_vendor" and not signals.provider_name:
        return "medium"
    if category == "inbound_endpoint" and not signals.limit_value:
        return "medium"
    return "low"


def _mitigation_artifacts(
    category: ApiRateLimitImpactCategory,
    signals: _TaskSignals,
) -> tuple[str, ...]:
    defaults = {
        "inbound_endpoint": (
            "Inbound throttle policy with per-tenant or per-user keys.",
            "429 response contract with Retry-After and observability tags.",
        ),
        "outbound_vendor": (
            "Vendor quota inventory with owner, limit, reset window, and escalation path.",
            "Client-side throttling and queueing around provider calls.",
        ),
        "batch_burst": (
            "Batch concurrency cap, chunk size, and schedule window.",
            "Burst rehearsal using representative task volume.",
        ),
        "retry_amplification": (
            "Retry budget with exponential backoff, jitter, idempotency, and circuit breaker behavior.",
            "Alerting for retry rate, 429s, and provider saturation.",
        ),
        "pagination_backoff": (
            "Cursor or page-token contract with maximum page size.",
            "Backoff handling for 429 and Retry-After responses.",
        ),
        "customer_quota_messaging": (
            "Customer-facing quota copy for limit reached, retry timing, and upgrade or support path.",
            "Support runbook for quota-related incidents and overrides.",
        ),
    }
    return tuple(_dedupe([*signals.mitigation_artifacts, *defaults[category]]))


def _providers_from_text(text: str) -> list[str]:
    return [provider for provider, pattern in _PROVIDER_PATTERNS.items() if pattern.search(text)]


def _context_signals(text: str) -> list[str]:
    signals: list[str] = []
    if re.search(r"\b(?:payment|payments|checkout|billing|invoice|stripe|adyen|paypal)\b", text, re.I):
        signals.append("payment")
    if re.search(r"\b(?:auth|authentication|authorization|oauth|login|sign[- ]?in|token|password)\b", text, re.I):
        signals.append("auth")
    if re.search(r"\b(?:customer[- ]facing|public|customer|user[- ]visible|checkout|portal|self[- ]serve)\b", text, re.I):
        signals.append("customer_facing")
    if re.search(r"\b(?:scheduled bulk|bulk|batch|backfill|cron|nightly|campaign|thousands|millions)\b", text, re.I):
        signals.append("scheduled_bulk")
    return signals


def _strings_from_keys(value: Any, keys: tuple[str, ...]) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    strings: list[str] = []
    for key in keys:
        strings.extend(_strings(value.get(key)))
    return strings


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    for field_name in ("files_or_modules", "files", "acceptance_criteria", "depends_on", "dependencies", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _metadata_key_has_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_has_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _metadata_key_has_signal(key_text):
                texts.append((field, str(key)))
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


def _metadata_key_has_signal(text: str) -> bool:
    return bool(
        _RATE_LIMIT_RE.search(text)
        or _API_RE.search(text)
        or any(pattern.search(text) for pattern in _CATEGORY_PATTERNS.values())
        or text in {key.replace("_", " ") for key in (*_OWNER_KEYS, *_LIMIT_KEYS, *_PROVIDER_KEYS, *_ARTIFACT_KEYS)}
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "ApiRateLimitImpactCategory",
    "ApiRateLimitSeverity",
    "TaskApiRateLimitImpactPlan",
    "TaskApiRateLimitImpactRecord",
    "build_task_api_rate_limit_impact_plan",
    "derive_task_api_rate_limit_impact_plan",
    "summarize_task_api_rate_limit_impact",
    "summarize_task_api_rate_limit_impacts",
    "task_api_rate_limit_impact_plan_to_dict",
    "task_api_rate_limit_impact_plan_to_markdown",
]
