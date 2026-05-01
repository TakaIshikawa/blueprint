"""Plan external service touchpoints for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


ExternalServiceCategory = Literal[
    "source_control",
    "chat",
    "issue_tracker",
    "llm_provider",
    "email",
    "webhook",
    "payments",
    "object_storage",
    "database",
    "external_api",
]
_T = TypeVar("_T")

_PATH_STRIP_RE = re.compile(r"^[`'\",;:(){}\[\]\s]+|[`'\",;:(){}\[\]\s.]+$")
_GENERIC_INTEGRATION_RE = re.compile(
    r"\b(?:api|client|integration|integrations|external|third[- ]?party|provider|"
    r"service|sync|webhook|callback|credential|credentials|token|secret|sandbox|"
    r"database|db|storage|bucket|email|payment|llm|model)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskExternalServiceEvidence:
    """One signal linking a task to an external service."""

    source: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "source": self.source,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class TaskExternalServiceTouchpoint:
    """Guidance for one external service touched by one task."""

    service: str
    category: ExternalServiceCategory
    credential_hint: str
    failure_mode: str
    retry_or_fallback_guidance: str
    validation_evidence: str
    evidence: tuple[TaskExternalServiceEvidence, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "service": self.service,
            "category": self.category,
            "credential_hint": self.credential_hint,
            "failure_mode": self.failure_mode,
            "retry_or_fallback_guidance": self.retry_or_fallback_guidance,
            "validation_evidence": self.validation_evidence,
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class TaskExternalServiceTouchpoints:
    """External service touchpoints for one execution task."""

    task_id: str
    title: str
    touchpoints: tuple[TaskExternalServiceTouchpoint, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "touchpoints": [item.to_dict() for item in self.touchpoints],
        }


@dataclass(frozen=True, slots=True)
class TaskExternalServiceTouchpointReport:
    """Plan-level external service touchpoint report."""

    plan_id: str
    tasks: tuple[TaskExternalServiceTouchpoints, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "tasks": [task.to_dict() for task in self.tasks],
            "summary": dict(self.summary),
        }


@dataclass(frozen=True, slots=True)
class _ServiceRule:
    service: str
    category: ExternalServiceCategory
    patterns: tuple[str, ...]
    credential_hint: str
    failure_mode: str
    retry_or_fallback_guidance: str
    validation_evidence: str
    context_patterns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task_id: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    validation_commands: tuple[str, ...]
    files_or_modules: tuple[str, ...]
    metadata: Mapping[str, Any]


_SERVICE_RULES: tuple[_ServiceRule, ...] = (
    _ServiceRule(
        service="GitHub",
        category="source_control",
        patterns=("github", "gh api", "github api", "octokit", ".github/"),
        credential_hint="GitHub token with the narrowest repo, issue, or workflow scope needed.",
        failure_mode="Rate limits, permission errors, missing repository access, or unavailable GitHub API.",
        retry_or_fallback_guidance="Retry transient 5xx and rate-limit responses with backoff; surface permission failures as actionable configuration errors.",
        validation_evidence="Mock GitHub API calls or use recorded contract fixtures that cover success, rate limit, and permission failure responses.",
        context_patterns=("repo", "pull request", "issue", "workflow", "source control"),
    ),
    _ServiceRule(
        service="Slack",
        category="chat",
        patterns=("slack", "slack api", "slack_sdk", "slack webhook"),
        credential_hint="Slack bot token or incoming webhook URL for the target workspace/channel.",
        failure_mode="Invalid token, missing channel permissions, rate limits, or message delivery failure.",
        retry_or_fallback_guidance="Retry rate-limited and transient Slack API responses; fall back to storing an undelivered notification for replay.",
        validation_evidence="Validate with mocked Slack responses for success, channel-not-found, invalid-auth, and rate-limit cases.",
        context_patterns=("chat", "notification", "digest", "message"),
    ),
    _ServiceRule(
        service="Jira",
        category="issue_tracker",
        patterns=("jira", "atlassian", "jira api"),
        credential_hint="Jira API token and site/project key configuration.",
        failure_mode="Invalid credentials, project key mismatch, field validation errors, or rate limits.",
        retry_or_fallback_guidance="Retry transient failures; report field-mapping errors without retrying.",
        validation_evidence="Use contract fixtures for create/update issue payloads and Jira validation error responses.",
        context_patterns=("ticket", "issue", "project", "backlog"),
    ),
    _ServiceRule(
        service="Linear",
        category="issue_tracker",
        patterns=("linear", "linear api", "linear.app"),
        credential_hint="Linear API key and team/workspace identifiers.",
        failure_mode="Authentication failure, team mismatch, schema validation errors, or GraphQL throttling.",
        retry_or_fallback_guidance="Retry throttled GraphQL calls with backoff; fail fast on schema and team configuration errors.",
        validation_evidence="Mock Linear GraphQL responses for issue creation, update, throttling, and validation errors.",
        context_patterns=("ticket", "issue", "project", "backlog"),
    ),
    _ServiceRule(
        service="OpenAI",
        category="llm_provider",
        patterns=("openai", "chatgpt", "gpt-", "responses api", "openai api"),
        credential_hint="OpenAI API key plus model and organization/project configuration when required.",
        failure_mode="Invalid API key, model unavailability, quota exhaustion, timeout, or non-deterministic model output.",
        retry_or_fallback_guidance="Retry transient provider failures with idempotency where available; provide deterministic fallback behavior for validation.",
        validation_evidence="Use mocked provider responses or golden fixtures for success, refusal/error, timeout, and quota responses.",
        context_patterns=("llm", "model", "prompt", "completion", "embedding"),
    ),
    _ServiceRule(
        service="Anthropic",
        category="llm_provider",
        patterns=("anthropic", "claude", "messages api"),
        credential_hint="Anthropic API key plus model configuration.",
        failure_mode="Invalid API key, model unavailability, rate limit, timeout, or non-deterministic model output.",
        retry_or_fallback_guidance="Retry transient provider failures with bounded backoff; isolate prompts and fixtures for repeatable validation.",
        validation_evidence="Use mocked Anthropic responses or fixtures for success, safety/error, timeout, and rate-limit cases.",
        context_patterns=("llm", "model", "prompt", "completion"),
    ),
    _ServiceRule(
        service="Email",
        category="email",
        patterns=("email", "smtp", "sendgrid", "mailgun", "ses", "postmark"),
        credential_hint="SMTP credentials or email provider API key plus sender/domain configuration.",
        failure_mode="Invalid sender, bounced delivery, provider rejection, rate limit, or template rendering error.",
        retry_or_fallback_guidance="Retry transient provider failures; queue failed sends and expose permanent rejections for operator review.",
        validation_evidence="Validate rendered email content and mock provider responses for accepted, rejected, bounced, and rate-limited sends.",
        context_patterns=("notification", "message", "digest"),
    ),
    _ServiceRule(
        service="Webhook",
        category="webhook",
        patterns=("webhook", "callback url", "callback signature", "webhook signature"),
        credential_hint="Webhook signing secret, endpoint URL, and event source configuration.",
        failure_mode="Signature verification failure, duplicate delivery, out-of-order events, timeout, or malformed payload.",
        retry_or_fallback_guidance="Make handlers idempotent; retry outbound webhook delivery with backoff and dead-letter exhausted attempts.",
        validation_evidence="Test signed payloads, invalid signatures, duplicate events, retryable failures, and malformed payloads.",
        context_patterns=("callback", "event delivery", "event handler"),
    ),
    _ServiceRule(
        service="Stripe",
        category="payments",
        patterns=("stripe", "stripe api", "stripe webhook"),
        credential_hint="Stripe secret key, webhook signing secret, and sandbox account configuration.",
        failure_mode="Declined payment, idempotency conflict, webhook replay, API rate limit, or invalid account mode.",
        retry_or_fallback_guidance="Use idempotency keys for writes; retry transient API failures and process webhooks idempotently.",
        validation_evidence="Use Stripe sandbox fixtures or mocks for successful payment, decline, replayed webhook, and rate-limit cases.",
        context_patterns=("payment", "billing", "checkout", "invoice", "subscription"),
    ),
    _ServiceRule(
        service="PayPal",
        category="payments",
        patterns=("paypal", "paypal api"),
        credential_hint="PayPal client ID/secret and sandbox or live environment selection.",
        failure_mode="Authorization failure, capture failure, webhook verification failure, or environment mismatch.",
        retry_or_fallback_guidance="Retry transient capture/status checks; reconcile final payment state from provider webhooks.",
        validation_evidence="Mock PayPal order creation, capture, webhook verification, and failed payment responses.",
        context_patterns=("payment", "billing", "checkout", "invoice", "subscription"),
    ),
    _ServiceRule(
        service="Object storage",
        category="object_storage",
        patterns=(
            "s3",
            "aws s3",
            "gcs",
            "google cloud storage",
            "azure blob",
            "object storage",
            "bucket",
        ),
        credential_hint="Storage credentials, bucket/container name, region, and object prefix configuration.",
        failure_mode="Missing bucket permissions, region mismatch, object-not-found, eventual consistency, or upload timeout.",
        retry_or_fallback_guidance="Retry transient upload/download failures; validate object existence and make cleanup idempotent.",
        validation_evidence="Use local storage fakes or mocked SDK calls for upload, download, missing object, and permission-denied cases.",
        context_patterns=("file", "artifact", "upload", "download", "archive"),
    ),
    _ServiceRule(
        service="Database",
        category="database",
        patterns=(
            "postgres",
            "postgresql",
            "mysql",
            "sqlite",
            "mongodb",
            "dynamodb",
            "redis",
            "database",
            "db/",
        ),
        credential_hint="Database DSN or host/user/password plus database name and migration/schema settings.",
        failure_mode="Connection failure, missing migration, transaction conflict, stale cache, or unavailable database.",
        retry_or_fallback_guidance="Retry connection acquisition cautiously; use transactions, idempotent migrations, and explicit cache reset paths.",
        validation_evidence="Run isolated database/cache tests with disposable state covering success, migration mismatch, and connection failure paths.",
        context_patterns=("data", "migration", "schema", "cache", "repository"),
    ),
)


def plan_task_external_service_touchpoints(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskExternalServiceTouchpointReport:
    """Identify external service touchpoints for every task in an execution plan."""
    payload = _plan_payload(plan)
    plan_context = _plan_context(payload)
    tasks = tuple(
        _task_touchpoints(record, plan_context)
        for record in _task_records(_task_payloads(payload.get("tasks")))
    )
    return TaskExternalServiceTouchpointReport(
        plan_id=_optional_text(payload.get("id")) or "plan",
        tasks=tasks,
        summary=_summary(tasks),
    )


def task_external_service_touchpoints_to_dict(
    report: TaskExternalServiceTouchpointReport,
) -> dict[str, Any]:
    """Serialize an external service touchpoint report to a plain dictionary."""
    return report.to_dict()


task_external_service_touchpoints_to_dict.__test__ = False


def _task_touchpoints(
    record: _TaskRecord,
    plan_context: Mapping[str, tuple[str, ...]],
) -> TaskExternalServiceTouchpoints:
    touchpoints: list[TaskExternalServiceTouchpoint] = []
    task_text = _task_text(record)
    generic_context = bool(_GENERIC_INTEGRATION_RE.search(task_text))

    for rule in _SERVICE_RULES:
        evidence = _rule_evidence(rule, record)
        if not evidence and generic_context:
            evidence = _plan_rule_evidence(rule, plan_context)
        if evidence:
            touchpoints.append(
                TaskExternalServiceTouchpoint(
                    service=rule.service,
                    category=rule.category,
                    credential_hint=rule.credential_hint,
                    failure_mode=rule.failure_mode,
                    retry_or_fallback_guidance=rule.retry_or_fallback_guidance,
                    validation_evidence=rule.validation_evidence,
                    evidence=tuple(_dedupe_evidence(evidence)),
                )
            )

    return TaskExternalServiceTouchpoints(
        task_id=record.task_id,
        title=record.title,
        touchpoints=tuple(touchpoints),
    )


def _rule_evidence(
    rule: _ServiceRule,
    record: _TaskRecord,
) -> list[TaskExternalServiceEvidence]:
    evidence: list[TaskExternalServiceEvidence] = []
    source_values = (
        ("description", (record.description,)),
        ("acceptance_criteria", record.acceptance_criteria),
        ("test_command", record.validation_commands),
        ("files_or_modules", record.files_or_modules),
    )
    for source, values in source_values:
        for value in values:
            if _matches_rule(rule, value):
                evidence.append(
                    TaskExternalServiceEvidence(
                        source=source,
                        detail=f"{source} references {rule.service}: {value}",
                    )
                )
                break
    evidence.extend(_metadata_evidence(rule, record.metadata))
    return evidence


def _metadata_evidence(
    rule: _ServiceRule,
    metadata: Mapping[str, Any],
) -> list[TaskExternalServiceEvidence]:
    evidence: list[TaskExternalServiceEvidence] = []
    for source, text in _metadata_texts(metadata):
        if _matches_rule(rule, text):
            evidence.append(
                TaskExternalServiceEvidence(
                    source=source,
                    detail=f"{source} references {rule.service}: {text}",
                )
            )
            break
    return evidence


def _plan_rule_evidence(
    rule: _ServiceRule,
    plan_context: Mapping[str, tuple[str, ...]],
) -> list[TaskExternalServiceEvidence]:
    evidence: list[TaskExternalServiceEvidence] = []
    for source in ("integration_points", "metadata.integration_points"):
        for value in plan_context.get(source, ()):
            if _matches_rule(rule, value):
                evidence.append(
                    TaskExternalServiceEvidence(
                        source=source,
                        detail=f"{source} references {rule.service}: {value}",
                    )
                )
                break
    return evidence


def _matches_rule(rule: _ServiceRule, value: str) -> bool:
    normalized = value.lower()
    return any(_matches_pattern(normalized, pattern) for pattern in rule.patterns)


def _matches_pattern(normalized: str, pattern: str) -> bool:
    if pattern.isalnum() and len(pattern) <= 3:
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(pattern)}(?![a-z0-9])", normalized))
    return pattern in normalized


def _plan_context(payload: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    metadata = payload.get("metadata")
    return {
        "integration_points": tuple(_strings(payload.get("integration_points"))),
        "metadata.integration_points": tuple(
            _strings(metadata.get("integration_points")) if isinstance(metadata, Mapping) else []
        ),
    }


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        metadata = task.get("metadata")
        records.append(
            _TaskRecord(
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                description=_optional_text(task.get("description")) or "",
                acceptance_criteria=tuple(_strings(task.get("acceptance_criteria"))),
                validation_commands=tuple(
                    _dedupe(
                        [
                            *_strings(task.get("test_command")),
                            *_strings(task.get("validation_command")),
                            *_strings(task.get("validation_commands")),
                            *_metadata_commands(metadata),
                        ]
                    )
                ),
                files_or_modules=tuple(
                    _dedupe(
                        _normalized_path(path)
                        for path in [
                            *_strings(task.get("files_or_modules")),
                            *_strings(task.get("expectedFiles")),
                            *_strings(task.get("expected_files")),
                            *_metadata_paths(metadata),
                        ]
                    )
                ),
                metadata=metadata if isinstance(metadata, Mapping) else {},
            )
        )
    return tuple(records)


def _metadata_commands(metadata: Any) -> list[str]:
    if not isinstance(metadata, Mapping):
        return []
    commands: list[str] = []
    for key in ("test_command", "test_commands", "validation_command", "validation_commands"):
        commands.extend(_strings(metadata.get(key)))
    return commands


def _metadata_paths(metadata: Any) -> list[str]:
    if not isinstance(metadata, Mapping):
        return []
    paths: list[str] = []
    for key in ("files", "paths", "expectedFiles", "expected_files"):
        paths.extend(_strings(metadata.get(key)))
    return paths


def _summary(tasks: tuple[TaskExternalServiceTouchpoints, ...]) -> dict[str, Any]:
    service_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    touchpoint_count = 0
    tasks_with_touchpoints = 0
    for task in tasks:
        if task.touchpoints:
            tasks_with_touchpoints += 1
        touchpoint_count += len(task.touchpoints)
        for touchpoint in task.touchpoints:
            service_counts[touchpoint.service] = service_counts.get(touchpoint.service, 0) + 1
            category_counts[touchpoint.category] = category_counts.get(touchpoint.category, 0) + 1
    return {
        "task_count": len(tasks),
        "tasks_with_touchpoints": tasks_with_touchpoints,
        "touchpoint_count": touchpoint_count,
        "service_counts": dict(sorted(service_counts.items())),
        "category_counts": dict(sorted(category_counts.items())),
    }


def _task_text(record: _TaskRecord) -> str:
    return " ".join(
        [
            record.title,
            record.description,
            *record.acceptance_criteria,
            *record.validation_commands,
            *record.files_or_modules,
            *_strings(record.metadata),
        ]
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child_prefix = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, child_prefix))
            else:
                for text in _strings(child):
                    texts.append((child_prefix, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, child_prefix))
            else:
                for text in _strings(item):
                    texts.append((child_prefix, text))
        return texts
    return [(prefix, text) for text in _strings(value)]


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(key))
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
    return _PATH_STRIP_RE.sub("", value).replace("\\", "/").strip("/")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _dedupe_evidence(
    evidence: Iterable[TaskExternalServiceEvidence],
) -> list[TaskExternalServiceEvidence]:
    deduped: list[TaskExternalServiceEvidence] = []
    seen: set[tuple[str, str]] = set()
    for item in evidence:
        key = (item.source, item.detail)
        if key in seen:
            continue
        deduped.append(item)
        seen.add(key)
    return deduped


__all__ = [
    "ExternalServiceCategory",
    "TaskExternalServiceEvidence",
    "TaskExternalServiceTouchpoint",
    "TaskExternalServiceTouchpointReport",
    "TaskExternalServiceTouchpoints",
    "plan_task_external_service_touchpoints",
    "task_external_service_touchpoints_to_dict",
]
