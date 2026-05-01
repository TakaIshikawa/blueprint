"""Plan notification deliverability readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


NotificationChannel = Literal["email", "sms", "push", "webhook", "in_app"]
NotificationDeliverabilityCategory = Literal[
    "email_delivery",
    "sms_delivery",
    "push_delivery",
    "webhook_delivery",
    "in_app_delivery",
]
NotificationDeliverabilitySeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CHANNEL_ORDER: dict[NotificationChannel, int] = {
    "email": 0,
    "sms": 1,
    "push": 2,
    "webhook": 3,
    "in_app": 4,
}
_CATEGORY_BY_CHANNEL: dict[NotificationChannel, NotificationDeliverabilityCategory] = {
    "email": "email_delivery",
    "sms": "sms_delivery",
    "push": "push_delivery",
    "webhook": "webhook_delivery",
    "in_app": "in_app_delivery",
}
_SEVERITY_ORDER: dict[NotificationDeliverabilitySeverity, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_CHANNEL_PATTERNS: dict[NotificationChannel, re.Pattern[str]] = {
    "email": re.compile(
        r"\b(?:email|e-mail|mailgun|sendgrid|postmark|ses|smtp|transactional mail|"
        r"newsletter|receipt|template|bounce|spf|dkim|dmarc)\b",
        re.I,
    ),
    "sms": re.compile(r"\b(?:sms|text message|twilio|nexmo|vonage|short code|long code|toll[- ]?free|10dlc)\b", re.I),
    "push": re.compile(
        r"\b(?:push notification|push notifications|apns|fcm|firebase cloud messaging|device token|"
        r"mobile notification|web push)\b",
        re.I,
    ),
    "webhook": re.compile(r"\b(?:webhook|webhooks|callback url|callback endpoint|signing secret|hmac|event delivery)\b", re.I),
    "in_app": re.compile(
        r"\b(?:in[- ]?app notification|in[- ]?product notification|notification center|"
        r"notification inbox|toast|banner notification|activity feed)\b",
        re.I,
    ),
}
_PROVIDER_PATTERNS: dict[str, re.Pattern[str]] = {
    "Amazon SES": re.compile(r"\b(?:amazon ses|aws ses|ses)\b", re.I),
    "APNs": re.compile(r"\b(?:apns|apple push notification)\b", re.I),
    "FCM": re.compile(r"\b(?:fcm|firebase cloud messaging|firebase)\b", re.I),
    "Mailgun": re.compile(r"\bmailgun\b", re.I),
    "Postmark": re.compile(r"\bpostmark\b", re.I),
    "SendGrid": re.compile(r"\bsendgrid\b", re.I),
    "Twilio": re.compile(r"\btwilio\b", re.I),
    "Vonage": re.compile(r"\b(?:vonage|nexmo)\b", re.I),
}
_CAPABILITY_PATTERNS: dict[str, re.Pattern[str]] = {
    "retry_strategy": re.compile(
        r"\b(?:retry|retries|backoff|exponential backoff|dead[- ]?letter|dlq|idempotenc|redeliver|replay)\b",
        re.I,
    ),
    "unsubscribe": re.compile(r"\b(?:unsubscribe|opt[- ]?out|stop keyword|preferences|suppression|consent)\b", re.I),
    "monitoring": re.compile(
        r"\b(?:monitor|alert|dashboard|metric|slo|bounce|complaint|delivery rate|open rate|click rate|"
        r"failure rate|dead[- ]?letter|dlq|receipt)\b",
        re.I,
    ),
    "rate_limit": re.compile(r"\b(?:rate limit|rate limiting|throttle|quota|send rate|burst|batch)\b", re.I),
    "template": re.compile(r"\b(?:template|copy review|content review|subject line|locali[sz]ation|rendering)\b", re.I),
    "sandbox": re.compile(r"\b(?:sandbox|test credential|test provider|provider validation|staging provider)\b", re.I),
    "customer_comms": re.compile(r"\b(?:customer communication|support script|status page|customer notice|launch comms)\b", re.I),
}
_EXPLICIT_METADATA_KEYS: dict[str, re.Pattern[str]] = {
    "channels": re.compile(r"\bchannels?\b", re.I),
    "providers": re.compile(r"\bproviders?\b", re.I),
    "retry_strategy": re.compile(r"\b(?:retry strategy|retries|retry|backoff|dlq|dead letter)\b", re.I),
    "unsubscribe_requirements": re.compile(r"\b(?:unsubscribe|opt out|suppression|consent)\b", re.I),
    "monitoring_signals": re.compile(r"\b(?:monitoring|signals|metrics|alerts|dashboard)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskNotificationDeliverabilityRecord:
    """Notification deliverability guidance for one execution task."""

    task_id: str
    title: str
    severity: NotificationDeliverabilitySeverity
    channels: tuple[NotificationChannel, ...]
    deliverability_categories: tuple[NotificationDeliverabilityCategory, ...]
    providers: tuple[str, ...]
    retry_strategy: tuple[str, ...]
    unsubscribe_requirements: tuple[str, ...]
    monitoring_signals: tuple[str, ...]
    recommended_artifacts: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "channels": list(self.channels),
            "deliverability_categories": list(self.deliverability_categories),
            "providers": list(self.providers),
            "retry_strategy": list(self.retry_strategy),
            "unsubscribe_requirements": list(self.unsubscribe_requirements),
            "monitoring_signals": list(self.monitoring_signals),
            "recommended_artifacts": list(self.recommended_artifacts),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskNotificationDeliverabilityPlan:
    """Plan-level notification deliverability review."""

    plan_id: str | None = None
    records: tuple[TaskNotificationDeliverabilityRecord, ...] = field(default_factory=tuple)
    notification_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "notification_task_ids": list(self.notification_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return notification deliverability records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[TaskNotificationDeliverabilityRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the notification deliverability plan as deterministic Markdown."""
        title = "# Task Notification Deliverability Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        channel_counts = self.summary.get("channel_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('notification_task_count', 0)} notification tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no signal: {self.summary.get('no_signal_task_count', 0)})."
            ),
            (
                "Channels: "
                f"email {channel_counts.get('email', 0)}, "
                f"sms {channel_counts.get('sms', 0)}, "
                f"push {channel_counts.get('push', 0)}, "
                f"webhook {channel_counts.get('webhook', 0)}, "
                f"in_app {channel_counts.get('in_app', 0)}."
            ),
            (
                "Severity: "
                f"high {severity_counts.get('high', 0)}, "
                f"medium {severity_counts.get('medium', 0)}, "
                f"low {severity_counts.get('low', 0)}."
            ),
        ]
        if not self.records:
            lines.extend(["", "No notification deliverability records were inferred."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Channels | Categories | Providers | Retry / DLQ | Unsubscribe | Monitoring | Recommended Artifacts | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.channels))} | "
                f"{_markdown_cell(', '.join(record.deliverability_categories))} | "
                f"{_markdown_cell('; '.join(record.providers) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.retry_strategy) or 'missing')} | "
                f"{_markdown_cell('; '.join(record.unsubscribe_requirements) or 'missing')} | "
                f"{_markdown_cell('; '.join(record.monitoring_signals) or 'missing')} | "
                f"{_markdown_cell('; '.join(record.recommended_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_notification_deliverability_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskNotificationDeliverabilityPlan:
    """Build notification deliverability guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                min(_CHANNEL_ORDER[channel] for channel in record.channels),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    channel_counts = {
        channel: sum(1 for record in records if channel in record.channels)
        for channel in _CHANNEL_ORDER
    }
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    return TaskNotificationDeliverabilityPlan(
        plan_id=plan_id,
        records=records,
        notification_task_ids=tuple(record.task_id for record in records),
        no_signal_task_ids=no_signal_task_ids,
        summary={
            "task_count": len(tasks),
            "notification_task_count": len(records),
            "no_signal_task_count": len(no_signal_task_ids),
            "channel_counts": channel_counts,
            "severity_counts": severity_counts,
        },
    )


def derive_task_notification_deliverability_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskNotificationDeliverabilityPlan:
    """Compatibility alias for building notification deliverability plans."""
    return build_task_notification_deliverability_plan(source)


def summarize_task_notification_deliverability(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskNotificationDeliverabilityPlan:
    """Compatibility alias matching other task-level planners."""
    return build_task_notification_deliverability_plan(source)


def task_notification_deliverability_plan_to_dict(
    result: TaskNotificationDeliverabilityPlan,
) -> dict[str, Any]:
    """Serialize a notification deliverability plan to a plain dictionary."""
    return result.to_dict()


task_notification_deliverability_plan_to_dict.__test__ = False


def task_notification_deliverability_plan_to_markdown(
    result: TaskNotificationDeliverabilityPlan,
) -> str:
    """Render a notification deliverability plan as Markdown."""
    return result.to_markdown()


task_notification_deliverability_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    channels: tuple[NotificationChannel, ...] = field(default_factory=tuple)
    providers: tuple[str, ...] = field(default_factory=tuple)
    retry_strategy: tuple[str, ...] = field(default_factory=tuple)
    unsubscribe_requirements: tuple[str, ...] = field(default_factory=tuple)
    monitoring_signals: tuple[str, ...] = field(default_factory=tuple)
    capabilities: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicit_metadata: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskNotificationDeliverabilityRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.channels:
        return None
    return TaskNotificationDeliverabilityRecord(
        task_id=task_id,
        title=title,
        severity=_severity(signals),
        channels=signals.channels,
        deliverability_categories=tuple(_CATEGORY_BY_CHANNEL[channel] for channel in signals.channels),
        providers=signals.providers,
        retry_strategy=signals.retry_strategy,
        unsubscribe_requirements=signals.unsubscribe_requirements,
        monitoring_signals=signals.monitoring_signals,
        recommended_artifacts=_recommended_artifacts(signals),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    channels: set[NotificationChannel] = set()
    providers: list[str] = []
    retry_strategy: list[str] = []
    unsubscribe_requirements: list[str] = []
    monitoring_signals: list[str] = []
    capabilities: set[str] = set()
    evidence: list[str] = []
    explicit_metadata: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_channels, path_capabilities = _path_signals(normalized)
        if path_channels or path_capabilities:
            channels.update(path_channels)
            capabilities.update(path_capabilities)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for channel, pattern in _CHANNEL_PATTERNS.items():
            if pattern.search(text):
                channels.add(channel)
                matched = True
        matched_providers = _providers_from_text(text)
        if matched_providers:
            providers.extend(matched_providers)
            matched = True
        if _CAPABILITY_PATTERNS["retry_strategy"].search(text):
            retry_strategy.append(snippet)
            capabilities.add("retry_strategy")
            matched = True
        if _CAPABILITY_PATTERNS["unsubscribe"].search(text):
            unsubscribe_requirements.append(snippet)
            capabilities.add("unsubscribe")
            matched = True
        if _CAPABILITY_PATTERNS["monitoring"].search(text):
            monitoring_signals.append(snippet)
            capabilities.add("monitoring")
            matched = True
        for capability in ("rate_limit", "template", "sandbox", "customer_comms"):
            if _CAPABILITY_PATTERNS[capability].search(text):
                capabilities.add(capability)
                matched = True
        if source_field.startswith("metadata.") and _explicit_metadata_key(source_field):
            explicit_metadata.append(source_field)
            matched = True
        if matched:
            evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for channel, pattern in _CHANNEL_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                channels.add(channel)
                matched = True
        matched_providers = _providers_from_text(command)
        if matched_providers:
            providers.extend(matched_providers)
            matched = True
        if any(pattern.search(command) or pattern.search(command_text) for pattern in _CAPABILITY_PATTERNS.values()):
            capabilities.add("sandbox")
            matched = True
        if matched:
            evidence.append(snippet)

    return _Signals(
        channels=tuple(channel for channel in _CHANNEL_ORDER if channel in channels),
        providers=tuple(_dedupe(providers)),
        retry_strategy=tuple(_dedupe(retry_strategy)),
        unsubscribe_requirements=tuple(_dedupe(unsubscribe_requirements)),
        monitoring_signals=tuple(_dedupe(monitoring_signals)),
        capabilities=tuple(sorted(capabilities)),
        evidence=tuple(_dedupe(evidence)),
        explicit_metadata=tuple(_dedupe(explicit_metadata)),
    )


def _path_signals(path: str) -> tuple[set[NotificationChannel], set[str]]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    channels: set[NotificationChannel] = set()
    capabilities: set[str] = set()
    if bool({"email", "emails", "mail", "mailer", "templates"} & parts) or any(
        token in text for token in ("sendgrid", "mailgun", "postmark", "smtp", "ses")
    ):
        channels.add("email")
    if bool({"sms", "texts", "texting"} & parts) or any(token in text for token in ("twilio", "vonage", "10dlc")):
        channels.add("sms")
    if bool({"push", "notifications"} & parts) and any(token in text for token in ("push", "apns", "fcm", "device")):
        channels.add("push")
    if "webhook" in text or "callback" in text:
        channels.add("webhook")
    if any(token in text for token in ("in app", "inapp", "notification center", "notification inbox", "toast")):
        channels.add("in_app")
    if any(token in name for token in ("retry", "backoff", "dlq", "dead_letter")):
        capabilities.add("retry_strategy")
    if any(token in text for token in ("unsubscribe", "opt out", "suppression")):
        capabilities.add("unsubscribe")
    if any(token in text for token in ("monitor", "metrics", "dashboard", "bounce", "complaint", "receipt")):
        capabilities.add("monitoring")
    if any(token in text for token in ("rate limit", "ratelimit", "throttle", "quota")):
        capabilities.add("rate_limit")
    if any(token in text for token in ("template", "templates")):
        capabilities.add("template")
    return channels, capabilities


def _severity(signals: _Signals) -> NotificationDeliverabilitySeverity:
    channel_set = set(signals.channels)
    capability_set = set(signals.capabilities)
    external_channels = channel_set - {"in_app"}
    if {"email", "sms"} & channel_set and not signals.unsubscribe_requirements:
        return "high"
    if external_channels and not signals.retry_strategy:
        return "high"
    if "webhook" in channel_set and not {"monitoring", "retry_strategy"} <= capability_set:
        return "high"
    if not signals.monitoring_signals:
        return "medium"
    if external_channels and not signals.providers:
        return "medium"
    if not {"rate_limit", "sandbox"} & capability_set and external_channels:
        return "medium"
    return "low"


def _recommended_artifacts(signals: _Signals) -> tuple[str, ...]:
    channel_set = set(signals.channels)
    capability_set = set(signals.capabilities)
    artifacts: list[str] = []
    if "email" in channel_set or "in_app" in channel_set or "push" in channel_set:
        artifacts.append("Template review for copy, localization, rendering, and sensitive content.")
    if {"email", "sms"} & channel_set:
        artifacts.append("Unsubscribe, opt-out, consent, and suppression-list handling.")
    if channel_set - {"in_app"}:
        artifacts.append("Rate limiting, quota, and provider-throttle behavior.")
        artifacts.append("Provider sandbox validation with test credentials and representative payloads.")
    if channel_set & {"email", "sms", "push", "webhook"}:
        artifacts.append("Retry, idempotency, and DLQ handling for transient delivery failures.")
    if "email" in channel_set:
        artifacts.append("Bounce, complaint, and suppression tracking.")
    if "webhook" in channel_set:
        artifacts.append("Webhook signing, replay protection, retry schedule, and delivery receipts.")
    if "push" in channel_set:
        artifacts.append("Device-token lifecycle, opt-in state, and provider feedback handling.")
    if "in_app" in channel_set:
        artifacts.append("Notification center read/unread state, retention, and customer visibility checks.")
    artifacts.append("Customer communication checks for support readiness, launch notes, and incident messaging.")

    if "template" in capability_set:
        artifacts.append("Confirm template review sign-off is linked before implementation starts.")
    if "monitoring" not in capability_set:
        artifacts.append("Define monitoring signals for delivery, failure, retry, and provider health.")
    return tuple(_dedupe(artifacts))


def _providers_from_text(text: str) -> list[str]:
    return [provider for provider, pattern in _PROVIDER_PATTERNS.items() if pattern.search(text)]


def _explicit_metadata_key(source_field: str) -> bool:
    key = source_field.rsplit(".", 1)[-1].replace("_", " ")
    return any(pattern.search(key) for pattern in _EXPLICIT_METADATA_KEYS.values())


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
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
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


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (*_CHANNEL_PATTERNS.values(), *_PROVIDER_PATTERNS.values(), *_CAPABILITY_PATTERNS.values())
    ) or _explicit_metadata_key(text)


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
    "NotificationChannel",
    "NotificationDeliverabilityCategory",
    "NotificationDeliverabilitySeverity",
    "TaskNotificationDeliverabilityPlan",
    "TaskNotificationDeliverabilityRecord",
    "build_task_notification_deliverability_plan",
    "derive_task_notification_deliverability_plan",
    "summarize_task_notification_deliverability",
    "task_notification_deliverability_plan_to_dict",
    "task_notification_deliverability_plan_to_markdown",
]
