"""Detect execution tasks that should pass through security review gates."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SecurityTriggerType = Literal[
    "auth",
    "authorization",
    "secrets",
    "pii",
    "payments",
    "webhooks",
    "admin_surface",
    "network_access",
    "dependency_change",
    "data_export",
    "permission_sensitive_file",
]
SecurityReviewSeverity = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_PRIORITY_RANK: dict[SecurityReviewSeverity, int] = {"low": 0, "medium": 1, "high": 2}

_AUTH_RE = re.compile(
    r"\b(?:auth|authentication|login|logout|signin|sign-in|session|sessions|"
    r"oauth|saml|oidc|jwt|token|tokens|mfa|2fa|password|passwords)\b",
    re.IGNORECASE,
)
_AUTHORIZATION_RE = re.compile(
    r"\b(?:authorization|authorize|permission|permissions|rbac|role|roles|"
    r"scope|scopes|policy|policies|acl|access control|entitlement|entitlements)\b",
    re.IGNORECASE,
)
_SECRETS_RE = re.compile(
    r"\b(?:secret|secrets|credential|credentials|api key|apikey|private key|"
    r"token signing|vault|kms|keychain|env var|environment variable)\b",
    re.IGNORECASE,
)
_PII_RE = re.compile(
    r"\b(?:pii|personal data|personally identifiable|email address|phone number|"
    r"ssn|social security|date of birth|dob|address|customer data|user data|"
    r"privacy|gdpr|hipaa)\b",
    re.IGNORECASE,
)
_PAYMENTS_RE = re.compile(
    r"\b(?:payment|payments|billing|invoice|invoices|subscription|subscriptions|"
    r"checkout|stripe|adyen|paypal|card|cards|refund|chargeback|pci)\b",
    re.IGNORECASE,
)
_WEBHOOKS_RE = re.compile(
    r"(?:webhook\w*|callback signature|event signature)",
    re.IGNORECASE,
)
_ADMIN_RE = re.compile(
    r"(?:admin\w*|administrator|superuser|staff portal|internal console|"
    r"moderation|impersonat(?:e|ion)|support console)\b",
    re.IGNORECASE,
)
_NETWORK_RE = re.compile(
    r"\b(?:network|http client|https?://|request|requests|fetch|egress|ingress|"
    r"socket|proxy|cors|dns|tls|ssl|firewall|allowlist|ip address|external api)\b",
    re.IGNORECASE,
)
_DEPENDENCY_RE = re.compile(
    r"\b(?:dependency|dependencies|package|packages|upgrade|upgrades|bump|"
    r"npm install|pip install|poetry add|gem install|cargo add|go get|vendor)\b",
    re.IGNORECASE,
)
_DATA_EXPORT_RE = re.compile(
    r"\b(?:export|exports|download|downloads|csv|jsonl|report|reports|"
    r"bulk extract|data extract|archive|backup)\b",
    re.IGNORECASE,
)

_FILE_SIGNAL_PATTERNS: tuple[tuple[SecurityTriggerType, re.Pattern[str]], ...] = (
    ("permission_sensitive_file", re.compile(r"(?:^|/)\.(?:env|npmrc|pypirc|netrc)$", re.I)),
    (
        "permission_sensitive_file",
        re.compile(
            r"(?:^|/)(?:\.github/workflows|deploy|deployment|infra|ops|charts?|helm|"
            r"k8s|kubernetes|terraform|iam|policies|policy|permissions?)(?:/|$)|"
            r"(?:^|/)Dockerfile$|docker-compose",
            re.IGNORECASE,
        ),
    ),
    (
        "auth",
        re.compile(
            r"(?:^|/)(?:auth|authentication|sessions?|oauth|saml|oidc|tokens?)(?:/|$)|"
            r"(?:auth|session|token|password)\w*\.",
            re.IGNORECASE,
        ),
    ),
    (
        "authorization",
        re.compile(
            r"(?:^|/)(?:authorization|permissions?|rbac|roles?|policies|acl)(?:/|$)|"
            r"(?:permission|policy|role|rbac)\w*\.",
            re.IGNORECASE,
        ),
    ),
    (
        "secrets",
        re.compile(
            r"(?:^|/)(?:secrets?|credentials?|vault|kms)(?:/|$)|"
            r"(?:secret|credential|private_key|apikey|api_key)\w*\.",
            re.IGNORECASE,
        ),
    ),
    ("payments", re.compile(r"(?:^|/)(?:billing|payments?|checkout|stripe|invoices?)(?:/|$)", re.I)),
    ("webhooks", re.compile(r"(?:^|/)(?:webhooks?|callbacks?)(?:/|$)|webhook\w*\.", re.I)),
    ("admin_surface", re.compile(r"(?:^|/)(?:admin|staff|moderation|support)(?:/|$)", re.I)),
    (
        "network_access",
        re.compile(r"(?:^|/)(?:clients?|http|network|integrations?|api)(?:/|$)|(?:client|http)\w*\.", re.I),
    ),
    (
        "dependency_change",
        re.compile(
            r"(?:^|/)(?:package-lock\.json|pnpm-lock\.yaml|yarn\.lock|poetry\.lock|"
            r"requirements(?:-.+)?\.txt|pyproject\.toml|package\.json|Gemfile(?:\.lock)?|"
            r"Cargo\.(?:toml|lock)|go\.(?:mod|sum)|composer\.(?:json|lock))$",
            re.IGNORECASE,
        ),
    ),
    ("data_export", re.compile(r"(?:^|/)(?:exports?|reports?|downloads?|backup|archives?)(?:/|$)", re.I)),
)

_TEXT_SIGNAL_PATTERNS: tuple[
    tuple[SecurityTriggerType, re.Pattern[str], SecurityReviewSeverity],
    ...,
] = (
    ("secrets", _SECRETS_RE, "high"),
    ("authorization", _AUTHORIZATION_RE, "high"),
    ("auth", _AUTH_RE, "high"),
    ("pii", _PII_RE, "high"),
    ("payments", _PAYMENTS_RE, "high"),
    ("webhooks", _WEBHOOKS_RE, "high"),
    ("admin_surface", _ADMIN_RE, "high"),
    ("network_access", _NETWORK_RE, "medium"),
    ("dependency_change", _DEPENDENCY_RE, "medium"),
    ("data_export", _DATA_EXPORT_RE, "medium"),
)

_TYPE_DEFAULT_SEVERITY: dict[SecurityTriggerType, SecurityReviewSeverity] = {
    "auth": "high",
    "authorization": "high",
    "secrets": "high",
    "pii": "high",
    "payments": "high",
    "webhooks": "high",
    "admin_surface": "high",
    "network_access": "medium",
    "dependency_change": "medium",
    "data_export": "medium",
    "permission_sensitive_file": "high",
}
_TYPE_REVIEWERS: dict[SecurityTriggerType, tuple[str, ...]] = {
    "auth": ("security", "identity"),
    "authorization": ("security", "code_owner"),
    "secrets": ("security", "platform"),
    "pii": ("security", "data_privacy"),
    "payments": ("security", "payments"),
    "webhooks": ("security", "integrations"),
    "admin_surface": ("security", "product"),
    "network_access": ("security", "platform"),
    "dependency_change": ("security", "platform"),
    "data_export": ("security", "data_privacy"),
    "permission_sensitive_file": ("security", "code_owner"),
}


@dataclass(frozen=True, slots=True)
class TaskSecurityReviewTrigger:
    """One collapsed security review trigger for an execution task."""

    task_id: str
    trigger_type: SecurityTriggerType
    severity: SecurityReviewSeverity
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_reviewers: tuple[str, ...] = field(default_factory=tuple)
    blocking: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "trigger_type": self.trigger_type,
            "severity": self.severity,
            "evidence": list(self.evidence),
            "recommended_reviewers": list(self.recommended_reviewers),
            "blocking": self.blocking,
        }


@dataclass(frozen=True, slots=True)
class TaskSecurityReviewTriggerResult:
    """Security review triggers detected across a plan or task collection."""

    plan_id: str | None = None
    triggers: tuple[TaskSecurityReviewTrigger, ...] = field(default_factory=tuple)
    trigger_counts_by_type: dict[SecurityTriggerType, int] = field(default_factory=dict)
    blocking_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "triggers": [trigger.to_dict() for trigger in self.triggers],
            "trigger_counts_by_type": dict(self.trigger_counts_by_type),
            "blocking_task_ids": list(self.blocking_task_ids),
        }

    def to_markdown(self) -> str:
        """Render a deterministic markdown summary for reviewer handoff."""
        if not self.triggers:
            return "## Security Review Triggers\n\nNo security review triggers detected."

        lines = ["## Security Review Triggers", ""]
        if self.plan_id:
            lines.extend([f"Plan: `{self.plan_id}`", ""])
        lines.append("| Task | Trigger | Severity | Blocking | Reviewers | Evidence |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for trigger in self.triggers:
            evidence = "<br>".join(_escape_markdown(value) for value in trigger.evidence)
            reviewers = ", ".join(trigger.recommended_reviewers)
            lines.append(
                "| "
                f"`{_escape_markdown(trigger.task_id)}` | "
                f"{trigger.trigger_type} | "
                f"{trigger.severity} | "
                f"{'yes' if trigger.blocking else 'no'} | "
                f"{_escape_markdown(reviewers)} | "
                f"{evidence} |"
            )
        return "\n".join(lines)


def detect_task_security_review_triggers(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskSecurityReviewTriggerResult:
    """Identify execution tasks that should receive explicit security review."""
    plan_id, tasks = _source_payload(source)
    triggers = tuple(
        trigger
        for index, task in enumerate(tasks, start=1)
        for trigger in _task_triggers(task, index)
    )
    return TaskSecurityReviewTriggerResult(
        plan_id=plan_id,
        triggers=triggers,
        trigger_counts_by_type=_trigger_counts(triggers),
        blocking_task_ids=tuple(
            _dedupe(trigger.task_id for trigger in triggers if trigger.blocking)
        ),
    )


def task_security_review_triggers_to_dict(
    result: TaskSecurityReviewTriggerResult,
) -> dict[str, Any]:
    """Serialize security review triggers to a plain dictionary."""
    return result.to_dict()


task_security_review_triggers_to_dict.__test__ = False


def task_security_review_triggers_to_markdown(
    result: TaskSecurityReviewTriggerResult,
) -> str:
    """Render security review triggers as a markdown summary."""
    return result.to_markdown()


task_security_review_triggers_to_markdown.__test__ = False


def _task_triggers(task: Mapping[str, Any], index: int) -> tuple[TaskSecurityReviewTrigger, ...]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    builders: dict[SecurityTriggerType, _TriggerBuilder] = {}

    for path in _strings(task.get("files_or_modules")):
        normalized = _normalized_path(path)
        for trigger_type, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _add(builders, trigger_type, _TYPE_DEFAULT_SEVERITY[trigger_type], f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        _add_text_signals(builders, source_field, text)

    for source_field, text in _test_command_texts(task):
        _add_text_signals(builders, source_field, text)

    for source_field, text in _metadata_texts(task.get("metadata")):
        field = source_field.casefold()
        if "security" in field or "review" in field:
            _add(builders, _metadata_trigger_type(text), "high", f"{source_field}: {text}")
        if "permission" in field or "auth" in field:
            _add(builders, "authorization", "high", f"{source_field}: {text}")
        if "secret" in field or "credential" in field:
            _add(builders, "secrets", "high", f"{source_field}: {text}")
        if "pii" in field or "privacy" in field:
            _add(builders, "pii", "high", f"{source_field}: {text}")
        if "dependency" in field or "package" in field:
            _add(builders, "dependency_change", "medium", f"{source_field}: {text}")
        _add_text_signals(builders, source_field, text)

    if _risk_level(task.get("risk_level")) == "high":
        for builder in builders.values():
            builder.blocking = True

    return tuple(
        TaskSecurityReviewTrigger(
            task_id=task_id,
            trigger_type=trigger_type,
            severity=builder.severity,
            evidence=tuple(_dedupe(builder.evidence)),
            recommended_reviewers=_TYPE_REVIEWERS[trigger_type],
            blocking=builder.blocking or builder.severity == "high",
        )
        for trigger_type, builder in sorted(builders.items(), key=_trigger_sort_key)
    )


def _add_text_signals(
    builders: dict[SecurityTriggerType, "_TriggerBuilder"],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    for trigger_type, pattern, severity in _TEXT_SIGNAL_PATTERNS:
        if pattern.search(text):
            _add(builders, trigger_type, severity, evidence)


@dataclass(slots=True)
class _TriggerBuilder:
    severity: SecurityReviewSeverity
    evidence: list[str] = field(default_factory=list)
    blocking: bool = False


def _add(
    builders: dict[SecurityTriggerType, _TriggerBuilder],
    trigger_type: SecurityTriggerType,
    severity: SecurityReviewSeverity,
    evidence: str,
) -> None:
    if trigger_type not in builders:
        builders[trigger_type] = _TriggerBuilder(severity=severity)
    elif _PRIORITY_RANK[severity] > _PRIORITY_RANK[builders[trigger_type].severity]:
        builders[trigger_type].severity = severity
    builders[trigger_type].evidence.append(evidence)
    if severity == "high":
        builders[trigger_type].blocking = True


def _trigger_sort_key(item: tuple[SecurityTriggerType, _TriggerBuilder]) -> tuple[int, str]:
    trigger_type, builder = item
    return (-_PRIORITY_RANK[builder.severity], trigger_type)


def _trigger_counts(
    triggers: Iterable[TaskSecurityReviewTrigger],
) -> dict[SecurityTriggerType, int]:
    counts: dict[SecurityTriggerType, int] = {}
    for trigger in triggers:
        counts[trigger.trigger_type] = counts.get(trigger.trigger_type, 0) + 1
    return {trigger_type: counts[trigger_type] for trigger_type in sorted(counts)}


def _metadata_trigger_type(text: str) -> SecurityTriggerType:
    normalized = text.casefold().replace("_", " ")
    for trigger_type in (
        "secrets",
        "authorization",
        "auth",
        "pii",
        "payments",
        "webhooks",
        "admin_surface",
        "network_access",
        "dependency_change",
        "data_export",
        "permission_sensitive_file",
    ):
        if trigger_type.replace("_", " ") in normalized:
            return trigger_type
    return "permission_sensitive_file"


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
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

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _test_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            texts.append((key, command))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            for index, command in enumerate(_commands_from_value(metadata.get(key))):
                texts.append((f"metadata.{key}[{index}]", command))
    return texts


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    if isinstance(value, (list, tuple, set)):
        return _strings(value)
    command = _optional_text(value)
    return [command] if command else []


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif (text := _optional_text(child)):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif (text := _optional_text(item)):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _risk_level(value: Any) -> SecurityReviewSeverity:
    text = (_optional_text(value) or "").casefold()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _escape_markdown(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SecurityReviewSeverity",
    "SecurityTriggerType",
    "TaskSecurityReviewTrigger",
    "TaskSecurityReviewTriggerResult",
    "detect_task_security_review_triggers",
    "task_security_review_triggers_to_dict",
    "task_security_review_triggers_to_markdown",
]
