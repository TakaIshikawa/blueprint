"""Plan outbound email deliverability readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


EmailFlowSignal = Literal[
    "outbound_email",
    "notification_email",
    "invite_email",
    "password_reset_email",
    "digest_email",
    "transactional_email",
    "email_template",
    "email_provider",
]
EmailDeliverabilitySafeguard = Literal[
    "spf",
    "dkim",
    "dmarc",
    "unsubscribe_preferences",
    "bounce_complaint_handling",
    "rate_limiting",
    "template_rendering_tests",
    "localization_accessibility",
    "fallback_support_visibility",
]
EmailDeliverabilityReadinessLevel = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[EmailFlowSignal, ...] = (
    "outbound_email",
    "notification_email",
    "invite_email",
    "password_reset_email",
    "digest_email",
    "transactional_email",
    "email_template",
    "email_provider",
)
_SIGNAL_RANK: dict[EmailFlowSignal, int] = {signal: index for index, signal in enumerate(_SIGNAL_ORDER)}
_SAFEGUARD_ORDER: tuple[EmailDeliverabilitySafeguard, ...] = (
    "spf",
    "dkim",
    "dmarc",
    "unsubscribe_preferences",
    "bounce_complaint_handling",
    "rate_limiting",
    "template_rendering_tests",
    "localization_accessibility",
    "fallback_support_visibility",
)
_READINESS_ORDER: dict[EmailDeliverabilityReadinessLevel, int] = {
    "weak": 0,
    "partial": 1,
    "strong": 2,
}
_OPTIONAL_UNSUBSCRIBE_SIGNALS = {"invite_email", "password_reset_email"}
_GENERIC_SIGNALS = {"outbound_email", "email_template", "email_provider"}
_REQUIRED_AUTHENTICATION: set[EmailDeliverabilitySafeguard] = {"spf", "dkim", "dmarc"}

_TEXT_SIGNAL_PATTERNS: dict[EmailFlowSignal, re.Pattern[str]] = {
    "outbound_email": re.compile(
        r"\b(?:outbound email|send emails?|email sending|mail delivery|smtp|mailer|sendgrid|mailgun|ses|postmark)\b",
        re.I,
    ),
    "notification_email": re.compile(r"\b(?:notification emails?|email notifications?|notify users? by email)\b", re.I),
    "invite_email": re.compile(r"\b(?:invite emails?|invitation emails?|team invites?|workspace invites?|magic link)\b", re.I),
    "password_reset_email": re.compile(
        r"\b(?:password reset emails?|reset password emails?|forgot password|account recovery emails?)\b",
        re.I,
    ),
    "digest_email": re.compile(r"\b(?:digest emails?|daily digest|weekly digest|summary emails?|newsletter)\b", re.I),
    "transactional_email": re.compile(
        r"\b(?:transactional emails?|receipt emails?|invoice emails?|order confirmation|account emails?|system emails?)\b",
        re.I,
    ),
    "email_template": re.compile(
        r"\b(?:email templates?|template rendering|mjml|handlebars email|liquid templates?|html email|text email)\b",
        re.I,
    ),
    "email_provider": re.compile(r"\b(?:sendgrid|mailgun|postmark|aws ses|ses|smtp provider|email provider|esp)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[EmailDeliverabilitySafeguard, re.Pattern[str]] = {
    "spf": re.compile(r"\b(?:spf|sender policy framework|txt record)\b", re.I),
    "dkim": re.compile(r"\b(?:dkim|domainkeys|domain key|selector)\b", re.I),
    "dmarc": re.compile(r"\b(?:dmarc|domain-based message authentication|p=reject|p=quarantine|rua=)\b", re.I),
    "unsubscribe_preferences": re.compile(
        r"\b(?:unsubscribe|list-unsubscribe|preference center|email preferences?|opt out|opt-out|mail preferences?)\b",
        re.I,
    ),
    "bounce_complaint_handling": re.compile(
        r"\b(?:bounce handling|bounces?|complaint handling|complaints?|suppression list|webhook|feedback loop|fbl)\b",
        re.I,
    ),
    "rate_limiting": re.compile(r"\b(?:rate limit|rate limiting|throttle|throttling|send quota|batch size|backoff)\b", re.I),
    "template_rendering_tests": re.compile(
        r"\b(?:template rendering tests?|render tests?|snapshot tests?|preview tests?|multipart|plain text fallback|email client tests?)\b",
        re.I,
    ),
    "localization_accessibility": re.compile(
        r"\b(?:locali[sz]ation|i18n|locale|translated?|accessibility|a11y|screen reader|alt text|contrast)\b",
        re.I,
    ),
    "fallback_support_visibility": re.compile(
        r"\b(?:delivery fallback|manual fallback|manual resend|resend|support visibility|support dashboard|support runbook|support handoff|delivery logs?|message logs?)\b",
        re.I,
    ),
}
_SAFEGUARD_RECOMMENDED_CHECKS: dict[EmailDeliverabilitySafeguard, str] = {
    "spf": "Verify SPF records include the sending provider and pass for the planned From domain.",
    "dkim": "Confirm DKIM keys, selectors, and signing are configured in each sending environment.",
    "dmarc": "Check DMARC policy alignment, aggregate reporting, and launch-safe policy progression.",
    "unsubscribe_preferences": "Add unsubscribe or preference-center handling for non-essential email and test opt-out behavior.",
    "bounce_complaint_handling": "Wire bounce and complaint webhooks into suppression handling and operational alerts.",
    "rate_limiting": "Define provider quotas, send throttles, retry backoff, and bulk-send limits before rollout.",
    "template_rendering_tests": "Test template rendering, variables, multipart bodies, links, and provider payloads.",
    "localization_accessibility": "Review email content for localization coverage, accessible markup, alt text, and readable contrast.",
    "fallback_support_visibility": "Document fallback/resend behavior and expose delivery state or logs to support.",
}


@dataclass(frozen=True, slots=True)
class TaskEmailDeliverabilityReadinessRecord:
    """Readiness guidance for one task that changes email delivery."""

    task_id: str
    title: str
    readiness_level: EmailDeliverabilityReadinessLevel
    detected_signals: tuple[EmailFlowSignal, ...]
    present_safeguards: tuple[EmailDeliverabilitySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[EmailDeliverabilitySafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness_level": self.readiness_level,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskEmailDeliverabilityReadinessPlan:
    """Plan-level email deliverability readiness review."""

    plan_id: str | None = None
    records: tuple[TaskEmailDeliverabilityReadinessRecord, ...] = field(default_factory=tuple)
    email_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskEmailDeliverabilityReadinessRecord, ...]:
        """Compatibility view for analyzers that expose guidance as recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "email_task_ids": list(self.email_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the readiness plan as deterministic Markdown."""
        title = "# Task Email Deliverability Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Email task count: {self.summary.get('email_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No outbound or transactional email tasks were detected."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                "| Task | Title | Readiness | Signals | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell('; '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_email_deliverability_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskEmailDeliverabilityReadinessPlan:
    """Build readiness records for outbound and transactional email tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
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
    return TaskEmailDeliverabilityReadinessPlan(
        plan_id=plan_id,
        records=records,
        email_task_ids=tuple(record.task_id for record in records),
        no_signal_task_ids=no_signal_task_ids,
        summary=_summary(records, task_count=len(tasks), no_signal_task_count=len(no_signal_task_ids)),
    )


def analyze_task_email_deliverability_readiness(source: Any) -> TaskEmailDeliverabilityReadinessPlan:
    """Compatibility alias for building email deliverability readiness plans."""
    return build_task_email_deliverability_readiness_plan(source)


def recommend_task_email_deliverability_readiness(source: Any) -> TaskEmailDeliverabilityReadinessPlan:
    """Compatibility alias for recommending email deliverability checks."""
    return build_task_email_deliverability_readiness_plan(source)


def generate_task_email_deliverability_readiness(source: Any) -> TaskEmailDeliverabilityReadinessPlan:
    """Compatibility alias for generating email deliverability readiness plans."""
    return build_task_email_deliverability_readiness_plan(source)


def summarize_task_email_deliverability_readiness(source: Any) -> TaskEmailDeliverabilityReadinessPlan:
    """Compatibility alias for summarizing email deliverability readiness plans."""
    return build_task_email_deliverability_readiness_plan(source)


def extract_task_email_deliverability_readiness(source: Any) -> TaskEmailDeliverabilityReadinessPlan:
    """Compatibility alias for extracting email deliverability readiness plans."""
    return build_task_email_deliverability_readiness_plan(source)


def task_email_deliverability_readiness_plan_to_dict(
    result: TaskEmailDeliverabilityReadinessPlan,
) -> dict[str, Any]:
    """Serialize an email deliverability readiness plan to a plain dictionary."""
    return result.to_dict()


task_email_deliverability_readiness_plan_to_dict.__test__ = False


def task_email_deliverability_readiness_plan_to_dicts(
    result: TaskEmailDeliverabilityReadinessPlan | Iterable[TaskEmailDeliverabilityReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize email deliverability readiness records to plain dictionaries."""
    if isinstance(result, TaskEmailDeliverabilityReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_email_deliverability_readiness_plan_to_dicts.__test__ = False


def task_email_deliverability_readiness_plan_to_markdown(
    result: TaskEmailDeliverabilityReadinessPlan,
) -> str:
    """Render an email deliverability readiness plan as Markdown."""
    return result.to_markdown()


task_email_deliverability_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[EmailFlowSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[EmailDeliverabilitySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskEmailDeliverabilityReadinessRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.signals:
        return None

    missing_safeguards = _missing_safeguards(signals.signals, signals.present_safeguards)
    readiness = _readiness_level(signals.present_safeguards, missing_safeguards)
    return TaskEmailDeliverabilityReadinessRecord(
        task_id=task_id,
        title=title,
        readiness_level=readiness,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommended_checks=tuple(_SAFEGUARD_RECOMMENDED_CHECKS[safeguard] for safeguard in missing_safeguards),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[EmailFlowSignal] = set()
    safeguard_hits: set[EmailDeliverabilitySafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        signal_added = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                signal_added = True
        if signal_added:
            signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[EmailFlowSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[EmailFlowSignal] = set()
    if {"email", "emails", "mail", "mailer", "smtp", "notifications", "digests", "invites"} & parts:
        signals.add("outbound_email")
    if any(token in text for token in ("notification email", "email notification", "notifications email")):
        signals.add("notification_email")
    if any(token in text for token in ("invite email", "invitation email", "team invite", "invites")):
        signals.add("invite_email")
    if any(token in text for token in ("password reset", "reset password", "forgot password")):
        signals.add("password_reset_email")
    if "digest" in text or "newsletter" in text:
        signals.add("digest_email")
    if any(token in text for token in ("transactional", "receipt", "invoice email", "order confirmation")):
        signals.add("transactional_email")
    if any(token in text for token in ("template", "templates", "mjml", "handlebars", "liquid")):
        signals.add("email_template")
    if any(token in text for token in ("sendgrid", "mailgun", "postmark", "aws ses", "ses", "smtp")):
        signals.add("email_provider")
    if name in {"mailer.py", "mailers.py", "email.py", "emails.py", "notifications.py"}:
        signals.add("outbound_email")
    return signals


def _missing_safeguards(
    signals: tuple[EmailFlowSignal, ...],
    present_safeguards: tuple[EmailDeliverabilitySafeguard, ...],
) -> tuple[EmailDeliverabilitySafeguard, ...]:
    required = set(_SAFEGUARD_ORDER)
    specific_signals = set(signals) - _GENERIC_SIGNALS
    if specific_signals & _OPTIONAL_UNSUBSCRIBE_SIGNALS and not specific_signals & {
        "digest_email",
        "transactional_email",
    }:
        required.remove("unsubscribe_preferences")
    present = set(present_safeguards)
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _readiness_level(
    present_safeguards: tuple[EmailDeliverabilitySafeguard, ...],
    missing_safeguards: tuple[EmailDeliverabilitySafeguard, ...],
) -> EmailDeliverabilityReadinessLevel:
    present = set(present_safeguards)
    if not missing_safeguards and _REQUIRED_AUTHENTICATION <= present:
        return "strong"
    if len(present) >= 3 or len(missing_safeguards) <= 3:
        return "partial"
    return "weak"


def _summary(
    records: tuple[TaskEmailDeliverabilityReadinessRecord, ...],
    *,
    task_count: int,
    no_signal_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "email_task_count": len(records),
        "no_signal_task_count": no_signal_task_count,
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
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
        "validation_command",
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
        "test_commands",
        "validation_commands",
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
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "EmailDeliverabilityReadinessLevel",
    "EmailDeliverabilitySafeguard",
    "EmailFlowSignal",
    "TaskEmailDeliverabilityReadinessPlan",
    "TaskEmailDeliverabilityReadinessRecord",
    "analyze_task_email_deliverability_readiness",
    "build_task_email_deliverability_readiness_plan",
    "extract_task_email_deliverability_readiness",
    "generate_task_email_deliverability_readiness",
    "recommend_task_email_deliverability_readiness",
    "summarize_task_email_deliverability_readiness",
    "task_email_deliverability_readiness_plan_to_dict",
    "task_email_deliverability_readiness_plan_to_dicts",
    "task_email_deliverability_readiness_plan_to_markdown",
]
