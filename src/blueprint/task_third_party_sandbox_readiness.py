"""Plan sandbox readiness for tasks integrating third-party services."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ThirdPartySandboxNeed = Literal[
    "test_account_availability",
    "fixture_test_data_setup",
    "credential_isolation",
    "webhook_endpoint_setup",
    "rate_limit_safe_validation",
    "fallback_manual_test_steps",
]
ThirdPartySandboxRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[ThirdPartySandboxRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SANDBOX_NEED_ORDER: tuple[ThirdPartySandboxNeed, ...] = (
    "test_account_availability",
    "fixture_test_data_setup",
    "credential_isolation",
    "webhook_endpoint_setup",
    "rate_limit_safe_validation",
    "fallback_manual_test_steps",
)
_VENDOR_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Adyen", re.compile(r"\badyen\b", re.I)),
    ("Anthropic", re.compile(r"\banthropic\b", re.I)),
    ("AWS", re.compile(r"\b(?:aws|amazon\s+(?:ses|s3|sns|sqs|cognito|connect))\b", re.I)),
    ("Braintree", re.compile(r"\bbraintree\b", re.I)),
    ("Discord", re.compile(r"\bdiscord\b", re.I)),
    ("GitHub", re.compile(r"\bgithub\b", re.I)),
    ("Google", re.compile(r"\b(?:google|gmail|google\s+calendar|google\s+drive|gcp)\b", re.I)),
    ("HubSpot", re.compile(r"\bhubspot\b", re.I)),
    ("Intercom", re.compile(r"\bintercom\b", re.I)),
    ("Mailgun", re.compile(r"\bmailgun\b", re.I)),
    ("Microsoft", re.compile(r"\b(?:microsoft|azure|teams|outlook|office\s*365)\b", re.I)),
    ("OpenAI", re.compile(r"\bopenai\b", re.I)),
    ("PagerDuty", re.compile(r"\bpagerduty\b", re.I)),
    ("PayPal", re.compile(r"\bpaypal\b", re.I)),
    ("Plaid", re.compile(r"\bplaid\b", re.I)),
    ("Postmark", re.compile(r"\bpostmark\b", re.I)),
    ("Salesforce", re.compile(r"\bsalesforce\b", re.I)),
    ("SendGrid", re.compile(r"\bsendgrid\b", re.I)),
    ("Shopify", re.compile(r"\bshopify\b", re.I)),
    ("Slack", re.compile(r"\bslack\b", re.I)),
    ("Square", re.compile(r"\bsquare\b", re.I)),
    ("Stripe", re.compile(r"\bstripe\b", re.I)),
    ("Twilio", re.compile(r"\btwilio\b", re.I)),
    ("Zendesk", re.compile(r"\bzendesk\b", re.I)),
)
_GENERIC_SERVICE_RE = re.compile(
    r"\b(?:third[- ]party|external service|external api|vendor api|partner api|"
    r"payment provider|payment gateway|messaging provider|email provider|sms provider|"
    r"crm|oauth(?:2)? app|oauth integration|sdk|webhook(?:s)?|provider client|"
    r"integration client)\b",
    re.I,
)
_PATH_SERVICE_RE = re.compile(
    r"(?:^|/)(?:integrations?|clients?|providers?)(?:/|$)|"
    r"(?:^|/)(?:webhooks?|oauth|sdk)(?:/|$)|"
    r"(?:stripe|twilio|sendgrid|salesforce|hubspot|slack|paypal|adyen|zendesk|shopify|plaid)",
    re.I,
)
_WEBHOOK_RE = re.compile(r"\bwebhook(?:s)?\b", re.I)
_PAYMENT_OR_OAUTH_RE = re.compile(
    r"\b(?:payment|billing|checkout|oauth|token|secret|credential)\b", re.I
)
_NEED_PATTERNS: dict[ThirdPartySandboxNeed, re.Pattern[str]] = {
    "test_account_availability": re.compile(
        r"\b(?:test account|sandbox account|developer account|vendor account|test tenant|test workspace|"
        r"test organization|merchant sandbox|sandbox user)\b",
        re.I,
    ),
    "fixture_test_data_setup": re.compile(
        r"\b(?:fixtures?|test data|sample data|seed data|mock payload|fake customer|test card|"
        r"test phone|test inbox|test record|golden payload)\b",
        re.I,
    ),
    "credential_isolation": re.compile(
        r"\b(?:credential isolation|sandbox credential|test credential|separate credential|separate secret|"
        r"non[- ]production secret|env vars?|api key|client secret|secret rotation|no production credential)\b",
        re.I,
    ),
    "webhook_endpoint_setup": re.compile(
        r"\b(?:webhook endpoint|webhook receiver|callback url|ngrok|tunnel|signing secret|"
        r"webhook signature|replay webhook|fixture webhook)\b",
        re.I,
    ),
    "rate_limit_safe_validation": re.compile(
        r"\b(?:rate limit|throttl|backoff|retry budget|qps|rps|quota(?:[- ]safe)?|paced validation|limited calls|"
        r"concurrency limit)\b",
        re.I,
    ),
    "fallback_manual_test_steps": re.compile(
        r"\b(?:manual test|manual verification|fallback|runbook|checklist|vendor dashboard|"
        r"manual QA|manual step|console verification)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskThirdPartySandboxReadinessRecord:
    """Sandbox readiness guidance for one third-party integration task."""

    task_id: str
    title: str
    risk_level: ThirdPartySandboxRisk
    service_names: tuple[str, ...] = field(default_factory=tuple)
    sandbox_needs: tuple[ThirdPartySandboxNeed, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    credential_setup_assumptions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "service_names": list(self.service_names),
            "sandbox_needs": list(self.sandbox_needs),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "credential_setup_assumptions": list(self.credential_setup_assumptions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskThirdPartySandboxReadinessPlan:
    """Task-level sandbox readiness plan for third-party integrations."""

    plan_id: str | None = None
    records: tuple[TaskThirdPartySandboxReadinessRecord, ...] = field(default_factory=tuple)
    third_party_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "third_party_task_ids": list(self.third_party_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return sandbox readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the sandbox readiness plan as deterministic Markdown."""
        title = "# Task Third-Party Sandbox Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Third-party task count: {self.summary.get('third_party_task_count', 0)}",
            f"- Missing acceptance criterion count: {self.summary.get('missing_acceptance_criterion_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No third-party integration tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Services | Sandbox Needs | Missing Acceptance Criteria | Credential Assumptions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell('; '.join(record.service_names) or 'External service')} | "
                f"{_markdown_cell('; '.join(record.sandbox_needs) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.credential_setup_assumptions) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_third_party_sandbox_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskThirdPartySandboxReadinessPlan:
    """Build sandbox readiness guidance for third-party integration tasks."""
    plan_id, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index)) is not None
    ]
    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            record.task_id,
            record.title.casefold(),
            record.service_names,
        )
    )
    result = tuple(records)
    risk_counts = {
        risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER
    }
    service_counts: dict[str, int] = {}
    for service in sorted(
        {service for record in result for service in record.service_names}, key=str.casefold
    ):
        service_counts[service] = sum(1 for record in result if service in record.service_names)
    return TaskThirdPartySandboxReadinessPlan(
        plan_id=plan_id,
        records=result,
        third_party_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "third_party_task_count": len(result),
            "missing_acceptance_criterion_count": sum(
                len(record.missing_acceptance_criteria) for record in result
            ),
            "risk_counts": risk_counts,
            "service_counts": service_counts,
        },
    )


def summarize_task_third_party_sandbox_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskThirdPartySandboxReadinessPlan:
    """Compatibility alias for building third-party sandbox readiness plans."""
    return build_task_third_party_sandbox_readiness_plan(source)


def task_third_party_sandbox_readiness_plan_to_dict(
    result: TaskThirdPartySandboxReadinessPlan,
) -> dict[str, Any]:
    """Serialize a third-party sandbox readiness plan to a plain dictionary."""
    return result.to_dict()


task_third_party_sandbox_readiness_plan_to_dict.__test__ = False


def task_third_party_sandbox_readiness_plan_to_markdown(
    result: TaskThirdPartySandboxReadinessPlan,
) -> str:
    """Render a third-party sandbox readiness plan as Markdown."""
    return result.to_markdown()


task_third_party_sandbox_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    service_names: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    webhook_evidence: tuple[str, ...] = field(default_factory=tuple)
    sensitive_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(task: Mapping[str, Any], index: int) -> TaskThirdPartySandboxReadinessRecord | None:
    signals = _signals(task)
    if not signals.evidence:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    sandbox_needs = _SANDBOX_NEED_ORDER
    acceptance_context = _acceptance_context(task)
    missing_needs = tuple(
        need for need in sandbox_needs if not _NEED_PATTERNS[need].search(acceptance_context)
    )
    return TaskThirdPartySandboxReadinessRecord(
        task_id=task_id,
        title=title,
        risk_level=_risk_level(signals, missing_needs),
        service_names=signals.service_names or ("External service",),
        sandbox_needs=sandbox_needs,
        missing_acceptance_criteria=_recommended_acceptance_criteria(missing_needs),
        credential_setup_assumptions=_credential_setup_assumptions(signals.service_names),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    service_names: set[str] = set()
    evidence: list[str] = []
    webhook_evidence: list[str] = []
    sensitive_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_services = _services(path_text)
        if matched_services:
            service_names.update(matched_services)
            evidence.append(f"files_or_modules: {path}")
        elif _PATH_SERVICE_RE.search(normalized):
            evidence.append(f"files_or_modules: {path}")
        if _WEBHOOK_RE.search(path_text):
            webhook_evidence.append(f"files_or_modules: {path}")
        if _PAYMENT_OR_OAUTH_RE.search(path_text):
            sensitive_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched_services = _services(text)
        generic_match = _GENERIC_SERVICE_RE.search(text)
        if matched_services:
            service_names.update(matched_services)
        if matched_services or generic_match:
            evidence.append(_evidence_snippet(source_field, text))
        if _WEBHOOK_RE.search(text):
            webhook_evidence.append(_evidence_snippet(source_field, text))
        if _PAYMENT_OR_OAUTH_RE.search(text):
            sensitive_evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        service_names=tuple(sorted(service_names, key=str.casefold)),
        evidence=tuple(_dedupe(evidence)),
        webhook_evidence=tuple(_dedupe(webhook_evidence)),
        sensitive_evidence=tuple(_dedupe(sensitive_evidence)),
    )


def _services(text: str) -> tuple[str, ...]:
    return tuple(name for name, pattern in _VENDOR_PATTERNS if pattern.search(text))


def _risk_level(
    signals: _Signals,
    missing_needs: tuple[ThirdPartySandboxNeed, ...],
) -> ThirdPartySandboxRisk:
    missing = set(missing_needs)
    if not missing:
        return "low"
    if "credential_isolation" in missing and (signals.service_names or signals.sensitive_evidence):
        return "high"
    if "test_account_availability" in missing and signals.service_names:
        return "high"
    if "webhook_endpoint_setup" in missing and signals.webhook_evidence:
        return "high"
    if "rate_limit_safe_validation" in missing and len(missing) >= 4:
        return "high"
    return "medium"


def _recommended_acceptance_criteria(
    missing_needs: tuple[ThirdPartySandboxNeed, ...],
) -> tuple[str, ...]:
    templates = {
        "test_account_availability": "A vendor sandbox or dedicated test account is available before implementation starts.",
        "fixture_test_data_setup": "Representative fixture data or vendor test objects cover successful, failed, and edge-case responses.",
        "credential_isolation": "Sandbox credentials are isolated from production secrets and documented in non-production setup steps.",
        "webhook_endpoint_setup": "Webhook or callback validation uses a non-production endpoint with signature verification and replay steps.",
        "rate_limit_safe_validation": "Validation uses throttling, quotas, or bounded test calls to avoid vendor rate-limit impact.",
        "fallback_manual_test_steps": "Manual fallback test steps are documented for vendor-dashboard or console verification.",
    }
    return tuple(templates[need] for need in missing_needs)


def _credential_setup_assumptions(service_names: tuple[str, ...]) -> tuple[str, ...]:
    services = ", ".join(service_names) if service_names else "the external service"
    return (
        f"Non-production credentials for {services} are provisioned separately from production.",
        "Credential names, owners, and rotation/setup steps are documented before agent implementation.",
    )


def _acceptance_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "risks",
        "risk",
        "test_command",
        "validation_commands",
    ):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(
                keyword in normalized
                for keyword in (
                    "acceptance",
                    "criteria",
                    "sandbox",
                    "credential",
                    "fixture",
                    "webhook",
                    "rate",
                    "manual",
                    "setup",
                    "test",
                )
            ):
                values.append(text)
    return " ".join(values)


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
        "criteria",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "tags",
        "labels",
        "notes",
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
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "risks",
        "depends_on",
        "tags",
        "labels",
        "notes",
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _GENERIC_SERVICE_RE.search(key_text) or _services(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _GENERIC_SERVICE_RE.search(key_text) or _services(key_text):
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
    "ThirdPartySandboxNeed",
    "ThirdPartySandboxRisk",
    "TaskThirdPartySandboxReadinessPlan",
    "TaskThirdPartySandboxReadinessRecord",
    "build_task_third_party_sandbox_readiness_plan",
    "summarize_task_third_party_sandbox_readiness",
    "task_third_party_sandbox_readiness_plan_to_dict",
    "task_third_party_sandbox_readiness_plan_to_markdown",
]
