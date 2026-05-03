"""Assess task readiness for email bounce and complaint handling work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


EmailBounceHandlingSignal = Literal[
    "bounce_handling",
    "hard_bounce",
    "soft_bounce",
    "complaint_handling",
    "suppression_list",
    "unsubscribe_interaction",
    "provider_webhook",
    "delivery_status",
    "reactivation",
]
EmailBounceHandlingSafeguard = Literal[
    "hard_bounce_suppression",
    "soft_bounce_retry_policy",
    "complaint_handling",
    "unsubscribe_interaction",
    "provider_webhook_verification",
    "delivery_status_persistence",
    "dashboard_alerting",
    "reactivation_policy",
]
EmailBounceHandlingReadiness = Literal["weak", "moderate", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[EmailBounceHandlingSignal, ...] = (
    "bounce_handling",
    "hard_bounce",
    "soft_bounce",
    "complaint_handling",
    "suppression_list",
    "unsubscribe_interaction",
    "provider_webhook",
    "delivery_status",
    "reactivation",
)
_SAFEGUARD_ORDER: tuple[EmailBounceHandlingSafeguard, ...] = (
    "hard_bounce_suppression",
    "soft_bounce_retry_policy",
    "complaint_handling",
    "unsubscribe_interaction",
    "provider_webhook_verification",
    "delivery_status_persistence",
    "dashboard_alerting",
    "reactivation_policy",
)
_READINESS_ORDER: dict[EmailBounceHandlingReadiness, int] = {"weak": 0, "moderate": 1, "strong": 2}
_CORE_SAFEGUARDS = frozenset(
    {
        "hard_bounce_suppression",
        "soft_bounce_retry_policy",
        "provider_webhook_verification",
        "delivery_status_persistence",
    }
)

_SIGNAL_PATTERNS: dict[EmailBounceHandlingSignal, re.Pattern[str]] = {
    "bounce_handling": re.compile(
        r"\b(?:bounce handling|bounce processing|email bounces?|mail bounces?|bounced recipients?|"
        r"bounce events?|bounce webhook|bounce webhooks)\b",
        re.I,
    ),
    "hard_bounce": re.compile(r"\b(?:hard bounces?|permanent bounces?|permanent failures?|invalid recipients?)\b", re.I),
    "soft_bounce": re.compile(r"\b(?:soft bounces?|temporary bounces?|transient failures?|mailbox full|retry bounces?)\b", re.I),
    "complaint_handling": re.compile(
        r"\b(?:complaint handling|spam complaints?|abuse complaints?|feedback loop|fbl|complaint events?)\b",
        re.I,
    ),
    "suppression_list": re.compile(
        r"\b(?:suppression list|suppressed recipients?|global suppression|email suppression|blocklist)\b",
        re.I,
    ),
    "unsubscribe_interaction": re.compile(
        r"\b(?:unsubscribe|list-unsubscribe|opt out|opt-out|preference center|email preferences?)\b",
        re.I,
    ),
    "provider_webhook": re.compile(
        r"\b(?:provider webhook|email webhook|sendgrid webhook|mailgun webhook|postmark webhook|ses notification|"
        r"sns notification|event webhook|webhook signature)\b",
        re.I,
    ),
    "delivery_status": re.compile(
        r"\b(?:delivery status|delivery state|message status|recipient status|delivery events?|delivery log|"
        r"message events?|accepted|delivered|rejected)\b",
        re.I,
    ),
    "reactivation": re.compile(
        r"\b(?:reactivat(?:e|ion)|resubscribe|restore recipient|unsuppress|suppression expiry|recipient recovery)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[EmailBounceHandlingSignal, re.Pattern[str]] = {
    "bounce_handling": re.compile(r"bounce|bounces|bounce[_-]?handling|bounce[_-]?processor", re.I),
    "hard_bounce": re.compile(r"hard[_-]?bounce|permanent[_-]?(?:bounce|failure)|invalid[_-]?recipient", re.I),
    "soft_bounce": re.compile(r"soft[_-]?bounce|temporary[_-]?(?:bounce|failure)|retry[_-]?bounce", re.I),
    "complaint_handling": re.compile(r"complaint|feedback[_-]?loop|fbl|abuse", re.I),
    "suppression_list": re.compile(r"suppress|suppression|blocklist", re.I),
    "unsubscribe_interaction": re.compile(r"unsubscribe|opt[_-]?out|preference[_-]?center", re.I),
    "provider_webhook": re.compile(r"webhook|sendgrid|mailgun|postmark|ses|sns", re.I),
    "delivery_status": re.compile(r"delivery[_-]?(?:status|state|event|log)|message[_-]?status", re.I),
    "reactivation": re.compile(r"reactivat|resubscribe|unsuppress|restore[_-]?recipient", re.I),
}
_SAFEGUARD_PATTERNS: dict[EmailBounceHandlingSafeguard, re.Pattern[str]] = {
    "hard_bounce_suppression": re.compile(
        r"\b(?:hard bounces?.{0,80}suppress(?:ion|ed|es)?|suppress(?:ion|ed|es)?.{0,80}hard bounces?|"
        r"permanent bounces?.{0,80}suppress(?:ion|ed|es)?|invalid recipients?.{0,80}suppress(?:ion|ed|es)?|"
        r"suppression list.{0,80}hard bounces?)\b",
        re.I,
    ),
    "soft_bounce_retry_policy": re.compile(
        r"\b(?:soft bounces?.{0,80}(?:retry|backoff|threshold|policy)|temporary bounces?.{0,80}(?:retry|backoff|threshold)|"
        r"retry policy|retry backoff|transient failure.{0,80}retry|mailbox full.{0,80}retry)\b",
        re.I,
    ),
    "complaint_handling": re.compile(
        r"\b(?:complaint handling|spam complaints?.{0,80}(?:suppress|process|webhook)|feedback loop|fbl|"
        r"abuse complaints?.{0,80}(?:suppress|process))\b",
        re.I,
    ),
    "unsubscribe_interaction": re.compile(
        r"\b(?:unsubscribe.{0,80}(?:suppression|bounce|complaint|priority|interaction)|list-unsubscribe|"
        r"preference center|opt out|opt-out|email preferences?)\b",
        re.I,
    ),
    "provider_webhook_verification": re.compile(
        r"\b(?:webhook signature|verify webhook|verified webhook|provider webhook.{0,80}(?:verification|signature|authentic|secret)|"
        r"sendgrid signature|mailgun signature|postmark signature|ses sns.{0,80}(?:signature|verify)|sns signature)\b",
        re.I,
    ),
    "delivery_status_persistence": re.compile(
        r"\b(?:delivery status persistence|persist(?:ed|s)? delivery status|store delivery status|delivery status.{0,80}(?:persist|store|table|history)|"
        r"message status.{0,80}(?:persist|store|history)|delivery event.{0,80}(?:persist|store|ledger|table))\b",
        re.I,
    ),
    "dashboard_alerting": re.compile(
        r"\b(?:dashboard|alerting|alerts?|metrics?|monitoring|bounce rate|complaint rate|suppression metrics?|"
        r"provider alerts?|pager)\b",
        re.I,
    ),
    "reactivation_policy": re.compile(
        r"\b(?:reactivation policy|reactivat(?:e|ion).{0,80}(?:policy|approval|manual|review)|resubscribe.{0,80}(?:policy|approval)|"
        r"unsuppress.{0,80}(?:policy|approval|manual)|restore recipient.{0,80}(?:policy|approval))\b",
        re.I,
    ),
}
_SAFEGUARD_RECOMMENDATIONS: dict[EmailBounceHandlingSafeguard, str] = {
    "hard_bounce_suppression": "Suppress hard-bounced recipients before any future send attempts.",
    "soft_bounce_retry_policy": "Define soft-bounce retry limits, backoff, thresholds, and final disposition.",
    "complaint_handling": "Process spam or abuse complaints and add complained recipients to suppression handling.",
    "unsubscribe_interaction": "Specify how unsubscribes, preference changes, bounces, and complaints interact.",
    "provider_webhook_verification": "Verify provider webhook signatures, secrets, or SNS authenticity before processing events.",
    "delivery_status_persistence": "Persist delivery, bounce, complaint, and suppression status for each recipient or message.",
    "dashboard_alerting": "Add dashboards and alerts for bounce rates, complaint rates, webhook failures, and suppression changes.",
    "reactivation_policy": "Document when and how suppressed recipients can be reviewed, reactivated, or resubscribed.",
}
_DOC_PATH_RE = re.compile(r"(?:^|/)(?:docs?|adr|design|rfcs?)(?:/|$)|\.(?:md|mdx|rst|txt)$", re.I)
_DOC_TASK_RE = re.compile(r"\b(?:docs?|documentation|readme|adr|design doc|runbook copy|changelog)\b", re.I)


@dataclass(frozen=True, slots=True)
class TaskEmailBounceHandlingReadinessRecord:
    """Readiness guidance for one email bounce handling task."""

    task_id: str
    title: str
    bounce_signals: tuple[EmailBounceHandlingSignal, ...]
    present_safeguards: tuple[EmailBounceHandlingSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[EmailBounceHandlingSafeguard, ...] = field(default_factory=tuple)
    readiness: EmailBounceHandlingReadiness = "moderate"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[EmailBounceHandlingSignal, ...]:
        """Compatibility view for callers that expect matched signals."""
        return self.bounce_signals

    @property
    def risk_level(self) -> EmailBounceHandlingReadiness:
        """Compatibility view for callers that group by risk/readiness."""
        return self.readiness

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "bounce_signals": list(self.bounce_signals),
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommendations": list(self.recommendations),
        }


@dataclass(frozen=True, slots=True)
class TaskEmailBounceHandlingReadinessPlan:
    """Plan-level email bounce handling readiness review."""

    plan_id: str | None = None
    records: tuple[TaskEmailBounceHandlingReadinessRecord, ...] = field(default_factory=tuple)
    bounce_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskEmailBounceHandlingReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskEmailBounceHandlingReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "bounce_task_ids": list(self.bounce_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return email bounce readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render email bounce readiness guidance as deterministic Markdown."""
        title = "# Task Email Bounce Handling Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Bounce task count: {self.summary.get('bounce_task_count', 0)}",
            f"- Suppressed task count: {self.summary.get('suppressed_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No email bounce handling readiness records were inferred."])
            if self.suppressed_task_ids:
                lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Recommendations |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.bounce_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommendations) or 'none')} |"
            )
        if self.suppressed_task_ids:
            lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
        return "\n".join(lines)


def build_task_email_bounce_handling_readiness_plan(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Build email bounce handling readiness records for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    record_ids = {record.task_id for record in records}
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in record_ids
    )
    return TaskEmailBounceHandlingReadinessPlan(
        plan_id=plan_id,
        records=records,
        bounce_task_ids=tuple(record.task_id for record in records),
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, task_count=len(tasks), suppressed_task_ids=suppressed_task_ids),
    )


def analyze_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for building email bounce handling readiness plans."""
    return build_task_email_bounce_handling_readiness_plan(source)


def derive_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for deriving email bounce handling readiness plans."""
    return build_task_email_bounce_handling_readiness_plan(source)


def extract_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for extracting email bounce handling readiness plans."""
    return build_task_email_bounce_handling_readiness_plan(source)


def generate_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for generating email bounce handling readiness plans."""
    return build_task_email_bounce_handling_readiness_plan(source)


def recommend_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for recommending email bounce handling safeguards."""
    return build_task_email_bounce_handling_readiness_plan(source)


def summarize_task_email_bounce_handling_readiness(source: Any) -> TaskEmailBounceHandlingReadinessPlan:
    """Compatibility alias for summarizing email bounce handling readiness plans."""
    return build_task_email_bounce_handling_readiness_plan(source)


def task_email_bounce_handling_readiness_plan_to_dict(
    result: TaskEmailBounceHandlingReadinessPlan,
) -> dict[str, Any]:
    """Serialize an email bounce handling readiness plan to a plain dictionary."""
    return result.to_dict()


task_email_bounce_handling_readiness_plan_to_dict.__test__ = False


def task_email_bounce_handling_readiness_plan_to_dicts(
    result: TaskEmailBounceHandlingReadinessPlan | Iterable[TaskEmailBounceHandlingReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize email bounce handling readiness records to plain dictionaries."""
    if isinstance(result, TaskEmailBounceHandlingReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_email_bounce_handling_readiness_plan_to_dicts.__test__ = False


def task_email_bounce_handling_readiness_plan_to_markdown(
    result: TaskEmailBounceHandlingReadinessPlan,
) -> str:
    """Render an email bounce handling readiness plan as Markdown."""
    return result.to_markdown()


task_email_bounce_handling_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[EmailBounceHandlingSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    safeguards: tuple[EmailBounceHandlingSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskEmailBounceHandlingReadinessRecord | None:
    if _is_documentation_only(task):
        return None
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    task_id = _task_id(task, index)
    return TaskEmailBounceHandlingReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        bounce_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.safeguards, missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommendations=tuple(_SAFEGUARD_RECOMMENDATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[EmailBounceHandlingSignal] = set()
    safeguard_hits: set[EmailBounceHandlingSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
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
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _readiness(
    present: tuple[EmailBounceHandlingSafeguard, ...],
    missing: tuple[EmailBounceHandlingSafeguard, ...],
) -> EmailBounceHandlingReadiness:
    if not missing:
        return "strong"
    present_set = set(present)
    if _CORE_SAFEGUARDS <= present_set and len(present_set) >= 5:
        return "moderate"
    return "weak"


def _summary(
    records: tuple[TaskEmailBounceHandlingReadinessRecord, ...],
    *,
    task_count: int,
    suppressed_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "bounce_task_count": len(records),
        "bounce_task_ids": [record.task_id for record in records],
        "suppressed_task_count": len(suppressed_task_ids),
        "suppressed_task_ids": list(suppressed_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.bounce_signals)
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


def _is_documentation_only(task: Mapping[str, Any]) -> bool:
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    has_doc_path = bool(paths) and all(_DOC_PATH_RE.search(_normalized_path(path)) for path in paths)
    task_text = " ".join(_strings([task.get("title"), task.get("description"), task.get("tags"), task.get("labels")]))
    return has_doc_path and _DOC_TASK_RE.search(task_text) is not None


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
    "EmailBounceHandlingReadiness",
    "EmailBounceHandlingSafeguard",
    "EmailBounceHandlingSignal",
    "TaskEmailBounceHandlingReadinessPlan",
    "TaskEmailBounceHandlingReadinessRecord",
    "analyze_task_email_bounce_handling_readiness",
    "build_task_email_bounce_handling_readiness_plan",
    "derive_task_email_bounce_handling_readiness",
    "extract_task_email_bounce_handling_readiness",
    "generate_task_email_bounce_handling_readiness",
    "recommend_task_email_bounce_handling_readiness",
    "summarize_task_email_bounce_handling_readiness",
    "task_email_bounce_handling_readiness_plan_to_dict",
    "task_email_bounce_handling_readiness_plan_to_dicts",
    "task_email_bounce_handling_readiness_plan_to_markdown",
]
