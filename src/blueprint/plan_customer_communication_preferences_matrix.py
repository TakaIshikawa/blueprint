"""Build plan-level customer communication preferences matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_secrets_rotation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


CustomerCommunicationPreferenceReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[CustomerCommunicationPreferenceReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_COMM_RE = re.compile(r"\b(?:communication|comms|notification|notify|email|sms|push|in-app|digest|preference|unsubscribe|opt[- ](?:in|out)|consent)\b", re.I)
_CUSTOMER_RE = re.compile(r"\b(?:customer|user|account|tenant|admin|recipient|subscriber)\b", re.I)
_AUDIENCE_RE = re.compile(r"\b(?:audience|segment|recipient|customer|tenant|admin|user|subscriber|cohort)\b", re.I)
_CHANNEL_RE = re.compile(r"\b(?:channel|email|sms|push|in-app|webhook|digest|slack|notification center)\b", re.I)
_PREFERENCE_RE = re.compile(r"\b(?:preference|preferences|opt[- ](?:in|out)|unsubscribe|subscribe|consent|frequency|quiet hours|locale)\b", re.I)
_OWNER_RE = re.compile(r"\b(?:owner|dri|responsible|team|lead|lifecycle|marketing ops|customer success|support)\b", re.I)
_DEPENDENCY_RE = re.compile(r"\b(?:dependency|depends|integration|esp|sendgrid|braze|customer.io|segment|crm|downstream|upstream)\b", re.I)
_VERIFICATION_RE = re.compile(r"\b(?:verify|verification|validate|validation|test|preview|seed|deliverability|suppression|monitor|audit)\b", re.I)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|disable|suppress|pause|restore|fallback|kill switch)\b", re.I)
_EVIDENCE_RE = re.compile(r"\b(?:audit|evidence|log|ticket|approval|record|consent record|suppression list|export)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanCustomerCommunicationPreferencesRow:
    """Readiness signals for one customer communication preference task."""

    task_id: str
    title: str
    audience: str = "missing"
    channels: str = "missing"
    preference_source: str = "missing"
    owner: str = "missing"
    dependency_impact: str = "missing"
    verification: str = "missing"
    rollback: str = "missing"
    evidence_status: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: CustomerCommunicationPreferenceReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "audience": self.audience,
            "channels": self.channels,
            "preference_source": self.preference_source,
            "owner": self.owner,
            "dependency_impact": self.dependency_impact,
            "verification": self.verification,
            "rollback": self.rollback,
            "evidence_status": self.evidence_status,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCustomerCommunicationPreferencesMatrix:
    """Plan-level customer communication preferences matrix."""

    plan_id: str | None = None
    rows: tuple[PlanCustomerCommunicationPreferencesRow, ...] = field(default_factory=tuple)
    communication_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_communication_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanCustomerCommunicationPreferencesRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "communication_task_ids": list(self.communication_task_ids),
            "no_communication_task_ids": list(self.no_communication_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan Customer Communication Preferences Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        if not self.rows:
            return "\n".join([title, "", "No customer communication preference rows were inferred."])
        lines = [
            title,
            "",
            "| Task | Title | Audience | Channels | Preference Source | Owner | Dependencies | Verification | Rollback | Evidence | Readiness | Gaps |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | {row.audience} | "
                f"{row.channels} | {row.preference_source} | {row.owner} | {row.dependency_impact} | "
                f"{row.verification} | {row.rollback} | {row.evidence_status} | {row.readiness} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_customer_communication_preferences_matrix(source: Any) -> PlanCustomerCommunicationPreferencesMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanCustomerCommunicationPreferencesRow] = []
    no_communication_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_communication_task_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], -len(row.gaps), row.task_id))
    result = tuple(rows)
    return PlanCustomerCommunicationPreferencesMatrix(
        plan_id=plan_id,
        rows=result,
        communication_task_ids=tuple(row.task_id for row in result),
        no_communication_task_ids=tuple(no_communication_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_customer_communication_preferences_matrix(source: Any) -> PlanCustomerCommunicationPreferencesMatrix:
    return build_plan_customer_communication_preferences_matrix(source)


def analyze_plan_customer_communication_preferences_matrix(source: Any) -> PlanCustomerCommunicationPreferencesMatrix:
    if isinstance(source, PlanCustomerCommunicationPreferencesMatrix):
        return source
    return build_plan_customer_communication_preferences_matrix(source)


def derive_plan_customer_communication_preferences_matrix(source: Any) -> PlanCustomerCommunicationPreferencesMatrix:
    return analyze_plan_customer_communication_preferences_matrix(source)


def extract_plan_customer_communication_preferences_matrix(source: Any) -> PlanCustomerCommunicationPreferencesMatrix:
    return derive_plan_customer_communication_preferences_matrix(source)


def summarize_plan_customer_communication_preferences_matrix(
    source: PlanCustomerCommunicationPreferencesMatrix | Iterable[PlanCustomerCommunicationPreferencesRow] | Any,
) -> dict[str, Any] | PlanCustomerCommunicationPreferencesMatrix:
    if isinstance(source, PlanCustomerCommunicationPreferencesMatrix):
        return dict(source.summary)
    if isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)) or hasattr(source, "tasks") or hasattr(source, "title"):
        return build_plan_customer_communication_preferences_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_customer_communication_preferences_matrix_to_dict(
    matrix: PlanCustomerCommunicationPreferencesMatrix,
) -> dict[str, Any]:
    return matrix.to_dict()


plan_customer_communication_preferences_matrix_to_dict.__test__ = False


def plan_customer_communication_preferences_matrix_to_dicts(
    matrix: PlanCustomerCommunicationPreferencesMatrix | Iterable[PlanCustomerCommunicationPreferencesRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanCustomerCommunicationPreferencesMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_customer_communication_preferences_matrix_to_dicts.__test__ = False


def plan_customer_communication_preferences_matrix_to_markdown(
    matrix: PlanCustomerCommunicationPreferencesMatrix,
) -> str:
    return matrix.to_markdown()


plan_customer_communication_preferences_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanCustomerCommunicationPreferencesRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_COMM_RE.search(context) and _CUSTOMER_RE.search(context)):
        return None
    statuses = {
        "audience": _status(_AUDIENCE_RE, texts),
        "channels": _status(_CHANNEL_RE, texts),
        "preference_source": _status(_PREFERENCE_RE, texts),
        "owner": _status(_OWNER_RE, texts),
        "dependency_impact": _status(_DEPENDENCY_RE, texts),
        "verification": _status(_VERIFICATION_RE, texts),
        "rollback": _status(_ROLLBACK_RE, texts),
        "evidence_status": _status(_EVIDENCE_RE, texts),
    }
    labels = {
        "audience": "audience",
        "channels": "channels",
        "preference_source": "preference source",
        "owner": "owner",
        "dependency_impact": "dependency impact",
        "verification": "verification",
        "rollback": "rollback",
        "evidence_status": "evidence status",
    }
    gaps = tuple(f"Missing {label}." for field, label in labels.items() if statuses[field] == "missing")
    readiness: CustomerCommunicationPreferenceReadiness = "ready"
    if statuses["preference_source"] == "missing" or statuses["verification"] == "missing" or statuses["rollback"] == "missing":
        readiness = "blocked"
    elif gaps:
        readiness = "partial"
    return PlanCustomerCommunicationPreferencesRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _COMM_RE.search(text) or _CUSTOMER_RE.search(text))),
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[PlanCustomerCommunicationPreferencesRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "communication_task_count": len(row_list),
        "no_communication_task_count": task_count - len(row_list),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
    }
