"""Build plan-level webhook delivery readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_cache_invalidation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _looks_like_plan,
    _looks_like_task,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


WebhookDeliveryReadiness = Literal["ready", "partial", "blocked"]
WebhookDeliveryReadinessArea = Literal[
    "retry_policy",
    "signature_verification",
    "delivery_logging",
    "replay_tooling",
    "timeout_handling",
    "dead_letter_handling",
    "consumer_notifications",
]
WebhookDeliveryRisk = Literal["high", "medium", "low"]

_READINESS_ORDER: dict[WebhookDeliveryReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_RISK_ORDER: dict[WebhookDeliveryRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AREA_ORDER: dict[WebhookDeliveryReadinessArea, int] = {
    "retry_policy": 0,
    "signature_verification": 1,
    "delivery_logging": 2,
    "replay_tooling": 3,
    "timeout_handling": 4,
    "dead_letter_handling": 5,
    "consumer_notifications": 6,
}
_WEBHOOK_RE = re.compile(r"\b(?:webhook|web hook|callback|event delivery|producer|consumer|subscription|subscriber)\b", re.I)
_SIGNALS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("retry_policy", "Missing retry policy.", re.compile(r"\b(?:retry|retries|backoff|retry-after|attempt|exponential|at least once)\b", re.I)),
    ("signature_verification", "Missing signature verification.", re.compile(r"\b(?:signature|signing|hmac|secret|verify|verification|authenticity)\b", re.I)),
    ("delivery_logging", "Missing delivery logging.", re.compile(r"\b(?:log|logging|audit|delivery record|delivery status|history|trace)\b", re.I)),
    ("replay_tooling", "Missing replay tooling.", re.compile(r"\b(?:replay|redeliver|re-deliver|resend|manual retry|backfill event)\b", re.I)),
    ("timeout_handling", "Missing timeout handling.", re.compile(r"\b(?:timeout|time out|deadline|latency|slow consumer|response window)\b", re.I)),
    ("dead_letter_handling", "Missing dead-letter handling.", re.compile(r"\b(?:dead[- ]letter|dlq|quarantine|poison|failed delivery queue|parking lot)\b", re.I)),
    ("consumer_notification", "Missing consumer notification coverage.", re.compile(r"\b(?:notify|notification|email|alert|consumer docs|subscriber notice|developer portal)\b", re.I)),
)
_AREA_PATTERNS: dict[WebhookDeliveryReadinessArea, re.Pattern[str]] = {
    "retry_policy": re.compile(r"\b(?:retry policy|delivery retry|retry schedule|retry attempts?|exponential backoff|backoff|retry-after)\b", re.I),
    "signature_verification": re.compile(r"\b(?:signature verification|verify signature|webhook signing|signed payload|hmac|secret rotation|timestamp tolerance)\b", re.I),
    "delivery_logging": re.compile(r"\b(?:delivery log|delivery logging|attempt log|audit log|webhook log|metrics|trace|request id|delivery status)\b", re.I),
    "replay_tooling": re.compile(r"\b(?:replay tooling|replay tool|redeliver|redelivery|manual replay|event replay|replay failed)\b", re.I),
    "timeout_handling": re.compile(r"\b(?:timeout|time out|connect timeout|read timeout|ack timeout|deadline|slow consumer)\b", re.I),
    "dead_letter_handling": re.compile(r"\b(?:dead[- ]letter|dlq|poison event|failed delivery queue|undeliverable|quarantine)\b", re.I),
    "consumer_notifications": re.compile(r"\b(?:consumer notification|subscriber notification|notify consumer|developer email|admin alert|failure notification|endpoint disabled)\b", re.I),
}
_DEFAULT_OWNERS: dict[WebhookDeliveryReadinessArea, str] = {
    "retry_policy": "delivery_owner",
    "signature_verification": "security_owner",
    "delivery_logging": "observability_owner",
    "replay_tooling": "delivery_owner",
    "timeout_handling": "delivery_owner",
    "dead_letter_handling": "delivery_owner",
    "consumer_notifications": "developer_experience_owner",
}
_AREA_GAPS: dict[WebhookDeliveryReadinessArea, str] = {
    "retry_policy": "Missing webhook retry policy.",
    "signature_verification": "Missing signature verification.",
    "delivery_logging": "Missing delivery logging.",
    "replay_tooling": "Missing replay tooling.",
    "timeout_handling": "Missing timeout handling.",
    "dead_letter_handling": "Missing dead-letter handling.",
    "consumer_notifications": "Missing consumer notification coverage.",
}
_ACTIONS: dict[WebhookDeliveryReadinessArea, str] = {
    "retry_policy": "Define retry attempts, backoff schedule, terminal failure behavior, and retry-safe event semantics.",
    "signature_verification": "Specify signing algorithm, secret handling, timestamp tolerance, and consumer verification path.",
    "delivery_logging": "Record delivery attempts, response codes, latency, request IDs, and terminal delivery status.",
    "replay_tooling": "Provide operator or customer tooling to replay failed webhook deliveries.",
    "timeout_handling": "Set connect/read timeouts and document slow-consumer handling.",
    "dead_letter_handling": "Route exhausted or malformed deliveries to a dead-letter queue with triage ownership.",
    "consumer_notifications": "Notify consumers about disabled endpoints, repeated failures, and replay outcomes.",
}
_HIGH_GAP_AREAS = frozenset({"retry_policy", "signature_verification", "dead_letter_handling"})
_OWNER_KEYS = ("owner", "owners", "owner_hint", "owner_team", "team", "dri", "api_owner", "backend_owner")


@dataclass(frozen=True, slots=True)
class PlanWebhookDeliveryReadinessRow:
    task_id: str = ""
    title: str = ""
    retry_policy: str = "missing"
    signature_verification: str = "missing"
    delivery_logging: str = "missing"
    replay_tooling: str = "missing"
    timeout_handling: str = "missing"
    dead_letter_handling: str = "missing"
    consumer_notification: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: WebhookDeliveryReadiness = "partial"
    readiness_score: float = 0.0
    evidence: tuple[str, ...] = field(default_factory=tuple)
    area: WebhookDeliveryReadinessArea | str = ""
    owner: str = ""
    risk: WebhookDeliveryRisk = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    score: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "retry_policy": self.retry_policy,
            "signature_verification": self.signature_verification,
            "delivery_logging": self.delivery_logging,
            "replay_tooling": self.replay_tooling,
            "timeout_handling": self.timeout_handling,
            "dead_letter_handling": self.dead_letter_handling,
            "consumer_notification": self.consumer_notification,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "readiness_score": self.readiness_score,
            "evidence": list(self.evidence),
            "area": self.area,
            "owner": self.owner,
            "risk": self.risk,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
            "score": self.score,
        }


@dataclass(frozen=True, slots=True)
class PlanWebhookDeliveryReadinessMatrix:
    plan_id: str | None = None
    rows: tuple[PlanWebhookDeliveryReadinessRow, ...] = field(default_factory=tuple)
    webhook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_webhook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    gap_areas: tuple[WebhookDeliveryReadinessArea, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanWebhookDeliveryReadinessRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "webhook_task_ids": list(self.webhook_task_ids),
            "no_webhook_task_ids": list(self.no_webhook_task_ids),
            "gap_areas": list(self.gap_areas),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan Webhook Delivery Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        lines = [title, "", f"Summary: {self.summary.get('webhook_task_count', 0)} of {self.summary.get('task_count', 0)} tasks require webhook delivery readiness (blocked: {counts.get('blocked', 0)}, partial: {counts.get('partial', 0)}, ready: {counts.get('ready', 0)})."]
        if not self.rows:
            lines.extend(["", "No webhook delivery readiness rows were inferred."])
            return "\n".join(lines)
        if self.rows[0].area:
            lines = [
                title,
                "",
                (
                    f"Summary: {self.summary.get('ready_area_count', 0)} of "
                    f"{self.summary.get('area_count', 0)} webhook delivery readiness areas ready "
                    f"(score: {self.summary.get('score', 0)})."
                ),
                "",
                "| Area | Owner | Readiness | Score | Risk | Evidence | Gaps | Next Action | Tasks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
            for row in self.rows:
                lines.append(
                    "| "
                    f"{row.area} | {_markdown_cell(row.owner)} | {row.readiness} | {row.score} | {row.risk} | "
                    f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                    f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                    f"{_markdown_cell(row.next_action)} | {_markdown_cell(', '.join(row.task_ids) or 'none')} |"
                )
            return "\n".join(lines)
        lines.extend(["", "| Task | Title | Retry | Signature | Logging | Replay | Timeout | Dead Letter | Notification | Readiness | Score | Gaps | Evidence |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"])
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | {_markdown_cell(row.title)} | {row.retry_policy} | "
                f"{row.signature_verification} | {row.delivery_logging} | {row.replay_tooling} | "
                f"{row.timeout_handling} | {row.dead_letter_handling} | {row.consumer_notification} | "
                f"{row.readiness} | {row.readiness_score:.2f} | {_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_webhook_delivery_readiness_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    if _use_area_mode(source):
        return _build_area_matrix(source)
    plan_id, tasks = _source_payload(source)
    rows: list[PlanWebhookDeliveryReadinessRow] = []
    skipped: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _row(task, index)
        if row is None:
            skipped.append(_task_id(task, index))
        else:
            rows.append(row)
    sorted_rows = tuple(sorted(rows, key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id)))
    return PlanWebhookDeliveryReadinessMatrix(
        plan_id=plan_id,
        rows=sorted_rows,
        webhook_task_ids=tuple(row.task_id for row in sorted_rows),
        no_webhook_task_ids=tuple(skipped),
        gap_areas=(),
        summary=_summary(len(tasks), sorted_rows, skipped),
    )


def generate_plan_webhook_delivery_readiness_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    if isinstance(source, PlanWebhookDeliveryReadinessMatrix):
        return source
    return build_plan_webhook_delivery_readiness_matrix(source)


def analyze_plan_webhook_delivery_readiness_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    return generate_plan_webhook_delivery_readiness_matrix(source)


def derive_plan_webhook_delivery_readiness_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    return analyze_plan_webhook_delivery_readiness_matrix(source)


def extract_plan_webhook_delivery_readiness_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    return derive_plan_webhook_delivery_readiness_matrix(source)


def summarize_plan_webhook_delivery_readiness_matrix(
    source: PlanWebhookDeliveryReadinessMatrix | Iterable[PlanWebhookDeliveryReadinessRow] | Any,
) -> dict[str, Any] | PlanWebhookDeliveryReadinessMatrix:
    if isinstance(source, PlanWebhookDeliveryReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_webhook_delivery_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_webhook_delivery_readiness_matrix_to_dict(matrix: PlanWebhookDeliveryReadinessMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_webhook_delivery_readiness_matrix_to_dict.__test__ = False


def plan_webhook_delivery_readiness_matrix_to_dicts(
    matrix: PlanWebhookDeliveryReadinessMatrix | Iterable[PlanWebhookDeliveryReadinessRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanWebhookDeliveryReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_webhook_delivery_readiness_matrix_to_dicts.__test__ = False


def plan_webhook_delivery_readiness_matrix_to_markdown(matrix: PlanWebhookDeliveryReadinessMatrix) -> str:
    return matrix.to_markdown()


plan_webhook_delivery_readiness_matrix_to_markdown.__test__ = False


def _row(task: Mapping[str, Any], index: int) -> PlanWebhookDeliveryReadinessRow | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not _WEBHOOK_RE.search(context):
        return None
    statuses = {name: _status(pattern, texts) for name, _, pattern in _SIGNALS}
    gaps = tuple(message for name, message, _ in _SIGNALS if statuses[name] == "missing")
    present = sum(1 for value in statuses.values() if value == "present")
    evidence = tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _WEBHOOK_RE.search(text) or any(pattern.search(text) for _, _, pattern in _SIGNALS)))
    return PlanWebhookDeliveryReadinessRow(
        task_id=task_id,
        title=title,
        gaps=gaps,
        readiness=_readiness(statuses),
        readiness_score=round(present / len(_SIGNALS), 2),
        evidence=evidence,
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _readiness(statuses: Mapping[str, str]) -> WebhookDeliveryReadiness:
    if statuses["signature_verification"] == "missing" or statuses["retry_policy"] == "missing":
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _summary(task_count: int, rows: Iterable[PlanWebhookDeliveryReadinessRow], skipped: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "webhook_task_count": len(row_list),
        "no_webhook_task_count": len(tuple(skipped)),
        "readiness_counts": {readiness: sum(1 for row in row_list if row.readiness == readiness) for readiness in _READINESS_ORDER},
        "gap_counts": {gap: sum(1 for row in row_list if gap in row.gaps) for gap in sorted({gap for row in row_list for gap in row.gaps})},
    }


def _use_area_mode(source: Any) -> bool:
    if isinstance(source, Mapping):
        return "tasks" in source and ("implementation_brief_id" in source or "milestones" in source)
    return False


def _build_area_matrix(source: Any) -> PlanWebhookDeliveryReadinessMatrix:
    plan_id, tasks = _source_payload(source)
    evidence: dict[WebhookDeliveryReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    task_ids: dict[WebhookDeliveryReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    owners: dict[WebhookDeliveryReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    detected: list[str] = []
    blocked = False
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        task_signal = bool(_WEBHOOK_RE.search(context))
        blocked = blocked or bool(re.search(r"\b(?:blocked|cannot proceed|missing dependency)\b", context, re.I))
        hints = _owner_hints(task)
        for area, pattern in _AREA_PATTERNS.items():
            matches = [_evidence_snippet(field, text) for field, text in texts if pattern.search(text)]
            if matches:
                task_signal = True
                evidence[area].extend(matches)
                task_ids[area].append(task_id)
                owners[area].extend(hints)
        if task_signal:
            detected.append(task_id)
            for area in _AREA_ORDER:
                owners[area].extend(hints)
    rows = () if not detected else tuple(
        _area_row(area, evidence[area], task_ids[area], owners[area], blocked)
        for area in _AREA_ORDER
    )
    return PlanWebhookDeliveryReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        webhook_task_ids=tuple(_dedupe(detected)),
        no_webhook_task_ids=tuple(_task_id(task, index) for index, task in enumerate(tasks, start=1) if _task_id(task, index) not in detected),
        gap_areas=tuple(row.area for row in rows if row.gaps),  # type: ignore[arg-type]
        summary=_area_summary(len(tasks), rows, detected),
    )


def _area_row(
    area: WebhookDeliveryReadinessArea,
    evidence: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
    blocked: bool,
) -> PlanWebhookDeliveryReadinessRow:
    evidence_tuple = tuple(_dedupe(evidence))
    gaps = () if evidence_tuple else (_AREA_GAPS[area],)
    readiness: WebhookDeliveryReadiness = "ready" if not gaps else ("blocked" if blocked else "partial")
    risk: WebhookDeliveryRisk = "low" if not gaps else ("high" if area in _HIGH_GAP_AREAS or blocked else "medium")
    score = 100 if not gaps else 0
    return PlanWebhookDeliveryReadinessRow(
        area=area,
        owner=next(iter(_dedupe(owners)), _DEFAULT_OWNERS[area]),
        evidence=evidence_tuple,
        gaps=gaps,
        readiness=readiness,
        readiness_score=score / 100,
        risk=risk,
        next_action="Ready for webhook delivery handoff." if not gaps else _ACTIONS[area],
        task_ids=tuple(_dedupe(task_ids)),
        score=score,
    )


def _area_summary(task_count: int, rows: Iterable[PlanWebhookDeliveryReadinessRow], ids: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    total = sum(row.score for row in row_list)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "area_count": len(row_list),
        "ready_area_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_area_count": sum(1 for row in row_list if row.gaps),
        "webhook_task_count": len(tuple(_dedupe(ids))),
        "score": round(total / len(row_list)) if row_list else 0,
        "readiness_counts": {key: sum(1 for row in row_list if row.readiness == key) for key in _READINESS_ORDER},
        "risk_counts": {key: sum(1 for row in row_list if row.risk == key) for key in _RISK_ORDER},
    }


def _owner_hints(task: Mapping[str, Any]) -> tuple[str, ...]:
    owners = list(_strings_from_value(task.get("owner_type")))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings_from_value(metadata.get(key)))
    return _dedupe(owners)


def _strings_from_value(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value.strip() else ()
    if isinstance(value, Mapping):
        return tuple(str(item) for item in value.values() if str(item).strip())
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value if str(item).strip())
    return (str(value),) if str(value).strip() else ()


__all__ = [
    "WebhookDeliveryReadiness",
    "WebhookDeliveryReadinessArea",
    "WebhookDeliveryRisk",
    "PlanWebhookDeliveryReadinessMatrix",
    "PlanWebhookDeliveryReadinessRow",
    "build_plan_webhook_delivery_readiness_matrix",
    "generate_plan_webhook_delivery_readiness_matrix",
    "analyze_plan_webhook_delivery_readiness_matrix",
    "derive_plan_webhook_delivery_readiness_matrix",
    "extract_plan_webhook_delivery_readiness_matrix",
    "summarize_plan_webhook_delivery_readiness_matrix",
    "plan_webhook_delivery_readiness_matrix_to_dict",
    "plan_webhook_delivery_readiness_matrix_to_dicts",
    "plan_webhook_delivery_readiness_matrix_to_markdown",
]
