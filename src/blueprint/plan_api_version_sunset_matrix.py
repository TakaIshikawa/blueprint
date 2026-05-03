"""Build plan-level API version sunset readiness matrices."""

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


ApiVersionSunsetReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiVersionSunsetReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_API_RE = re.compile(r"\b(?:api|endpoint|route|rest|graphql|webhook|sdk|v[0-9]+|version)\b", re.I)
_SUNSET_RE = re.compile(r"\b(?:sunset|deprecat(?:e|es|ed|ion)|retire|end[- ]of[- ]life|eol|remove v[0-9]+|version migration)\b", re.I)
_SURFACE_RE = re.compile(r"\b(?:endpoint|route|api surface|path|method|sdk|graphql|webhook|v[0-9]+)\b", re.I)
_OWNER_RE = re.compile(r"\b(?:owner|dri|responsible|team|lead|api platform|developer experience|dx)\b", re.I)
_CLIENT_RE = re.compile(r"\b(?:client|consumer|customer|partner|integration|sdk|downstream|usage|traffic|adoption)\b", re.I)
_WINDOW_RE = re.compile(r"\b(?:sunset date|deadline|migration window|compatibility window|grace period|overlap|notice period)\b", re.I)
_ROLLOUT_RE = re.compile(r"\b(?:rollout|sequence|phase|wave|order|cutover|redirect|block|rate limit|brownout)\b", re.I)
_VERIFICATION_RE = re.compile(r"\b(?:verify|verification|validate|validation|test|contract test|smoke|monitor|dashboard|alert|traffic)\b", re.I)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|restore|fallback|extend|unblock|disable enforcement|abort)\b", re.I)
_NOTICE_RE = re.compile(r"\b(?:notice|notify|communication|email|changelog|release notes|docs|developer portal|announcement)\b", re.I)
_AUDIT_RE = re.compile(r"\b(?:audit|evidence|log|ticket|approval|signoff|record|exception)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanApiVersionSunsetRow:
    """Readiness signals for one API version sunset task."""

    task_id: str
    title: str
    api_surface: str = "missing"
    sunset_owner: str = "missing"
    dependent_clients: str = "missing"
    compatibility_window: str = "missing"
    rollout_order: str = "missing"
    verification: str = "missing"
    rollback: str = "missing"
    customer_notice: str = "missing"
    audit_evidence: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiVersionSunsetReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "api_surface": self.api_surface,
            "sunset_owner": self.sunset_owner,
            "dependent_clients": self.dependent_clients,
            "compatibility_window": self.compatibility_window,
            "rollout_order": self.rollout_order,
            "verification": self.verification,
            "rollback": self.rollback,
            "customer_notice": self.customer_notice,
            "audit_evidence": self.audit_evidence,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanApiVersionSunsetMatrix:
    """Plan-level API version sunset readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanApiVersionSunsetRow, ...] = field(default_factory=tuple)
    sunset_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_sunset_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiVersionSunsetRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "sunset_task_ids": list(self.sunset_task_ids),
            "no_sunset_task_ids": list(self.no_sunset_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan API Version Sunset Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        if not self.rows:
            return "\n".join([title, "", "No API version sunset rows were inferred."])
        lines = [
            title,
            "",
            "| Task | Title | Surface | Owner | Clients | Window | Rollout | Verification | Rollback | Notice | Audit | Readiness | Gaps |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | {row.api_surface} | "
                f"{row.sunset_owner} | {row.dependent_clients} | {row.compatibility_window} | "
                f"{row.rollout_order} | {row.verification} | {row.rollback} | {row.customer_notice} | "
                f"{row.audit_evidence} | {row.readiness} | {_markdown_cell('; '.join(row.gaps) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_version_sunset_matrix(source: Any) -> PlanApiVersionSunsetMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiVersionSunsetRow] = []
    no_sunset_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_sunset_task_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], -len(row.gaps), row.task_id))
    result = tuple(rows)
    return PlanApiVersionSunsetMatrix(
        plan_id=plan_id,
        rows=result,
        sunset_task_ids=tuple(row.task_id for row in result),
        no_sunset_task_ids=tuple(no_sunset_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_api_version_sunset_matrix(source: Any) -> PlanApiVersionSunsetMatrix:
    return build_plan_api_version_sunset_matrix(source)


def analyze_plan_api_version_sunset_matrix(source: Any) -> PlanApiVersionSunsetMatrix:
    if isinstance(source, PlanApiVersionSunsetMatrix):
        return source
    return build_plan_api_version_sunset_matrix(source)


def derive_plan_api_version_sunset_matrix(source: Any) -> PlanApiVersionSunsetMatrix:
    return analyze_plan_api_version_sunset_matrix(source)


def extract_plan_api_version_sunset_matrix(source: Any) -> PlanApiVersionSunsetMatrix:
    return derive_plan_api_version_sunset_matrix(source)


def summarize_plan_api_version_sunset_matrix(
    source: PlanApiVersionSunsetMatrix | Iterable[PlanApiVersionSunsetRow] | Any,
) -> dict[str, Any] | PlanApiVersionSunsetMatrix:
    if isinstance(source, PlanApiVersionSunsetMatrix):
        return dict(source.summary)
    if isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)) or hasattr(source, "tasks") or hasattr(source, "title"):
        return build_plan_api_version_sunset_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_api_version_sunset_matrix_to_dict(matrix: PlanApiVersionSunsetMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_version_sunset_matrix_to_dict.__test__ = False


def plan_api_version_sunset_matrix_to_dicts(
    matrix: PlanApiVersionSunsetMatrix | Iterable[PlanApiVersionSunsetRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiVersionSunsetMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_version_sunset_matrix_to_dicts.__test__ = False


def plan_api_version_sunset_matrix_to_markdown(matrix: PlanApiVersionSunsetMatrix) -> str:
    return matrix.to_markdown()


plan_api_version_sunset_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanApiVersionSunsetRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_API_RE.search(context) and _SUNSET_RE.search(context)):
        return None
    statuses = {
        "api_surface": _status(_SURFACE_RE, texts),
        "sunset_owner": _status(_OWNER_RE, texts),
        "dependent_clients": _status(_CLIENT_RE, texts),
        "compatibility_window": _status(_WINDOW_RE, texts),
        "rollout_order": _status(_ROLLOUT_RE, texts),
        "verification": _status(_VERIFICATION_RE, texts),
        "rollback": _status(_ROLLBACK_RE, texts),
        "customer_notice": _status(_NOTICE_RE, texts),
        "audit_evidence": _status(_AUDIT_RE, texts),
    }
    labels = {
        "api_surface": "API surface",
        "sunset_owner": "sunset owner",
        "dependent_clients": "dependent clients",
        "compatibility_window": "compatibility or migration window",
        "rollout_order": "rollout order",
        "verification": "verification",
        "rollback": "rollback",
        "customer_notice": "customer notice",
        "audit_evidence": "audit evidence",
    }
    gaps = tuple(f"Missing {label}." for field, label in labels.items() if statuses[field] == "missing")
    readiness: ApiVersionSunsetReadiness = "ready"
    if statuses["sunset_owner"] == "missing" or statuses["verification"] == "missing" or statuses["customer_notice"] == "missing":
        readiness = "blocked"
    elif gaps:
        readiness = "partial"
    return PlanApiVersionSunsetRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _API_RE.search(text) or _SUNSET_RE.search(text))),
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[PlanApiVersionSunsetRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "sunset_task_count": len(row_list),
        "no_sunset_task_count": task_count - len(row_list),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
    }
