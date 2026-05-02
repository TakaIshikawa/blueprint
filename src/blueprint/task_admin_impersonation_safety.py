"""Plan safeguards for admin impersonation and delegated account access tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AdminImpersonationAccessSurface = Literal[
    "admin_console",
    "support_tooling",
    "customer_account",
    "authentication_session",
    "api",
    "data_export",
]
AdminImpersonationSafeguard = Literal[
    "scoped_permissions",
    "explicit_reason_capture",
    "user_visible_audit_trail",
    "session_time_limit",
    "approval_workflow",
    "sensitive_action_blocking",
    "post_session_review",
]
AdminImpersonationRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[AdminImpersonationRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: tuple[AdminImpersonationAccessSurface, ...] = (
    "admin_console",
    "support_tooling",
    "customer_account",
    "authentication_session",
    "api",
    "data_export",
)
_SAFEGUARD_ORDER: tuple[AdminImpersonationSafeguard, ...] = (
    "scoped_permissions",
    "explicit_reason_capture",
    "user_visible_audit_trail",
    "session_time_limit",
    "approval_workflow",
    "sensitive_action_blocking",
    "post_session_review",
)

_IMPERSONATION_RE = re.compile(
    r"\b(?:admin impersonation|impersonat(?:e|es|ing|ion)|login[- ]?as(?:[- ]a)?[- ]user|"
    r"log in as (?:a )?user|support login|masquerade|assume user|act as (?:a )?(?:user|customer)|"
    r"switch user|view as user|delegated access|delegate access|break[- ]glass|"
    r"emergency access|customer support account access|support access to customer accounts?)\b",
    re.I,
)
_SUPPORT_ACCOUNT_ACCESS_RE = re.compile(
    r"\b(?:support|helpdesk|customer success|support agent|csr|agent)\b.{0,80}"
    r"\b(?:access|open|view|enter|manage)\b.{0,80}\b(?:customer|user|account|tenant|workspace)s?\b|"
    r"\b(?:customer|user|account|tenant|workspace)s?\b.{0,80}\b(?:access|open|view|enter|manage)\b.{0,80}"
    r"\b(?:support|helpdesk|customer success|support agent|csr|agent)\b",
    re.I,
)
_BREAK_GLASS_RE = re.compile(r"\b(?:break[- ]glass|emergency access|incident access|privileged override)\b", re.I)

_SURFACE_PATTERNS: dict[AdminImpersonationAccessSurface, re.Pattern[str]] = {
    "admin_console": re.compile(r"\b(?:admin|administrator|ops console|backoffice|internal console)\b", re.I),
    "support_tooling": re.compile(r"\b(?:support|helpdesk|customer success|agent console|zendesk|intercom)\b", re.I),
    "customer_account": re.compile(
        r"\b(?:customer accounts?|user accounts?|account access|tenant|workspace|organization accounts?)\b",
        re.I,
    ),
    "authentication_session": re.compile(r"\b(?:login|log in|session|auth|authentication|sso|token|cookie)\b", re.I),
    "api": re.compile(r"\b(?:api|endpoint|route|controller|mutation|graphql|rest)\b", re.I),
    "data_export": re.compile(r"\b(?:export|download|csv|report|billing|payment|invoice|payout)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[AdminImpersonationSafeguard, re.Pattern[str]] = {
    "scoped_permissions": re.compile(
        r"\b(?:scoped permissions?|scope(?:d)? access|rbac|role[- ]based|least privilege|"
        r"permission gate|support role|read[- ]only)\b",
        re.I,
    ),
    "explicit_reason_capture": re.compile(
        r"\b(?:reason capture|capture (?:a )?reason|justification|required reason|ticket id|case id|"
        r"support ticket|reason code|access reason)\b",
        re.I,
    ),
    "user_visible_audit_trail": re.compile(
        r"\b(?:user[- ]visible audit|customer[- ]visible audit|audit trail|audit log|access log|"
        r"visible to (?:the )?(?:user|customer)|customer notification|user notification)\b",
        re.I,
    ),
    "session_time_limit": re.compile(
        r"\b(?:session time limit|time[- ]boxed|timeboxed|expires?|expiry|ttl|timeout|max duration|"
        r"limited duration|auto[- ]expire|session cap)\b",
        re.I,
    ),
    "approval_workflow": re.compile(
        r"\b(?:approval workflow|approval required|manager approval|security approval|two[- ]person|"
        r"four[- ]eyes|peer approval|approved before|access request)\b",
        re.I,
    ),
    "sensitive_action_blocking": re.compile(
        r"\b(?:block(?:s|ed|ing)? sensitive|sensitive[- ]action block|disable (?:billing|payment|password|"
        r"email|export|delete|write)|read[- ]only|prevent (?:destructive|write|billing|payment|password|export)|"
        r"no (?:destructive|write|billing|payment|password|export) actions?)\b",
        re.I,
    ),
    "post_session_review": re.compile(
        r"\b(?:post[- ]session review|after[- ]action review|access review|session review|review queue|"
        r"supervisor review|periodic review|review completed sessions?)\b",
        re.I,
    ),
}
_SAFEGUARD_ACCEPTANCE_CRITERIA: dict[AdminImpersonationSafeguard, str] = {
    "scoped_permissions": "Limit impersonation to explicitly scoped support roles and permitted account surfaces.",
    "explicit_reason_capture": "Require a support case, ticket, or reason before an impersonation session starts.",
    "user_visible_audit_trail": "Record impersonation activity in an audit trail visible to authorized customer or user viewers.",
    "session_time_limit": "Automatically expire impersonation sessions after a short configured duration.",
    "approval_workflow": "Require approval for privileged, break-glass, or unusually broad impersonation access.",
    "sensitive_action_blocking": "Block sensitive changes such as billing, credentials, exports, destructive writes, and permission edits.",
    "post_session_review": "Route completed impersonation sessions to review for misuse or policy exceptions.",
}


@dataclass(frozen=True, slots=True)
class TaskAdminImpersonationSafetyRecommendation:
    """Safeguard recommendations for one impersonation or delegated-access task."""

    task_id: str
    title: str
    access_surfaces: tuple[AdminImpersonationAccessSurface, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[AdminImpersonationSafeguard, ...] = field(default_factory=tuple)
    risk_level: AdminImpersonationRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "access_surfaces": list(self.access_surfaces),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_acceptance_criteria": list(self.recommended_acceptance_criteria),
        }


@dataclass(frozen=True, slots=True)
class TaskAdminImpersonationSafetyPlan:
    """Plan-level admin impersonation safety recommendations."""

    plan_id: str | None = None
    recommendations: tuple[TaskAdminImpersonationSafetyRecommendation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskAdminImpersonationSafetyRecommendation, ...]:
        """Compatibility view matching analyzers that expose rows as records."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendations as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render the safety plan as deterministic Markdown."""
        title = "# Task Admin Impersonation Safety Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impersonation task count: {self.summary.get('impersonation_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.recommendations:
            lines.extend(["", "No admin impersonation or delegated-access tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
                "| Task | Title | Risk | Access Surfaces | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{_markdown_cell(recommendation.title)} | "
                f"{recommendation.risk_level} | "
                f"{_markdown_cell('; '.join(recommendation.access_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_admin_impersonation_safety_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> TaskAdminImpersonationSafetyPlan:
    """Build safeguard recommendations for impersonation and delegated-access tasks."""
    plan_id, plan_context, tasks = _source_payload(source)
    recommendations = tuple(
        sorted(
            (
                recommendation
                for index, task in enumerate(tasks, start=1)
                if (recommendation := _recommendation_for_task(task, index, plan_context)) is not None
            ),
            key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id, item.title.casefold()),
        )
    )
    return TaskAdminImpersonationSafetyPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        summary=_summary(recommendations, task_count=len(tasks)),
    )


def extract_task_admin_impersonation_safety_recommendations(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | tuple[TaskAdminImpersonationSafetyRecommendation, ...]
        | list[TaskAdminImpersonationSafetyRecommendation]
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> tuple[TaskAdminImpersonationSafetyRecommendation, ...]:
    """Return impersonation safety recommendations for relevant execution tasks."""
    if isinstance(source, (list, tuple)) and all(
        isinstance(item, TaskAdminImpersonationSafetyRecommendation) for item in source
    ):
        return tuple(source)
    return build_task_admin_impersonation_safety_plan(source).recommendations


def summarize_task_admin_impersonation_safety(
    source_or_plan: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskAdminImpersonationSafetyPlan
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> TaskAdminImpersonationSafetyPlan:
    """Build the safety plan, accepting an existing plan unchanged."""
    if isinstance(source_or_plan, TaskAdminImpersonationSafetyPlan):
        return source_or_plan
    return build_task_admin_impersonation_safety_plan(source_or_plan)


def task_admin_impersonation_safety_plan_to_dict(
    plan: TaskAdminImpersonationSafetyPlan,
) -> dict[str, Any]:
    """Serialize an admin impersonation safety plan to a plain dictionary."""
    return plan.to_dict()


task_admin_impersonation_safety_plan_to_dict.__test__ = False


def task_admin_impersonation_safety_plan_to_dicts(
    recommendations: (
        TaskAdminImpersonationSafetyPlan
        | tuple[TaskAdminImpersonationSafetyRecommendation, ...]
        | list[TaskAdminImpersonationSafetyRecommendation]
    ),
) -> list[dict[str, Any]]:
    """Serialize admin impersonation safety recommendations to dictionaries."""
    if isinstance(recommendations, TaskAdminImpersonationSafetyPlan):
        return recommendations.to_dicts()
    return [recommendation.to_dict() for recommendation in recommendations]


task_admin_impersonation_safety_plan_to_dicts.__test__ = False


def task_admin_impersonation_safety_plan_to_markdown(plan: TaskAdminImpersonationSafetyPlan) -> str:
    """Render an admin impersonation safety plan as Markdown."""
    return plan.to_markdown()


task_admin_impersonation_safety_plan_to_markdown.__test__ = False


def _recommendation_for_task(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> TaskAdminImpersonationSafetyRecommendation | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    task_context = [*_candidate_texts(task), *_metadata_texts(task.get("metadata"))]
    context = [*plan_context, *task_context]

    signal_evidence: list[str] = []
    surface_hits: dict[AdminImpersonationAccessSurface, list[str]] = {surface: [] for surface in _SURFACE_ORDER}
    safeguard_hits: dict[AdminImpersonationSafeguard, list[str]] = {safeguard: [] for safeguard in _SAFEGUARD_ORDER}
    break_glass = False

    for source_field, text in task_context:
        snippet = _evidence_snippet(source_field, text)
        if _IMPERSONATION_RE.search(text) or _SUPPORT_ACCOUNT_ACCESS_RE.search(text):
            signal_evidence.append(snippet)
        if _BREAK_GLASS_RE.search(text):
            break_glass = True

    for source_field, text in context:
        snippet = _evidence_snippet(source_field, text)
        if _BREAK_GLASS_RE.search(text):
            break_glass = True
        for surface, pattern in _SURFACE_PATTERNS.items():
            if pattern.search(text):
                surface_hits[surface].append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits[safeguard].append(snippet)

    if not signal_evidence:
        return None

    access_surfaces = tuple(surface for surface in _SURFACE_ORDER if _dedupe(surface_hits[surface]))
    if not access_surfaces:
        access_surfaces = ("admin_console",)
    present_safeguards = {
        safeguard for safeguard, evidence in safeguard_hits.items() if _dedupe(evidence)
    }
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present_safeguards)
    return TaskAdminImpersonationSafetyRecommendation(
        task_id=task_id,
        title=title,
        access_surfaces=access_surfaces,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(
            missing_safeguards,
            present_safeguards=present_safeguards,
            access_surfaces=access_surfaces,
            break_glass=break_glass,
        ),
        evidence=tuple(
            _dedupe(
                [
                    *signal_evidence,
                    *sum(surface_hits.values(), []),
                    *sum(safeguard_hits.values(), []),
                ]
            )
        ),
        recommended_acceptance_criteria=tuple(
            _SAFEGUARD_ACCEPTANCE_CRITERIA[safeguard] for safeguard in missing_safeguards
        ),
    )


def _risk_level(
    missing_safeguards: tuple[AdminImpersonationSafeguard, ...],
    *,
    present_safeguards: set[AdminImpersonationSafeguard],
    access_surfaces: tuple[AdminImpersonationAccessSurface, ...],
    break_glass: bool,
) -> AdminImpersonationRiskLevel:
    reducers = {"sensitive_action_blocking", "user_visible_audit_trail"}
    if not missing_safeguards:
        return "low"
    if reducers <= present_safeguards and len(missing_safeguards) <= 2:
        return "low"
    if reducers <= present_safeguards:
        return "medium"
    if break_glass and not {"approval_workflow", "session_time_limit", "user_visible_audit_trail"} <= present_safeguards:
        return "high"
    if {"support_tooling", "customer_account"}.issubset(access_surfaces) and len(missing_safeguards) >= 3:
        return "high"
    if len(missing_safeguards) >= 4:
        return "high"
    return "medium"


def _summary(
    recommendations: tuple[TaskAdminImpersonationSafetyRecommendation, ...],
    *,
    task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impersonation_task_count": len(recommendations),
        "missing_safeguard_count": sum(len(item.missing_safeguards) for item in recommendations),
        "at_risk_task_count": sum(1 for item in recommendations if item.risk_level != "low"),
        "risk_counts": {
            level: sum(1 for item in recommendations if item.risk_level == level)
            for level in _RISK_ORDER
        },
        "access_surface_counts": {
            surface: sum(1 for item in recommendations if surface in item.access_surfaces)
            for surface in _SURFACE_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for item in recommendations if safeguard in item.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(source.id),
            tuple(_plan_context(plan)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                tuple(_plan_context(plan)),
                _task_payloads(plan.get("tasks")),
            )
        return None, (), [dict(source)]

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, (), tasks


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
    return tasks


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "test_strategy",
        "handoff_prompt",
        "status",
        "target_engine",
        "project_type",
    ):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    texts.extend(_metadata_texts(plan.get("metadata"), "plan.metadata"))
    for index, milestone in enumerate(_items(plan.get("milestones"))):
        if isinstance(milestone, Mapping):
            for field_name, value in sorted(milestone.items(), key=lambda item: str(item[0])):
                for text in _strings(value):
                    texts.append((f"plan.milestones[{index}].{field_name}", text))
        elif text := _optional_text(milestone):
            texts.append((f"plan.milestones[{index}]", text))
    return texts


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for index, path in enumerate(_strings(task.get("files_or_modules"))):
        texts.append((f"files_or_modules[{index}]", path))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if (
                _IMPERSONATION_RE.search(key_text)
                or _SUPPORT_ACCOUNT_ACCESS_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _SURFACE_PATTERNS.values())
                or any(pattern.search(key_text) for pattern in _SAFEGUARD_PATTERNS.values())
            ):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts = []
        for index, item in enumerate(_items(value)):
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
        for key, child in sorted(value.items(), key=lambda item: str(item[0])):
            key_text = _optional_text(key)
            if key_text:
                strings.append(key_text.replace("_", " "))
            strings.extend(_strings(child))
        return strings
    if isinstance(value, (list, tuple, set)):
        strings = []
        for item in _items(value):
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, set):
        return sorted(value, key=lambda item: str(item))
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|")


__all__ = [
    "AdminImpersonationAccessSurface",
    "AdminImpersonationRiskLevel",
    "AdminImpersonationSafeguard",
    "TaskAdminImpersonationSafetyPlan",
    "TaskAdminImpersonationSafetyRecommendation",
    "build_task_admin_impersonation_safety_plan",
    "extract_task_admin_impersonation_safety_recommendations",
    "summarize_task_admin_impersonation_safety",
    "task_admin_impersonation_safety_plan_to_dict",
    "task_admin_impersonation_safety_plan_to_dicts",
    "task_admin_impersonation_safety_plan_to_markdown",
]
