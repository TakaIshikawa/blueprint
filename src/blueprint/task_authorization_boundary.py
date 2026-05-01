"""Plan authorization boundary checks for sensitive execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AuthorizationBoundaryConcern = Literal[
    "authentication",
    "authorization",
    "rbac",
    "ownership",
    "tenant_isolation",
    "admin_access",
    "impersonation",
    "permission_migration",
    "auth_policy",
]

_SPACE_RE = re.compile(r"\s+")
_CONCERN_ORDER: dict[AuthorizationBoundaryConcern, int] = {
    "authentication": 0,
    "authorization": 1,
    "rbac": 2,
    "ownership": 3,
    "tenant_isolation": 4,
    "admin_access": 5,
    "impersonation": 6,
    "permission_migration": 7,
    "auth_policy": 8,
}
_CONCERN_PATTERNS: dict[AuthorizationBoundaryConcern, re.Pattern[str]] = {
    "authentication": re.compile(
        r"\b(?:authn|authentication|login|sign[- ]?in|session|oauth|sso|mfa|2fa)\b",
        re.I,
    ),
    "authorization": re.compile(
        r"\b(?:authorization|authorisation|authorize|authorise|permissions?|privileges?|"
        r"access control|acl|entitlement|scope check|policy check)\b",
        re.I,
    ),
    "rbac": re.compile(r"\b(?:rbac|role[- ]based|roles?|role matrix|role check)\b", re.I),
    "ownership": re.compile(
        r"\b(?:ownership|owner check|resource owner|created by|owned by|owner[-_ ]?id|"
        r"belongs to|membership)\b",
        re.I,
    ),
    "tenant_isolation": re.compile(
        r"\b(?:tenant isolation|tenant boundary|tenant[-_ ]?id|multi[- ]?tenant|"
        r"cross[- ]?tenant|workspace boundary|organization boundary|org[-_ ]?id)\b",
        re.I,
    ),
    "admin_access": re.compile(
        r"\b(?:admin(?:istrator)?(?: access| role| only| portal| console)?|superuser|"
        r"staff access|support access|privileged access|admin-only)\b",
        re.I,
    ),
    "impersonation": re.compile(
        r"\b(?:impersonat\w+|act as user|admin as user|sudo user|break[- ]?glass|"
        r"delegated admin)\b",
        re.I,
    ),
    "permission_migration": re.compile(
        r"\b(?:permission migration|migrate permissions?|permission backfill|role migration|"
        r"rbac migration|acl migration|entitlement migration|access migration)\b",
        re.I,
    ),
    "auth_policy": re.compile(
        r"\b(?:auth policy|authorization policy|permission policy|access policy|policy engine|"
        r"opa|cedar|casbin|rego|deny by default|allow by default)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[AuthorizationBoundaryConcern, re.Pattern[str]] = {
    "authentication": re.compile(r"(?:^|/)(?:auth|authentication|login|sessions?|oauth)(?:/|$)", re.I),
    "authorization": re.compile(r"(?:^|/)(?:authz|authorization|permissions?|access)(?:/|$)", re.I),
    "rbac": re.compile(r"(?:^|/)(?:rbac|roles?)(?:/|$)", re.I),
    "tenant_isolation": re.compile(r"(?:^|/)(?:tenants?|workspaces?|organizations?|orgs?)(?:/|$)", re.I),
    "admin_access": re.compile(r"(?:^|/)(?:admin|staff|support)(?:/|$)", re.I),
    "impersonation": re.compile(r"(?:^|/)(?:impersonation|break[-_]?glass)(?:/|$)", re.I),
}
_NEGATIVE_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskAuthorizationBoundaryPlan:
    """Authorization boundary plan for one affected execution task."""

    task_id: str
    title: str
    concerns: tuple[AuthorizationBoundaryConcern, ...] = field(default_factory=tuple)
    boundary_checks: tuple[str, ...] = field(default_factory=tuple)
    reviewer_roles: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    escalation_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "concerns": list(self.concerns),
            "boundary_checks": list(self.boundary_checks),
            "reviewer_roles": list(self.reviewer_roles),
            "evidence": list(self.evidence),
            "escalation_reasons": list(self.escalation_reasons),
        }


def generate_task_authorization_boundary_plans(
    plan: Mapping[str, Any] | ExecutionPlan | None,
) -> list[TaskAuthorizationBoundaryPlan]:
    """Return authorization boundary plans for affected tasks in an execution plan."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    records: list[TaskAuthorizationBoundaryPlan] = []
    for index, task in enumerate(tasks, start=1):
        if record := _record_for_task(task, index):
            records.append(record)
    return sorted(records, key=lambda record: (record.task_id, record.title))


def task_authorization_boundary_plans_to_dicts(
    plans: tuple[TaskAuthorizationBoundaryPlan, ...] | list[TaskAuthorizationBoundaryPlan],
) -> list[dict[str, Any]]:
    """Serialize task authorization boundary plans to dictionaries."""
    return [plan.to_dict() for plan in plans]


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskAuthorizationBoundaryPlan | None:
    text_items = _task_texts(task)
    concerns: set[AuthorizationBoundaryConcern] = set()
    evidence: list[str] = []
    for source_field, text in text_items:
        if source_field == "files_or_modules":
            matches = [
                concern for concern, pattern in _PATH_PATTERNS.items() if pattern.search(text)
            ]
        else:
            matches = [
                concern for concern, pattern in _CONCERN_PATTERNS.items() if pattern.search(text)
            ]
        for concern in matches:
            concerns.add(concern)
            evidence.append(_evidence_snippet(source_field, text))

    if not concerns or _only_low_signal_documentation(text_items):
        return None

    ordered_concerns = tuple(sorted(concerns, key=lambda concern: _CONCERN_ORDER[concern]))
    return TaskAuthorizationBoundaryPlan(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or f"Task {index}",
        concerns=ordered_concerns,
        boundary_checks=_boundary_checks(ordered_concerns),
        reviewer_roles=_reviewer_roles(ordered_concerns),
        evidence=tuple(_dedupe(evidence)),
        escalation_reasons=_escalation_reasons(ordered_concerns),
    )


def _boundary_checks(
    concerns: tuple[AuthorizationBoundaryConcern, ...],
) -> tuple[str, ...]:
    checks: list[str] = [
        "Verify server-side authorization is enforced before every protected read, write, export, and background job.",
        "Add negative tests that prove unauthorized, cross-role, and cross-boundary actors are denied.",
    ]
    concern_checks: dict[AuthorizationBoundaryConcern, tuple[str, ...]] = {
        "authentication": (
            "Confirm unauthenticated, expired-session, and wrong-identity requests cannot reach protected behavior.",
        ),
        "authorization": (
            "Map each changed action to explicit allow and deny decisions with deny-by-default fallback.",
        ),
        "rbac": (
            "Exercise the role matrix for allowed, denied, inherited, and removed-role cases.",
        ),
        "ownership": (
            "Check ownership and membership using server-trusted identifiers, not client-supplied owner fields.",
        ),
        "tenant_isolation": (
            "Test at least two tenants or workspaces and prove list, detail, mutation, search, export, and job paths stay scoped.",
        ),
        "admin_access": (
            "Verify admin-only paths reject non-admins and log privileged actions with actor, target, and reason.",
        ),
        "impersonation": (
            "Require explicit impersonation approval or reason capture, visible session state, scoped permissions, expiry, and audit events.",
        ),
        "permission_migration": (
            "Reconcile permissions before and after migration, including removed access, inherited roles, and rollback behavior.",
        ),
        "auth_policy": (
            "Review policy changes for default decision, precedence, wildcard grants, and backwards compatibility.",
        ),
    }
    for concern in concerns:
        checks.extend(concern_checks[concern])
    return tuple(_dedupe(checks))


def _reviewer_roles(
    concerns: tuple[AuthorizationBoundaryConcern, ...],
) -> tuple[str, ...]:
    roles = ["security reviewer", "backend owner"]
    if any(concern in concerns for concern in ("rbac", "authorization", "auth_policy")):
        roles.append("authorization domain owner")
    if "tenant_isolation" in concerns:
        roles.append("multi-tenant architecture reviewer")
    if any(concern in concerns for concern in ("admin_access", "impersonation")):
        roles.append("trust and safety reviewer")
    if "permission_migration" in concerns:
        roles.append("data migration owner")
    return tuple(_dedupe(roles))


def _escalation_reasons(
    concerns: tuple[AuthorizationBoundaryConcern, ...],
) -> tuple[str, ...]:
    reasons = ["Authorization-sensitive task requires explicit boundary review before autonomous execution."]
    if any(concern in concerns for concern in ("rbac", "authorization", "auth_policy")):
        reasons.append("Permission semantics or policy behavior can grant unintended access.")
    if "tenant_isolation" in concerns:
        reasons.append("Tenant or workspace boundary changes can expose one customer's data to another.")
    if any(concern in concerns for concern in ("admin_access", "impersonation")):
        reasons.append("Privileged or impersonated access needs independent approval and audit coverage.")
    if "permission_migration" in concerns:
        reasons.append("Permission migration can silently preserve, remove, or broaden access at scale.")
    return tuple(_dedupe(reasons))


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any]:
    if plan is None:
        return {}
    if isinstance(plan, ExecutionPlan):
        return plan.model_dump(mode="python")
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if not isinstance(plan, Mapping):
        return {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            value = item.model_dump(mode="python")
            if isinstance(value, Mapping):
                tasks.append(dict(value))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
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
    for field_name in ("files_or_modules", "acceptance_criteria", "depends_on", "tags", "labels", "risks"):
        for text in _strings(task.get(field_name)):
            texts.append((field_name, text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    if not isinstance(value, Mapping):
        return texts
    for key in sorted(value):
        source_field = f"{prefix}.{key}"
        item = value[key]
        if isinstance(item, Mapping):
            texts.extend(_metadata_texts(item, source_field))
        else:
            for text in _strings(item):
                texts.append((source_field, text))
    return texts


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, (int, float, bool)):
        return [str(value)]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return []


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _clean_text(value)
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    text = _clean_text(text)
    if len(text) > 180:
        text = f"{text[:177].rstrip()}..."
    return f"{source_field}: {text}"


def _only_low_signal_documentation(text_items: list[tuple[str, str]]) -> bool:
    combined = " ".join(text for _, text in text_items)
    return bool(_NEGATIVE_RE.search(combined)) and not re.search(
        r"\b(?:implement|enforce|change|migrate|add|update|delete|create|grant|revoke|admin-only|"
        r"impersonat\w+|tenant boundary|authorization policy)\b",
        combined,
        re.I,
    )


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result
