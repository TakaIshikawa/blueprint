"""Plan identity-mapping safeguards for SSO, IdP, SCIM, and enterprise login tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SsoMappingIdentitySurface = Literal[
    "sso",
    "saml",
    "oidc",
    "identity_provider",
    "scim",
    "group_claim",
    "role_mapping",
    "tenant_domain",
    "enterprise_login",
]
SsoMappingSafeguard = Literal[
    "claim_mapping_tests",
    "fallback_access",
    "admin_recovery",
    "tenant_isolation",
    "audit_logging",
    "documentation_updates",
]
SsoMappingRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[SsoMappingRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: tuple[SsoMappingIdentitySurface, ...] = (
    "sso",
    "saml",
    "oidc",
    "identity_provider",
    "scim",
    "group_claim",
    "role_mapping",
    "tenant_domain",
    "enterprise_login",
)
_SAFEGUARD_ORDER: tuple[SsoMappingSafeguard, ...] = (
    "claim_mapping_tests",
    "fallback_access",
    "admin_recovery",
    "tenant_isolation",
    "audit_logging",
    "documentation_updates",
)
_ELEVATED_MAPPING_SURFACES = {"group_claim", "role_mapping", "tenant_domain"}
_RESIDUAL_MAPPING_SURFACES = {"role_mapping", "tenant_domain"}

_PATH_SURFACE_PATTERNS: tuple[tuple[SsoMappingIdentitySurface, re.Pattern[str]], ...] = (
    ("sso", re.compile(r"(?:^|/)(?:sso|single[-_]?sign[-_]?on)(?:/|\.|_|-|$)", re.I)),
    ("saml", re.compile(r"(?:^|/)(?:saml|saml2|metadata[-_]?xml)(?:/|\.|_|-|$)", re.I)),
    ("oidc", re.compile(r"(?:^|/)(?:oidc|openid|oauth|oauth2)(?:/|\.|_|-|$)", re.I)),
    ("identity_provider", re.compile(r"(?:^|/)(?:idp|identity[-_]?providers?|auth[-_]?providers?)(?:/|\.|_|-|$)", re.I)),
    ("scim", re.compile(r"(?:^|/)(?:scim|provisioning|deprovisioning)(?:/|\.|_|-|$)", re.I)),
    ("group_claim", re.compile(r"(?:^|/)(?:group[-_]?claims?|claims?[-_]?mapping)(?:/|\.|_|-|$)", re.I)),
    ("role_mapping", re.compile(r"(?:^|/)(?:role[-_]?mappings?|rbac[-_]?mapping|claims?[-_]?roles?)(?:/|\.|_|-|$)", re.I)),
    ("tenant_domain", re.compile(r"(?:^|/)(?:tenant[-_]?domains?|domain[-_]?mapping|verified[-_]?domains?)(?:/|\.|_|-|$)", re.I)),
    ("enterprise_login", re.compile(r"(?:^|/)(?:enterprise[-_]?login|enterprise[-_]?auth)(?:/|\.|_|-|$)", re.I)),
)
_TEXT_SURFACE_PATTERNS: dict[SsoMappingIdentitySurface, re.Pattern[str]] = {
    "sso": re.compile(r"\b(?:sso|single sign[- ]on|single-sign-on|single sign on)\b", re.I),
    "saml": re.compile(r"\b(?:saml|saml2|saml assertion|saml response|saml metadata|metadata xml)\b", re.I),
    "oidc": re.compile(r"\b(?:oidc|openid connect|id token|oauth2?|authorization code flow)\b", re.I),
    "identity_provider": re.compile(r"\b(?:idp|identity provider|identity providers|okta|azure ad|entra id|onelogin|ping identity)\b", re.I),
    "scim": re.compile(r"\b(?:scim|provisioning|deprovisioning|user provisioning|group provisioning)\b", re.I),
    "group_claim": re.compile(r"\b(?:group claims?|groups? claim|claim groups?|groups? attribute|memberOf|group mapping)\b", re.I),
    "role_mapping": re.compile(r"\b(?:role mappings?|map roles?|rbac mapping|claims? to roles?|groups? to roles?|attribute mapping)\b", re.I),
    "tenant_domain": re.compile(r"\b(?:tenant domains?|domain mappings?|verified domains?|email domains?|domain based routing|home realm discovery)\b", re.I),
    "enterprise_login": re.compile(r"\b(?:enterprise login|enterprise auth|enterprise authentication|company login|organization login)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[SsoMappingSafeguard, re.Pattern[str]] = {
    "claim_mapping_tests": re.compile(
        r"\b(?:claim mapping tests?|mapping tests?|saml assertion tests?|id token tests?|"
        r"group claim tests?|role mapping tests?|fixture(?:s)? for claims?|negative claim tests?)\b",
        re.I,
    ),
    "fallback_access": re.compile(
        r"\b(?:fallback access|backup login|password fallback|non[- ]sso fallback|break[- ]glass|"
        r"local admin login|bypass sso|emergency login)\b",
        re.I,
    ),
    "admin_recovery": re.compile(
        r"\b(?:admin recovery|recover admin|recovery path|support recovery|account recovery|"
        r"restore admin access|unlock admins?|admin lockout)\b",
        re.I,
    ),
    "tenant_isolation": re.compile(
        r"\b(?:tenant isolation|cross[- ]tenant|wrong tenant|tenant boundary|domain ownership|"
        r"verified domains?|tenant scoped|workspace isolation|organization isolation)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit logs?|audit trail|security event|identity event|login event|"
        r"provisioning event|mapping change log)\b",
        re.I,
    ),
    "documentation_updates": re.compile(
        r"\b(?:documentation updates?|docs updates?|update docs|admin docs|runbook|setup guide|"
        r"migration guide|configuration guide|release notes)\b",
        re.I,
    ),
}
_SAFEGUARD_ACCEPTANCE_CRITERIA: dict[SsoMappingSafeguard, str] = {
    "claim_mapping_tests": "Add SAML/OIDC claim mapping tests for expected, missing, malformed, and multi-group claims.",
    "fallback_access": "Keep a tested fallback access path for users and admins when SSO mapping fails.",
    "admin_recovery": "Document and test admin recovery for lockout or incorrect IdP mapping.",
    "tenant_isolation": "Verify tenant/domain routing cannot grant access across tenant boundaries.",
    "audit_logging": "Record SSO, SCIM, claim, role, and tenant mapping changes in audit logs.",
    "documentation_updates": "Update admin setup and migration documentation for IdP, claim, role, and domain mapping changes.",
}
_LOGIN_COPY_ONLY_RE = re.compile(
    r"\b(?:copy|wording|label|microcopy|empty state|tooltip|button text|helper text|translation)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskSsoMappingImpactRecommendation:
    """Identity-mapping impact guidance for one SSO-related task."""

    task_id: str
    title: str
    impacted_identity_surfaces: tuple[SsoMappingIdentitySurface, ...] = field(default_factory=tuple)
    required_safeguards: tuple[SsoMappingSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SsoMappingSafeguard, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    risk_level: SsoMappingRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_identity_surfaces": list(self.impacted_identity_surfaces),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_follow_up_actions": list(self.recommended_follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskSsoMappingImpactPlan:
    """Plan-level SSO mapping impact recommendations."""

    plan_id: str | None = None
    recommendations: tuple[TaskSsoMappingImpactRecommendation, ...] = field(default_factory=tuple)
    sso_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskSsoMappingImpactRecommendation, ...]:
        """Compatibility view matching analyzers that expose rows as records."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
            "records": [record.to_dict() for record in self.records],
            "sso_task_ids": list(self.sso_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return impact recommendations as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render SSO mapping impact recommendations as deterministic Markdown."""
        title = "# Task SSO Mapping Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- SSO mapping task count: {self.summary.get('sso_task_count', 0)}",
            f"- Missing acceptance criteria count: {self.summary.get('missing_acceptance_criteria_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.recommendations:
            lines.extend(["", "No SSO mapping impact recommendations were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
                "| Task | Title | Risk | Identity Surfaces | Present Safeguards | Missing Acceptance Criteria | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{_markdown_cell(recommendation.title)} | "
                f"{recommendation.risk_level} | "
                f"{_markdown_cell(', '.join(recommendation.impacted_identity_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(recommendation.present_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_sso_mapping_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Build identity-mapping impact recommendations for SSO-related execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_recommendation(task, index) for index, task in enumerate(tasks, start=1)]
    recommendations = tuple(
        sorted(
            (recommendation for recommendation in candidates if recommendation is not None),
            key=lambda recommendation: (
                _RISK_ORDER[recommendation.risk_level],
                recommendation.task_id,
                recommendation.title.casefold(),
            ),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskSsoMappingImpactPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        sso_task_ids=tuple(recommendation.task_id for recommendation in recommendations),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(recommendations, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_sso_mapping_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Compatibility alias for building SSO mapping impact recommendations."""
    return build_task_sso_mapping_impact_plan(source)


def summarize_task_sso_mapping_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Compatibility alias for building SSO mapping impact recommendations."""
    return build_task_sso_mapping_impact_plan(source)


def extract_task_sso_mapping_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Compatibility alias for building SSO mapping impact recommendations."""
    return build_task_sso_mapping_impact_plan(source)


def generate_task_sso_mapping_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Compatibility alias for generating SSO mapping impact recommendations."""
    return build_task_sso_mapping_impact_plan(source)


def recommend_task_sso_mapping_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSsoMappingImpactPlan:
    """Compatibility alias for recommending SSO mapping impact safeguards."""
    return build_task_sso_mapping_impact_plan(source)


def task_sso_mapping_impact_plan_to_dict(result: TaskSsoMappingImpactPlan) -> dict[str, Any]:
    """Serialize an SSO mapping impact plan to a plain dictionary."""
    return result.to_dict()


task_sso_mapping_impact_plan_to_dict.__test__ = False


def task_sso_mapping_impact_plan_to_markdown(result: TaskSsoMappingImpactPlan) -> str:
    """Render an SSO mapping impact plan as Markdown."""
    return result.to_markdown()


task_sso_mapping_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[SsoMappingIdentitySurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SsoMappingSafeguard, ...] = field(default_factory=tuple)
    acceptance_safeguards: tuple[SsoMappingSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_recommendation(task: Mapping[str, Any], index: int) -> TaskSsoMappingImpactRecommendation | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None
    if signals.surfaces == ("enterprise_login",) and _login_copy_only_task(task):
        return None

    missing_safeguards = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.acceptance_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskSsoMappingImpactRecommendation(
        task_id=task_id,
        title=title,
        impacted_identity_surfaces=signals.surfaces,
        required_safeguards=_SAFEGUARD_ORDER,
        present_safeguards=signals.present_safeguards,
        missing_acceptance_criteria=tuple(_SAFEGUARD_ACCEPTANCE_CRITERIA[safeguard] for safeguard in missing_safeguards),
        risk_level=_risk_level(signals.surfaces, missing_safeguards),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.safeguard_evidence])),
        recommended_follow_up_actions=tuple(_SAFEGUARD_ACCEPTANCE_CRITERIA[safeguard] for safeguard in missing_safeguards),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[SsoMappingIdentitySurface] = set()
    safeguard_hits: set[SsoMappingSafeguard] = set()
    acceptance_hits: set[SsoMappingSafeguard] = set()
    surface_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    acceptance_texts: list[str] = []
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        acceptance_texts.append(text)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                acceptance_hits.add(safeguard)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    for text in acceptance_texts:
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        acceptance_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in acceptance_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_surfaces(path: str) -> set[SsoMappingIdentitySurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[SsoMappingIdentitySurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if name in {"sso.py", "sso.ts", "sso.tsx", "saml.py", "oidc.py", "scim.py"}:
        surfaces.add(name.split(".")[0])  # type: ignore[arg-type]
    return surfaces


def _risk_level(
    surfaces: tuple[SsoMappingIdentitySurface, ...],
    missing_safeguards: tuple[SsoMappingSafeguard, ...],
) -> SsoMappingRiskLevel:
    if any(surface in _ELEVATED_MAPPING_SURFACES for surface in surfaces) and missing_safeguards:
        return "high"
    if missing_safeguards:
        return "medium"
    if any(surface in _RESIDUAL_MAPPING_SURFACES for surface in surfaces):
        return "medium"
    return "low"


def _summary(
    recommendations: tuple[TaskSsoMappingImpactRecommendation, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "sso_task_count": len(recommendations),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_acceptance_criteria_count": sum(
            len(recommendation.missing_acceptance_criteria) for recommendation in recommendations
        ),
        "risk_counts": {
            risk: sum(1 for recommendation in recommendations if recommendation.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "surface_counts": {
            surface: sum(
                1 for recommendation in recommendations if surface in recommendation.impacted_identity_surfaces
            )
            for surface in sorted(
                {surface for recommendation in recommendations for surface in recommendation.impacted_identity_surfaces}
            )
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for recommendation in recommendations if safeguard in recommendation.present_safeguards)
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
    return any(pattern.search(value) for pattern in [*_TEXT_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _login_copy_only_task(task: Mapping[str, Any]) -> bool:
    combined = " ".join(text for _, text in _candidate_texts(task))
    return bool(_LOGIN_COPY_ONLY_RE.search(combined)) and not any(
        pattern.search(combined)
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items()
        if surface != "enterprise_login"
    )


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
    "SsoMappingIdentitySurface",
    "SsoMappingRiskLevel",
    "SsoMappingSafeguard",
    "TaskSsoMappingImpactPlan",
    "TaskSsoMappingImpactRecommendation",
    "analyze_task_sso_mapping_impact",
    "build_task_sso_mapping_impact_plan",
    "extract_task_sso_mapping_impact",
    "generate_task_sso_mapping_impact",
    "recommend_task_sso_mapping_impact",
    "summarize_task_sso_mapping_impact",
    "task_sso_mapping_impact_plan_to_dict",
    "task_sso_mapping_impact_plan_to_markdown",
]
