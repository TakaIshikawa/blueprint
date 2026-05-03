"""Plan API SSO integration readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SSOSignal = Literal[
    "sso_protocol_support",
    "identity_provider_integration",
    "scim_provisioning",
    "jit_provisioning",
    "sso_attribute_mapping",
    "multi_idp_support",
    "sso_session_management",
    "sso_testing",
]
SSOSafeguard = Literal[
    "sso_library_integration",
    "saml_oidc_configuration",
    "idp_metadata_management",
    "scim_endpoint_implementation",
    "attribute_mapping_configuration",
    "multi_idp_routing_logic",
    "sso_session_handling",
    "sso_testing_coverage",
]
SSOReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[SSOReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[SSOSignal, ...] = (
    "sso_protocol_support",
    "identity_provider_integration",
    "scim_provisioning",
    "jit_provisioning",
    "sso_attribute_mapping",
    "multi_idp_support",
    "sso_session_management",
    "sso_testing",
)
_SAFEGUARD_ORDER: tuple[SSOSafeguard, ...] = (
    "sso_library_integration",
    "saml_oidc_configuration",
    "idp_metadata_management",
    "scim_endpoint_implementation",
    "attribute_mapping_configuration",
    "multi_idp_routing_logic",
    "sso_session_handling",
    "sso_testing_coverage",
)
_SIGNAL_PATTERNS: dict[SSOSignal, re.Pattern[str]] = {
    "sso_protocol_support": re.compile(
        r"\b(?:sso protocol|saml 2\.0|saml|oauth 2\.0|oauth|openid connect|oidc|federated protocol)\b",
        re.I,
    ),
    "identity_provider_integration": re.compile(
        r"\b(?:idp integration|identity provider|okta|azure ad|auth0|google workspace|ping|onelogin)\b",
        re.I,
    ),
    "scim_provisioning": re.compile(
        r"\b(?:scim|scim 2\.0|user provisioning|deprovisioning|scim endpoint|provision user)\b",
        re.I,
    ),
    "jit_provisioning": re.compile(
        r"\b(?:just-?in-?time|jit provisioning|auto-?create user|on-?demand provisioning)\b",
        re.I,
    ),
    "sso_attribute_mapping": re.compile(
        r"\b(?:attribute mapping|claim mapping|saml attribute|oidc claim|profile mapping)\b",
        re.I,
    ),
    "multi_idp_support": re.compile(
        r"\b(?:multi[- ]?idp|multiple identity provider|idp selection|home realm discovery)\b",
        re.I,
    ),
    "sso_session_management": re.compile(
        r"\b(?:sso session|session management|single logout|slo|session timeout|session revocation)\b",
        re.I,
    ),
    "sso_testing": re.compile(
        r"\b(?:sso test|test sso|mock idp|saml test|oauth test|oidc test)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[SSOSignal, re.Pattern[str]] = {
    "sso_protocol_support": re.compile(r"sso|saml|oauth|oidc|protocol", re.I),
    "identity_provider_integration": re.compile(r"idp|identity|provider|okta|azure", re.I),
    "scim_provisioning": re.compile(r"scim|provision|user[_-]?lifecycle", re.I),
    "jit_provisioning": re.compile(r"jit|just[_-]?in[_-]?time|auto[_-]?create", re.I),
    "sso_attribute_mapping": re.compile(r"attribute|claim|mapping|profile", re.I),
    "multi_idp_support": re.compile(r"multi[_-]?idp|routing|realm", re.I),
    "sso_session_management": re.compile(r"session|logout|slo|timeout", re.I),
    "sso_testing": re.compile(r"sso[_-]?test|mock[_-]?idp", re.I),
}
_SAFEGUARD_PATTERNS: dict[SSOSafeguard, re.Pattern[str]] = {
    "sso_library_integration": re.compile(
        r"\b(?:(?:sso|saml|oauth|oidc).{0,80}(?:library|package|sdk|integration|install|dependency)|"
        r"(?:library|package|sdk|integration|install|dependency).{0,80}(?:sso|saml|oauth|oidc)|"
        r"passport|node-saml|omniauth|authlib)\b",
        re.I,
    ),
    "saml_oidc_configuration": re.compile(
        r"\b(?:(?:saml|oidc|oauth).{0,80}(?:config|configuration|setup|settings?|environment|env)|"
        r"(?:config|configuration|setup|settings?).{0,80}(?:saml|oidc|oauth)|"
        r"acs url|entity id|callback url|redirect uri)\b",
        re.I,
    ),
    "idp_metadata_management": re.compile(
        r"\b(?:(?:idp|identity provider).{0,80}(?:metadata|certificate|cert|signing|key)|"
        r"(?:metadata|certificate|cert|signing|key).{0,80}(?:idp|identity provider)|"
        r"metadata url|metadata xml|x509 certificate)\b",
        re.I,
    ),
    "scim_endpoint_implementation": re.compile(
        r"\b(?:scim.{0,80}(?:endpoint|route|handler|controller|implementation|code|logic)|"
        r"(?:endpoint|route|handler|controller|implementation|code|logic).{0,80}scim|"
        r"/scim/v2/users|/scim/v2/groups)\b",
        re.I,
    ),
    "attribute_mapping_configuration": re.compile(
        r"\b(?:(?:attribute|claim).{0,80}(?:mapping|map|config|configuration|transform|normalization)|"
        r"(?:mapping|map|config|configuration|transform|normalization).{0,80}(?:attribute|claim)|"
        r"attribute map|claim map)\b",
        re.I,
    ),
    "multi_idp_routing_logic": re.compile(
        r"\b(?:(?:multi[- ]?idp|idp selection).{0,80}(?:routing|route|logic|handler|selection)|"
        r"(?:routing|route|logic|handler|selection).{0,80}(?:multi[- ]?idp|idp selection)|"
        r"home realm discovery|idp routing)\b",
        re.I,
    ),
    "sso_session_handling": re.compile(
        r"\b(?:(?:sso|saml).{0,80}(?:session|logout|slo|timeout|expiry|revocation|management)|"
        r"(?:session|logout|slo|timeout|expiry|revocation|management).{0,80}(?:sso|saml)|"
        r"single logout|session management)\b",
        re.I,
    ),
    "sso_testing_coverage": re.compile(
        r"\b(?:(?:sso|saml|oauth|oidc).{0,80}(?:test|tests|testing|spec|coverage|scenario|case)|"
        r"(?:test|tests|testing|spec|coverage|scenario|case).{0,80}(?:sso|saml|oauth|oidc)|"
        r"mock idp|test idp|sso tests?)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:sso|saml|oauth|oidc|identity provider)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[SSOSafeguard, str] = {
    "sso_library_integration": "Integrate SSO library (e.g., Passport.js, OmniAuth, AuthLib) for SAML/OAuth/OIDC protocol support.",
    "saml_oidc_configuration": "Configure SAML/OIDC settings including ACS URL, entity ID, callback URL, and redirect URI.",
    "idp_metadata_management": "Implement IDP metadata management including certificate/key handling and metadata URL configuration.",
    "scim_endpoint_implementation": "Implement SCIM v2 endpoints (/scim/v2/users, /scim/v2/groups) for user provisioning/deprovisioning.",
    "attribute_mapping_configuration": "Configure attribute/claim mapping to transform IDP assertions to application user profiles.",
    "multi_idp_routing_logic": "Implement multi-IDP routing logic with IDP selection and home realm discovery.",
    "sso_session_handling": "Implement SSO session management including single logout (SLO), timeout, and session revocation.",
    "sso_testing_coverage": "Add SSO integration tests with mock IDP for SAML/OAuth/OIDC flow validation.",
}


@dataclass(frozen=True, slots=True)
class TaskApiSSOIntegrationReadinessFinding:
    """API SSO integration readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[SSOSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SSOSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SSOSafeguard, ...] = field(default_factory=tuple)
    readiness: SSOReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskApiSSOIntegrationReadinessPlan:
    """Plan-level API SSO integration readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiSSOIntegrationReadinessFinding, ...] = field(default_factory=tuple)
    sso_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiSSOIntegrationReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "sso_task_ids": list(self.sso_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }


def plan_task_api_sso_integration_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiSSOIntegrationReadinessPlan:
    """Generate API SSO integration readiness safeguards for all tasks in a plan."""
    plan_id, tasks = _plan_payload(plan)
    findings: list[TaskApiSSOIntegrationReadinessFinding] = []
    sso_task_ids: list[str] = []
    not_applicable_task_ids: list[str] = []

    for task in tasks:
        task_id, task_payload = _task_payload(task)
        if not task_id:
            continue

        signals = _detect_signals(task_payload)
        if _has_no_impact(task_payload) or not signals:
            not_applicable_task_ids.append(task_id)
            continue

        sso_task_ids.append(task_id)
        safeguards = _detect_safeguards(task_payload, signals)
        missing = tuple(s for s in _expected_safeguards(signals) if s not in safeguards)
        readiness = _assess_readiness(signals, safeguards, missing)
        evidence = _collect_evidence(task_payload, signals, safeguards)
        remediations = tuple(_REMEDIATIONS[s] for s in missing)

        findings.append(
            TaskApiSSOIntegrationReadinessFinding(
                task_id=task_id,
                title=_task_title(task_payload),
                detected_signals=signals,
                present_safeguards=safeguards,
                missing_safeguards=missing,
                readiness=readiness,
                evidence=evidence,
                actionable_remediations=remediations,
            )
        )

    return TaskApiSSOIntegrationReadinessPlan(
        plan_id=plan_id,
        findings=tuple(findings),
        sso_task_ids=tuple(sso_task_ids),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_summary(findings, sso_task_ids, not_applicable_task_ids),
    )


def _plan_payload(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[str | None, list[Any]]:
    if isinstance(plan, ExecutionPlan):
        return plan.id, list(plan.tasks)
    if isinstance(plan, Mapping):
        try:
            validated = ExecutionPlan.model_validate(plan)
            return validated.id, list(validated.tasks)
        except (TypeError, ValueError, ValidationError):
            pass
        return plan.get("id"), plan.get("tasks", [])
    if hasattr(plan, "id") and hasattr(plan, "tasks"):
        return getattr(plan, "id", None), list(getattr(plan, "tasks", []))
    return None, []


def _task_payload(task: ExecutionTask | Mapping[str, Any] | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(task, ExecutionTask):
        return task.id, dict(task.model_dump(mode="python"))
    if isinstance(task, Mapping):
        try:
            validated = ExecutionTask.model_validate(task)
            return validated.id, dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return task.get("id"), dict(task)
    if hasattr(task, "id"):
        return getattr(task, "id", None), _object_to_dict(task)
    return None, {}


def _object_to_dict(obj: object) -> dict[str, Any]:
    result = {}
    for attr in ("id", "title", "description", "acceptance_criteria", "files", "dependencies"):
        if hasattr(obj, attr):
            result[attr] = getattr(obj, attr)
    return result


def _task_title(payload: Mapping[str, Any]) -> str:
    return str(payload.get("title", "")).strip() or "Untitled task"


def _has_no_impact(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(str(v) for v in payload.values() if v)
    return bool(_NO_IMPACT_RE.search(searchable))


def _detect_signals(payload: Mapping[str, Any]) -> tuple[SSOSignal, ...]:
    text_signals = _text_signals(payload)
    path_signals = _path_signals(payload)
    combined = set(text_signals) | set(path_signals)
    return tuple(s for s in _SIGNAL_ORDER if s in combined)


def _text_signals(payload: Mapping[str, Any]) -> list[SSOSignal]:
    searchable = " ".join(
        str(payload.get(f, ""))
        for f in ("title", "description", "acceptance_criteria", "files")
        if payload.get(f)
    )
    return [signal for signal in _SIGNAL_ORDER if _SIGNAL_PATTERNS[signal].search(searchable)]


def _path_signals(payload: Mapping[str, Any]) -> list[SSOSignal]:
    files = payload.get("files", [])
    if isinstance(files, str):
        files = [files]
    paths = " ".join(str(PurePosixPath(f).as_posix()) for f in files if f)
    return [signal for signal in _SIGNAL_ORDER if _PATH_SIGNAL_PATTERNS[signal].search(paths)]


def _detect_safeguards(payload: Mapping[str, Any], signals: tuple[SSOSignal, ...]) -> tuple[SSOSafeguard, ...]:
    searchable = " ".join(str(v) for v in payload.values() if v)
    detected = [
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable)
    ]
    return tuple(detected)


def _expected_safeguards(signals: tuple[SSOSignal, ...]) -> list[SSOSafeguard]:
    mapping: dict[SSOSignal, list[SSOSafeguard]] = {
        "sso_protocol_support": ["sso_library_integration", "saml_oidc_configuration"],
        "identity_provider_integration": ["idp_metadata_management", "saml_oidc_configuration"],
        "scim_provisioning": ["scim_endpoint_implementation"],
        "jit_provisioning": ["attribute_mapping_configuration"],
        "sso_attribute_mapping": ["attribute_mapping_configuration"],
        "multi_idp_support": ["multi_idp_routing_logic"],
        "sso_session_management": ["sso_session_handling"],
        "sso_testing": ["sso_testing_coverage"],
    }
    expected: set[SSOSafeguard] = set()
    for signal in signals:
        expected.update(mapping.get(signal, []))
    return [s for s in _SAFEGUARD_ORDER if s in expected]


def _assess_readiness(
    signals: tuple[SSOSignal, ...],
    safeguards: tuple[SSOSafeguard, ...],
    missing: tuple[SSOSafeguard, ...],
) -> SSOReadiness:
    if not signals:
        return "partial"
    expected = _expected_safeguards(signals)
    if not expected:
        return "strong"
    coverage = len(safeguards) / len(expected) if expected else 0
    if coverage >= 0.8:
        return "strong"
    if coverage >= 0.4:
        return "partial"
    return "weak"


def _collect_evidence(
    payload: Mapping[str, Any],
    signals: tuple[SSOSignal, ...],
    safeguards: tuple[SSOSafeguard, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    searchable_fields = {
        "title": payload.get("title", ""),
        "description": payload.get("description", ""),
        "acceptance_criteria": payload.get("acceptance_criteria", ""),
        "files": payload.get("files", []),
    }

    for signal in signals[:3]:  # Limit evidence collection
        pattern = _SIGNAL_PATTERNS[signal]
        for field_name, value in searchable_fields.items():
            text = str(value) if value else ""
            if match := pattern.search(text):
                snippet = match.group(0)
                evidence.append(f"{field_name}: ...{snippet}...")
                break

    for safeguard in safeguards[:2]:  # Limit evidence collection
        pattern = _SAFEGUARD_PATTERNS[safeguard]
        for field_name, value in searchable_fields.items():
            text = str(value) if value else ""
            if match := pattern.search(text):
                snippet = match.group(0)
                evidence.append(f"{field_name}: ...{snippet}...")
                break

    return tuple(evidence[:5])


def _summary(
    findings: list[TaskApiSSOIntegrationReadinessFinding],
    sso_task_ids: list[str],
    not_applicable_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "total_tasks": len(findings) + len(not_applicable_task_ids),
        "sso_tasks": len(sso_task_ids),
        "not_applicable_tasks": len(not_applicable_task_ids),
        "readiness_distribution": {
            "strong": sum(1 for f in findings if f.readiness == "strong"),
            "partial": sum(1 for f in findings if f.readiness == "partial"),
            "weak": sum(1 for f in findings if f.readiness == "weak"),
        },
        "most_common_gaps": _top_gaps([f.missing_safeguards for f in findings]),
    }


def _top_gaps(all_gaps: list[tuple[SSOSafeguard, ...]]) -> list[str]:
    from collections import Counter

    flat = [gap for gaps in all_gaps for gap in gaps]
    return [gap for gap, _ in Counter(flat).most_common(3)]


__all__ = [
    "SSOSignal",
    "SSOSafeguard",
    "SSOReadiness",
    "TaskApiSSOIntegrationReadinessFinding",
    "TaskApiSSOIntegrationReadinessPlan",
    "plan_task_api_sso_integration_readiness",
]
