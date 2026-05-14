"""Assess readiness for partner API onboarding execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskPartnerApiOnboardingReadinessPlan = SimpleReadinessPlan
TaskPartnerApiOnboardingReadinessRecord = SimpleReadinessRecord
TaskPartnerApiOnboardingReadinessFinding = SimpleReadinessRecord
TaskPartnerApiOnboardingReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "partner_api_onboarding": re.compile(r"\b(?:partner api onboarding|partner onboarding|onboard partners?|external partner onboarding|partner enablement)\b", re.I),
    "partner_credentials": re.compile(r"\b(?:partner credentials?|api credentials?|client credentials?|partner api keys?|credential issuance|client secret|developer key)\b", re.I),
    "developer_portal_onboarding": re.compile(r"\b(?:developer portal|partner portal|developer onboarding|portal onboarding|self[- ]serve onboarding|app registration)\b", re.I),
    "partner_sandbox_setup": re.compile(r"\b(?:partner sandbox|sandbox setup|sandbox tenant|sandbox app|test partner|sandbox credentials?|test data)\b", re.I),
    "external_partner_integration": re.compile(r"\b(?:external partner integration|partner integration|partner api|external partners?|partner enablement|integration enablement)\b", re.I),
}
_PATH_SIGNALS = {
    "partner_api_onboarding": re.compile(r"(?:partner.*onboard|onboard.*partner|partner[_-]?api|external[_-]?partner)", re.I),
    "partner_credentials": re.compile(r"(?:partner.*credential|credential.*partner|api[_-]?keys?|client[_-]?secret|developer[_-]?key)", re.I),
    "developer_portal_onboarding": re.compile(r"(?:developer[_-]?portal|partner[_-]?portal|app[_-]?registration|portal[_-]?onboard)", re.I),
    "partner_sandbox_setup": re.compile(r"(?:partner[_-]?sandbox|sandbox.*partner|test[_-]?partner|sandbox[_-]?credentials?|test[_-]?data)", re.I),
    "external_partner_integration": re.compile(r"(?:partners?|partner[_-]?integrations?|integrations?/partners?|external[_-]?integrations?)", re.I),
}
_CRITERIA = {
    "partner_identity": re.compile(r"\b(?:partner identity|partner id|partner identifier|partner account|partner tenant|organization id|legal entity|app identity)\b", re.I),
    "credential_issuance": re.compile(r"\b(?:credential issuance|issue credentials?|api key issuance|client id|client secret|credential rotation|secret delivery|key provisioning)\b", re.I),
    "sandbox_test_data": re.compile(r"\b(?:sandbox|test data|fixture data|sample data|test account|sandbox tenant|sandbox credentials?|test partner)\b", re.I),
    "access_scopes": re.compile(r"\b(?:access scopes?|oauth scopes?|permission scopes?|scoped access|least privilege|permissions?|entitlements?)\b", re.I),
    "rate_limits_quotas": re.compile(r"\b(?:rate limits?|quotas?|throttl\w*|requests per minute|rpm|requests per second|rps|usage cap|limit policy)\b", re.I),
    "documentation": re.compile(r"\b(?:documentation|docs|developer guide|partner guide|api docs|openapi|quickstart|runbook|integration guide)\b", re.I),
    "support_escalation_path": re.compile(r"\b(?:support path|support channel|escalation path|partner support|on[- ]call|contact|slack channel|support queue|incident escalation)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|integration tests?|contract tests?|sandbox tests?|credential tests?|onboarding tests?|portal tests?)\b", re.I),
}
_GUIDANCE = {
    "partner_identity": "Define partner identity, partner ID, account, tenant, organization, legal entity, or app identity.",
    "credential_issuance": "Specify credential issuance, key provisioning, client ID, client secret, delivery, or rotation.",
    "sandbox_test_data": "Prepare sandbox setup, sandbox credentials, test accounts, fixtures, sample data, or test partners.",
    "access_scopes": "Document access scopes, permissions, entitlements, scoped access, or least-privilege rules.",
    "rate_limits_quotas": "Define partner rate limits, quotas, throttling, request caps, or limit policy.",
    "documentation": "Publish partner, developer, API, OpenAPI, quickstart, runbook, or integration documentation.",
    "support_escalation_path": "Identify support channels, contacts, escalation paths, on-call ownership, or support queues.",
    "tests": "Add integration, contract, sandbox, credential, onboarding, portal, or API tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:partner api onboarding|partner onboarding|partner credentials?|developer portal|partner sandbox|external partner integration|partner api)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_partner_api_onboarding_readiness_plan(source: Any) -> TaskPartnerApiOnboardingReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Partner API Onboarding Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_partner_api_onboarding_readiness = build_task_partner_api_onboarding_readiness_plan
extract_task_partner_api_onboarding_readiness = build_task_partner_api_onboarding_readiness_plan
generate_task_partner_api_onboarding_readiness = build_task_partner_api_onboarding_readiness_plan
derive_task_partner_api_onboarding_readiness = build_task_partner_api_onboarding_readiness_plan
summarize_task_partner_api_onboarding_readiness = build_task_partner_api_onboarding_readiness_plan
summarize_task_partner_api_onboarding_readiness_plan = build_task_partner_api_onboarding_readiness_plan


def recommend_task_partner_api_onboarding_readiness(source: Any) -> tuple[TaskPartnerApiOnboardingReadinessRecord, ...]:
    return build_task_partner_api_onboarding_readiness_plan(source).records


def task_partner_api_onboarding_readiness_plan_to_dict(plan: TaskPartnerApiOnboardingReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_partner_api_onboarding_readiness_plan_to_dict.__test__ = False


def task_partner_api_onboarding_readiness_plan_to_dicts(
    plan: TaskPartnerApiOnboardingReadinessPlan | Iterable[TaskPartnerApiOnboardingReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_partner_api_onboarding_readiness_plan_to_dicts.__test__ = False
task_partner_api_onboarding_readiness_to_dicts = task_partner_api_onboarding_readiness_plan_to_dicts
task_partner_api_onboarding_readiness_to_dicts.__test__ = False


def task_partner_api_onboarding_readiness_plan_to_markdown(plan: TaskPartnerApiOnboardingReadinessPlan) -> str:
    return plan.to_markdown()


task_partner_api_onboarding_readiness_plan_to_markdown.__test__ = False
