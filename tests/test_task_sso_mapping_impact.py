import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_sso_mapping_impact import (
    TaskSsoMappingImpactPlan,
    TaskSsoMappingImpactRecommendation,
    analyze_task_sso_mapping_impact,
    build_task_sso_mapping_impact_plan,
    extract_task_sso_mapping_impact,
    generate_task_sso_mapping_impact,
    recommend_task_sso_mapping_impact,
    summarize_task_sso_mapping_impact,
    task_sso_mapping_impact_plan_to_dict,
    task_sso_mapping_impact_plan_to_markdown,
)


def test_detects_saml_oidc_scim_and_recommends_identity_safeguards():
    result = build_task_sso_mapping_impact_plan(
        _plan(
            [
                _task(
                    "task-saml-oidc",
                    title="Implement SAML and OIDC enterprise SSO",
                    description=(
                        "Add SAML metadata upload and OIDC id token parsing for enterprise login. "
                        "SCIM provisioning sends group claims for access."
                    ),
                    acceptance_criteria=["Enterprise SSO can be configured by admins."],
                )
            ]
        )
    )

    assert isinstance(result, TaskSsoMappingImpactPlan)
    assert result.sso_task_ids == ("task-saml-oidc",)
    record = result.records[0]
    assert isinstance(record, TaskSsoMappingImpactRecommendation)
    assert {
        "sso",
        "saml",
        "oidc",
        "scim",
        "group_claim",
        "enterprise_login",
    } <= set(record.impacted_identity_surfaces)
    assert record.required_safeguards == (
        "claim_mapping_tests",
        "fallback_access",
        "admin_recovery",
        "tenant_isolation",
        "audit_logging",
        "documentation_updates",
    )
    assert "claim_mapping_tests" not in record.present_safeguards
    assert any("SAML/OIDC claim mapping tests" in item for item in record.missing_acceptance_criteria)
    assert record.risk_level == "high"
    assert any("description:" in item and "SAML metadata" in item for item in record.evidence)
    assert result.summary["sso_task_count"] == 1
    assert result.summary["surface_counts"]["saml"] == 1


def test_metadata_and_file_path_evidence_detect_identity_provider_role_mapping():
    result = analyze_task_sso_mapping_impact(
        _plan(
            [
                _task(
                    "task-role-map",
                    title="Map IdP groups to app roles",
                    description="Wire identity provider claims into RBAC.",
                    files_or_modules=[
                        "src/auth/saml/group_claims.py",
                        "src/security/role_mapping/service.py",
                    ],
                    metadata={
                        "identity_provider": "Okta IdP sends memberOf group claims.",
                        "migration": {"tenant_domain": "Verified domains route users to the correct tenant."},
                    },
                    acceptance_criteria=[
                        "Role mapping tests cover expected group claims.",
                        "Audit logging records mapping change events.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"saml", "identity_provider", "group_claim", "role_mapping", "tenant_domain"} <= set(
        record.impacted_identity_surfaces
    )
    assert record.present_safeguards == ("claim_mapping_tests", "tenant_isolation", "audit_logging")
    assert record.risk_level == "high"
    assert any("files_or_modules: src/auth/saml/group_claims.py" == item for item in record.evidence)
    assert any("metadata.identity_provider" in item for item in record.evidence)
    assert any("metadata.migration.tenant_domain" in item for item in record.evidence)


def test_role_and_tenant_mapping_are_not_high_risk_when_acceptance_coverage_is_complete():
    result = build_task_sso_mapping_impact_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Change tenant domain and role mappings",
                    description="Update verified domain routing and map IdP groups to roles.",
                    files_or_modules=["src/auth/tenant_domains/role_mapping.py"],
                    acceptance_criteria=[
                        "Claim mapping tests cover SAML assertions, OIDC id tokens, malformed claims, and multi-group claims.",
                        "Fallback access keeps a backup login available if SSO mapping fails.",
                        "Admin recovery can restore admin access after lockout.",
                        "Tenant isolation tests block cross-tenant access and verify domain ownership.",
                        "Audit logging records identity event and mapping change log entries.",
                        "Documentation updates cover the admin setup guide and migration guide.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"role_mapping", "tenant_domain"} <= set(record.impacted_identity_surfaces)
    assert record.missing_acceptance_criteria == ()
    assert record.risk_level == "medium"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["missing_acceptance_criteria_count"] == 0


def test_missing_acceptance_criteria_are_stable_and_markdown_escapes_pipes():
    result = build_task_sso_mapping_impact_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Enterprise | SSO setup",
                    description="Add enterprise login with IdP configuration and SSO audit logging.",
                    acceptance_criteria=["Audit logs capture SSO login events."],
                )
            ]
        )
    )

    payload = task_sso_mapping_impact_plan_to_dict(result)
    markdown = task_sso_mapping_impact_plan_to_markdown(result)
    record = result.records[0]

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["recommendations"]
    assert payload["records"] == payload["recommendations"]
    assert record.present_safeguards == ("audit_logging",)
    assert len(record.missing_acceptance_criteria) == 5
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "records",
        "sso_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "impacted_identity_surfaces",
        "required_safeguards",
        "present_safeguards",
        "missing_acceptance_criteria",
        "risk_level",
        "evidence",
        "recommended_follow_up_actions",
    ]
    assert markdown.startswith("# Task SSO Mapping Impact Plan: plan-sso-mapping")
    assert "Enterprise \\| SSO setup" in markdown
    assert "| Task | Title | Risk | Identity Surfaces | Present Safeguards | Missing Acceptance Criteria | Evidence |" in markdown


def test_ignores_unrelated_login_ui_copy_task():
    result = build_task_sso_mapping_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update login UI wording",
                    description="Change button copy and helper text on the standard login form.",
                    files_or_modules=["src/ui/login_form.tsx"],
                    acceptance_criteria=["Login copy matches approved wording."],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.sso_task_ids == ()
    assert result.ignored_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "sso_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_acceptance_criteria_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "surface_counts": {},
        "present_safeguard_counts": {
            "claim_mapping_tests": 0,
            "fallback_access": 0,
            "admin_recovery": 0,
            "tenant_isolation": 0,
            "audit_logging": 0,
            "documentation_updates": 0,
        },
    }
    assert "No SSO mapping impact recommendations" in result.to_markdown()
    assert "Ignored tasks: task-copy" in result.to_markdown()


def test_execution_plan_iterable_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Configure SCIM provisioning",
                description="SCIM provisioning syncs IdP groups.",
                acceptance_criteria=[
                    "Claim mapping tests cover group claims.",
                    "Fallback access keeps backup login available.",
                    "Admin recovery restores admin access.",
                    "Tenant isolation prevents cross-tenant provisioning.",
                    "Audit logging records provisioning events.",
                    "Documentation updates include the SCIM setup guide.",
                ],
            ),
            _task("task-copy", title="Update auth copy", description="Adjust login label wording."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    iterable_result = build_task_sso_mapping_impact_plan(
        [
            _task(
                "task-one",
                title="OIDC IdP support",
                description="Add OpenID Connect identity provider support.",
            )
        ]
    )

    result = summarize_task_sso_mapping_impact(model)

    assert plan == original
    assert result.plan_id == "plan-sso-mapping"
    assert result.sso_task_ids == ("task-z",)
    assert result.ignored_task_ids == ("task-copy",)
    assert result.records[0].risk_level == "low"
    assert extract_task_sso_mapping_impact(model).to_dict() == result.to_dict()
    assert generate_task_sso_mapping_impact(model).to_dict() == result.to_dict()
    assert recommend_task_sso_mapping_impact(model).to_dict() == result.to_dict()
    assert iterable_result.plan_id is None
    assert iterable_result.sso_task_ids == ("task-one",)


def _plan(tasks, plan_id="plan-sso-mapping"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-sso-mapping",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-sso-mapping",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
