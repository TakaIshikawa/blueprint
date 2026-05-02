import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_role_delegation_requirements import (
    SourceRoleDelegationRequirement,
    SourceRoleDelegationRequirementsReport,
    build_source_role_delegation_requirements,
    derive_source_role_delegation_requirements,
    extract_source_role_delegation_requirements,
    generate_source_role_delegation_requirements,
    source_role_delegation_requirements_to_dict,
    source_role_delegation_requirements_to_dicts,
    source_role_delegation_requirements_to_markdown,
    summarize_source_role_delegation_requirements,
)


def test_extracts_role_catalog_scope_approval_expiry_review_revocation_audit_and_break_glass():
    result = build_source_role_delegation_requirements(
        _source_brief(
            source_payload={
                "role_delegation": {
                    "catalog": (
                        "Role catalog must define security admin, billing admin, viewer, and editor roles."
                    ),
                    "scope": "Delegated admin assignments must be scoped to workspace and project resources.",
                    "approval": "Access requests require manager approval and security approver review before grant.",
                    "expiry": "Temporary privileged access must expire after 8 hours.",
                    "break_glass": "Break-glass emergency admin access must alert security and require post-use review.",
                    "review": "Privilege review must run quarterly for stale access.",
                    "revocation": "Revocation flow must remove access within 24 hours after offboarding.",
                    "audit": "Audit logging must track role grants, approvals, revocations, and break-glass access.",
                }
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, SourceRoleDelegationRequirementsReport)
    assert all(isinstance(record, SourceRoleDelegationRequirement) for record in result.records)
    assert set(by_type) == {
        "role_catalog",
        "delegation_scope",
        "approver_workflow",
        "time_bound_access",
        "break_glass_access",
        "privilege_review",
        "revocation_flow",
        "audit_logging",
    }
    assert by_type["role_catalog"].value == "security admin, billing admin, viewer, editor"
    assert by_type["delegation_scope"].role_scope == "workspace"
    assert by_type["time_bound_access"].value == "after 8 hours"
    assert by_type["privilege_review"].value == "quarterly"
    assert by_type["revocation_flow"].value == "within 24 hours"
    assert by_type["audit_logging"].confidence == "high"
    assert by_type["approver_workflow"].unresolved_questions[0].startswith("Confirm approval steps")
    assert by_type["revocation_flow"].suggested_plan_impacts[0].startswith("Implement role removal")
    assert result.summary["requires_approval"] is True
    assert result.summary["requires_expiry"] is True
    assert result.summary["requires_review"] is True
    assert result.summary["requires_revocation"] is True
    assert result.summary["requires_audit_logging"] is True
    assert result.summary["status"] == "ready_for_role_delegation_planning"


def test_structured_source_brief_and_implementation_brief_are_supported():
    source = _source_brief(
        source_payload={
            "privileged_access": [
                {
                    "role": "Security admin",
                    "delegation_scope": "restricted to tenant accounts",
                    "approval": "two-person approval required",
                    "duration": "temporary access expires after 4 hours",
                    "audit_logging": "record delegated access grants",
                },
                {
                    "role_catalog": "Custom roles and read-only admin are required for support delegation.",
                    "revocation": "auto-remove delegated access after 30 days",
                },
            ]
        }
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Delegated admin grants must be limited to workspace resources.",
                "Access review recertification should run monthly for privileged roles.",
            ],
            definition_of_done=[
                "Break glass emergency access is audited and reviewed after every incident."
            ],
        )
    )

    source_result = build_source_role_delegation_requirements(SourceBrief.model_validate(source))
    implementation_result = generate_source_role_delegation_requirements(implementation)

    by_type = {record.requirement_type: record for record in source_result.records}
    assert {
        "role_catalog",
        "delegation_scope",
        "approver_workflow",
        "time_bound_access",
        "revocation_flow",
        "audit_logging",
    } <= set(by_type)
    assert by_type["time_bound_access"].value == "after 4 hours"
    assert by_type["revocation_flow"].value == "after 30 days"
    assert by_type["delegation_scope"].source_field == "source_payload.privileged_access[0]"
    implementation_types = {record.requirement_type for record in implementation_result.records}
    assert {
        "delegation_scope",
        "privilege_review",
        "break_glass_access",
        "audit_logging",
    } <= implementation_types
    assert implementation_result.source_id == "impl-role-delegation"


def test_serialization_markdown_aliases_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="role-delegation-model",
        source_payload={
            "acceptance_criteria": [
                "Delegated admin role assignments must require approval and audit trail evidence.",
                "Delegated admin role assignments must require approval and audit trail evidence.",
                "Role catalog must include security admin | billing admin roles.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_role_delegation_requirements(source)
    model_result = extract_source_role_delegation_requirements(model)
    derived = derive_source_role_delegation_requirements(model)
    text_result = build_source_role_delegation_requirements(
        "Temporary privileged access should expire after 2 hours and revocation must remove roles."
    )
    object_result = build_source_role_delegation_requirements(
        SimpleNamespace(
            id="object-role",
            metadata={"roles": "Role management must support access review monthly."},
        )
    )
    payload = source_role_delegation_requirements_to_dict(model_result)
    markdown = source_role_delegation_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_role_delegation_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_role_delegation_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_role_delegation_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "source_field",
        "requirement_type",
        "role_scope",
        "value",
        "evidence",
        "confidence",
        "unresolved_questions",
        "suggested_plan_impacts",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Source Field | Type | Scope | Value |" in markdown
    assert "security admin \\| billing admin" in markdown
    assert text_result.records[0].requirement_type == "time_bound_access"
    assert object_result.records[0].requirement_type == "role_catalog"


def test_generic_ownership_assignment_no_impact_malformed_and_blank_inputs_are_empty():
    no_scope = build_source_role_delegation_requirements(
        _source_brief(
            summary="Role management and delegated admin access are out of scope for this release."
        )
    )
    unrelated = build_source_role_delegation_requirements(
        _source_brief(
            title="Task ownership",
            summary="Each implementation task should have an owner and assignee before sprint planning.",
            source_payload={
                "requirements": [
                    "Assign an owner to every onboarding checklist task.",
                    "Project owner reviews launch copy.",
                ]
            },
        )
    )
    malformed = build_source_role_delegation_requirements({"source_payload": {"notes": object()}})
    blank = build_source_role_delegation_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_type_counts": {
            "role_catalog": 0,
            "delegation_scope": 0,
            "approver_workflow": 0,
            "time_bound_access": 0,
            "break_glass_access": 0,
            "privilege_review": 0,
            "revocation_flow": 0,
            "audit_logging": 0,
        },
        "requirement_types": [],
        "role_scopes": [],
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requires_approval": False,
        "requires_expiry": False,
        "requires_review": False,
        "requires_revocation": False,
        "requires_audit_logging": False,
        "status": "no_role_delegation_language",
    }
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source role delegation requirements were inferred." in unrelated.to_markdown()


def test_deterministic_sorting_across_multiple_sources():
    result = build_source_role_delegation_requirements(
        [
            _source_brief(
                source_id="z-source",
                source_payload={
                    "roles": "Delegated admin grants must be scoped to tenant resources."
                },
            ),
            _source_brief(
                source_id="a-source",
                source_payload={
                    "roles": [
                        "Privilege review must run annually for security admin access.",
                        "Temporary access must expire after 1 day.",
                    ]
                },
            ),
        ]
    )

    keys = [
        (record.source_brief_id, record.requirement_type, record.value or "")
        for record in result.records
    ]

    assert result.source_id is None
    assert keys == sorted(
        keys,
        key=lambda item: (
            item[0] or "",
            [
                "role_catalog",
                "delegation_scope",
                "approver_workflow",
                "time_bound_access",
                "break_glass_access",
                "privilege_review",
                "revocation_flow",
                "audit_logging",
            ].index(item[1]),
            item[2],
        ),
    )
    assert (
        result.to_dict()
        == build_source_role_delegation_requirements(
            [
                _source_brief(
                    source_id="z-source",
                    source_payload={
                        "roles": "Delegated admin grants must be scoped to tenant resources."
                    },
                ),
                _source_brief(
                    source_id="a-source",
                    source_payload={
                        "roles": [
                            "Privilege review must run annually for security admin access.",
                            "Temporary access must expire after 1 day.",
                        ]
                    },
                ),
            ]
        ).to_dict()
    )


def _source_brief(
    *,
    source_id="source-role-delegation",
    title="Role delegation requirements",
    domain="identity",
    summary="General role delegation requirements.",
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-role-delegation",
        "source_brief_id": "source-role-delegation",
        "title": "Role delegation rollout",
        "domain": "identity",
        "target_user": "security admins",
        "buyer": None,
        "workflow_context": "Teams need delegated access requirements before task generation.",
        "problem_statement": "Role delegation requirements need to be extracted early.",
        "mvp_goal": "Plan delegated admin and privileged access work from source briefs.",
        "product_surface": "identity",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review role delegation controls.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
