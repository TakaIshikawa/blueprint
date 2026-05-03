import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_scim_provisioning_requirements import (
    SourceSCIMProvisioningRequirement,
    SourceSCIMProvisioningRequirementsReport,
    build_source_scim_provisioning_requirements,
    derive_source_scim_provisioning_requirements,
    extract_source_scim_provisioning_requirements,
    generate_source_scim_provisioning_requirements,
    source_scim_provisioning_requirements_to_dict,
    source_scim_provisioning_requirements_to_dicts,
    source_scim_provisioning_requirements_to_markdown,
    summarize_source_scim_provisioning_requirements,
)


def test_structured_scim_payload_extracts_lifecycle_actions_in_order():
    result = build_source_scim_provisioning_requirements(
        _source_brief(
            source_payload={
                "identity_lifecycle": {
                    "provider": "Enterprise IdP constraints require Okta and Microsoft Entra setup per tenant.",
                    "provisioning": "SCIM 2.0 must provision users with userName and active attributes before launch.",
                    "deprovisioning": "Deprovisioning must revoke access and remove users after termination.",
                    "group_sync": "Directory group sync should sync groups and group memberships hourly.",
                    "group_push": "Okta group push must push groups owned by the IdP.",
                    "role_mapping": "Role mapping maps IdP groups claim to admin and member roles.",
                    "jit": "Just-in-Time provisioning creates users on first login with default role.",
                    "directory_sync": "Active Directory sync is the source of truth for users and groups.",
                    "suspension": "Suspended users must be blocked from login while retaining audit history.",
                    "external_ids": "SCIM externalId and NameID must map to the directory ID.",
                }
            },
        )
    )

    by_action = {record.lifecycle_action: record for record in result.records}

    assert isinstance(result, SourceSCIMProvisioningRequirementsReport)
    assert all(isinstance(record, SourceSCIMProvisioningRequirement) for record in result.records)
    assert [record.lifecycle_action for record in result.records] == [
        "provisioning",
        "deprovisioning",
        "group_sync",
        "group_push",
        "role_mapping",
        "jit_provisioning",
        "directory_sync",
        "suspension",
        "external_id_mapping",
        "idp_constraint",
    ]
    assert by_action["provisioning"].confidence == "high"
    assert by_action["provisioning"].identity_provider == ""
    assert by_action["provisioning"].mapped_entity.startswith("users")
    assert by_action["provisioning"].timing == "before launch"
    assert by_action["group_sync"].identity_provider == ""
    assert by_action["group_sync"].timing == "hourly"
    assert by_action["group_push"].identity_provider == "Okta"
    assert by_action["role_mapping"].mapped_entity == "groups, claim, roles"
    assert by_action["jit_provisioning"].timing == "on first login"
    assert by_action["directory_sync"].identity_provider == "Active Directory"
    assert by_action["suspension"].mapped_entity == "Suspended users"
    assert by_action["external_id_mapping"].mapped_entity == "externalId, NameID, directory ID"
    assert by_action["idp_constraint"].identity_provider == "Okta, Microsoft Entra"
    assert result.summary["requirement_count"] == 10
    assert result.summary["action_counts"]["role_mapping"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_free_text_identity_lifecycle_requirements_and_duplicates_collapse():
    result = build_source_scim_provisioning_requirements(
        _source_brief(
            summary=(
                "Enterprise identity lifecycle requires SCIM user provisioning for Okta. "
                "SCIM user provisioning should create and update users. "
                "SCIM user provisioning must create and update users before launch for Okta. "
                "Deprovisioning must suspend users, revoke access, and disable login on offboarding. "
                "Group sync should sync directory groups daily. "
                "Role mapping should map groups claim to workspace roles."
            )
        )
    )

    by_action = {record.lifecycle_action: record for record in result.records}

    assert [record.lifecycle_action for record in result.records] == [
        "provisioning",
        "deprovisioning",
        "group_sync",
        "role_mapping",
        "directory_sync",
        "suspension",
    ]
    assert by_action["provisioning"].confidence == "high"
    assert by_action["provisioning"].source_field == "summary"
    assert by_action["provisioning"].identity_provider == "Okta"
    assert by_action["provisioning"].timing == "before launch"
    assert len(by_action["provisioning"].evidence) == 3
    assert any("must create and update users before launch" in item for item in by_action["provisioning"].evidence)
    assert by_action["deprovisioning"].mapped_entity == "users"
    assert by_action["suspension"].lifecycle_action == by_action["suspension"].category
    assert by_action["role_mapping"].requirement_category == "role_mapping"


def test_model_object_serialization_markdown_and_alias_helpers_are_stable():
    source = _source_brief(
        source_id="scim-model",
        summary="SCIM provisioning must support Azure AD users before launch.",
        source_payload={
            "identity": {
                "role_mapping": "Role mapping must map Azure AD groups to app roles.",
                "external_ids": "externalId must store the immutable directory ID.",
            }
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            integration_points=[
                "JIT provisioning must create users on first login.",
                "Group push must support Okta group push.",
            ]
        )
    )
    obj = SimpleNamespace(
        id="object-scim",
        scim="Directory sync must reconcile users and groups nightly from Workday.",
    )

    result = build_source_scim_provisioning_requirements(source)
    generated = generate_source_scim_provisioning_requirements(model)
    derived = derive_source_scim_provisioning_requirements(model)
    extracted = extract_source_scim_provisioning_requirements(model)
    implementation_result = build_source_scim_provisioning_requirements(implementation)
    object_result = build_source_scim_provisioning_requirements(obj)
    payload = source_scim_provisioning_requirements_to_dict(generated)
    markdown = source_scim_provisioning_requirements_to_markdown(generated)

    assert source == original
    assert generated.to_dict() == result.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted == generated.requirements
    assert summarize_source_scim_provisioning_requirements(generated) == generated.summary
    assert source_scim_provisioning_requirements_to_dicts(generated) == payload["requirements"]
    assert source_scim_provisioning_requirements_to_dicts(generated.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert list(payload) == ["source_id", "title", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "lifecycle_action",
        "identity_provider",
        "mapped_entity",
        "timing",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
        "unresolved_questions",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source SCIM Provisioning Requirements Report: scim-model")
    assert "| Lifecycle Action | Provider | Mapped Entity | Timing | Confidence | Source | Evidence | Planning Notes |" in markdown
    assert "Azure AD" in markdown
    assert "externalId" in markdown
    assert [record.lifecycle_action for record in implementation_result.records] == [
        "group_push",
        "jit_provisioning",
    ]
    assert [record.lifecycle_action for record in object_result.records] == ["directory_sync"]
    assert object_result.records[0].identity_provider == "Workday"


def test_out_of_scope_invalid_and_repeated_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-scim"
        summary = "SCIM provisioning, directory sync, and role mapping are out of scope for this release."

    empty = build_source_scim_provisioning_requirements(
        _source_brief(
            source_id="empty-scim",
            title="Identity help copy",
            summary="Keep identity help copy unchanged. No SCIM provisioning or group mapping work is required.",
        )
    )
    repeat = build_source_scim_provisioning_requirements(
        _source_brief(
            source_id="empty-scim",
            title="Identity help copy",
            summary="Keep identity help copy unchanged. No SCIM provisioning or group mapping work is required.",
        )
    )
    negated = build_source_scim_provisioning_requirements(BriefLike())
    invalid = build_source_scim_provisioning_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "action_counts": {
            "provisioning": 0,
            "deprovisioning": 0,
            "group_sync": 0,
            "group_push": 0,
            "role_mapping": 0,
            "jit_provisioning": 0,
            "directory_sync": 0,
            "suspension": 0,
            "external_id_mapping": 0,
            "idp_constraint": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "lifecycle_actions": [],
        "status": "no_scim_provisioning_requirements_found",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-scim"
    assert empty.requirements == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No SCIM provisioning requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="scim-source",
    title="SCIM provisioning requirements",
    domain="identity",
    summary="General identity lifecycle requirements.",
    source_payload=None,
    source_links=None,
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
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, integration_points=None):
    return {
        "id": "impl-scim",
        "source_brief_id": "source-scim",
        "title": "Enterprise SCIM rollout",
        "domain": "identity",
        "target_user": "enterprise admins",
        "buyer": None,
        "workflow_context": "Enterprise setup requires identity lifecycle tasks before launch.",
        "problem_statement": "SCIM provisioning requirements need planning coverage.",
        "mvp_goal": "Plan identity lifecycle provisioning.",
        "product_surface": "admin identity",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [],
        "validation_plan": "Review generated plan for SCIM provisioning coverage.",
        "definition_of_done": ["SCIM provisioning requirements are represented."],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
