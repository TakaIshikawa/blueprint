import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_permission_scope_requirements import (
    SourcePermissionScopeRequirement,
    SourcePermissionScopeRequirementsReport,
    build_source_permission_scope_requirements,
    derive_source_permission_scope_requirements,
    extract_source_permission_scope_requirements,
    generate_source_permission_scope_requirements,
    source_permission_scope_requirements_to_dict,
    source_permission_scope_requirements_to_dicts,
    source_permission_scope_requirements_to_markdown,
    summarize_source_permission_scope_requirements,
)


def test_nested_source_payload_extracts_permission_scope_requirements_with_paths():
    result = build_source_permission_scope_requirements(
        _source_brief(
            source_payload={
                "authorization": {
                    "requirements": [
                        "RBAC must define owner, admin, editor, and viewer roles.",
                        "Permissions must be limited to billing.read and billing.write scopes.",
                        "ABAC policy must use tenant and region attributes for export access.",
                    ],
                    "admin_capabilities": {
                        "user_management": "Tenant admins can manage users and roles.",
                    },
                    "delegation": [
                        "Managers may delegate approval for time-bound access requests.",
                    ],
                    "least_privilege": "Grant minimum permissions and deny by default.",
                }
            }
        )
    )

    assert isinstance(result, SourcePermissionScopeRequirementsReport)
    assert all(isinstance(record, SourcePermissionScopeRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "role_requirement",
        "permission_scope",
        "rbac",
        "abac",
        "least_privilege",
        "admin_capability",
        "delegation",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["rbac"].source_field == "source_payload"
    assert by_type["rbac"].source_path == "source_payload.authorization.requirements[0]"
    assert "source_payload.authorization.requirements[1]" in by_type["permission_scope"].source_path
    assert by_type["admin_capability"].source_path.endswith("admin_capabilities.user_management")
    assert any("billing.read" in item for item in by_type["permission_scope"].evidence)
    assert "Tenant admins can manage users" in by_type["admin_capability"].summary
    assert result.summary["requirement_count"] == len(result.records)
    assert result.summary["type_counts"]["delegation"] == 1


def test_plain_brief_fields_and_implementation_brief_are_scanned():
    source_result = extract_source_permission_scope_requirements(
        _source_brief(
            summary=(
                "Support least privilege authorization. Admins may impersonate users only "
                "with explicit support permission."
            ),
            source_payload={"notes": "No extra text."},
        )
    )
    implementation_result = derive_source_permission_scope_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Access control must use role-based access control for requester and approver roles.",
                    "Delegated admin access must be revocable.",
                ]
            )
        )
    )

    assert [record.requirement_type for record in source_result.records] == [
        "role_requirement",
        "permission_scope",
        "least_privilege",
        "admin_capability",
    ]
    assert [record.requirement_type for record in implementation_result.records] == [
        "role_requirement",
        "permission_scope",
        "rbac",
        "admin_capability",
        "delegation",
    ]
    assert implementation_result.source_brief_id == "impl-permission"


def test_negated_unrelated_and_invalid_inputs_return_empty_reports():
    negated = build_source_permission_scope_requirements(
        _source_brief(
            title="Copy cleanup",
            summary="No role, permission, RBAC, ABAC, admin capability, or delegation work is needed.",
            source_payload={
                "requirements": [
                    "Keep the project scope unchanged.",
                    "Permission scope changes are out of scope for this release.",
                ]
            },
        )
    )
    unrelated = build_source_permission_scope_requirements(
        _source_brief(summary="Improve dashboard loading copy and empty states.")
    )
    invalid = build_source_permission_scope_requirements(object())

    assert negated.source_brief_id == "permission-source"
    assert negated.requirements == ()
    assert negated.records == ()
    assert negated.to_dicts() == []
    assert negated.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "role_requirement": 0,
            "permission_scope": 0,
            "rbac": 0,
            "abac": 0,
            "least_privilege": 0,
            "admin_capability": 0,
            "delegation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
    }
    assert unrelated.records == ()
    assert invalid.source_brief_id is None
    assert invalid.records == ()
    assert "No source permission scope requirements were inferred" in negated.to_markdown()


def test_serialization_helpers_are_stable_and_do_not_mutate_inputs():
    source = _source_brief(
        source_id="permission-model",
        summary="RBAC requirements must include owner and viewer roles.",
        source_payload={
            "requirements": [
                "Least privilege permissions must be limited to project.read.",
                "Admins can delegate billing approval.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    obj = SimpleNamespace(
        id="object-permission",
        summary="ABAC policy must use department attributes for report export access.",
    )

    mapping_result = build_source_permission_scope_requirements(source)
    model_result = generate_source_permission_scope_requirements(model)
    derived_result = derive_source_permission_scope_requirements(model)
    iterable_result = build_source_permission_scope_requirements([model, obj])
    payload = source_permission_scope_requirements_to_dict(model_result)
    markdown = source_permission_scope_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_permission_scope_requirements_to_dict(mapping_result)
    assert derived_result.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_permission_scope_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_permission_scope_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_permission_scope_requirements(model_result) == model_result.summary
    assert iterable_result.source_brief_id is None
    assert iterable_result.summary["source_count"] == 2
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "summary",
        "evidence",
        "source_field",
        "source_path",
        "confidence",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Permission Scope Requirements Report: permission-model")
    assert "| Type | Summary | Source Field | Source Path | Confidence | Evidence |" in markdown


def _source_brief(
    *,
    source_id="permission-source",
    title="Permission scope requirements",
    domain="platform",
    summary="General permission scope requirements.",
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


def _implementation_brief(
    *,
    source_id="impl-permission",
    title="Authorization implementation",
    summary="Authorization implementation requirements.",
    scope=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "platform",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Implement authorization safely.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate authorization behavior.",
        "definition_of_done": [],
    }
