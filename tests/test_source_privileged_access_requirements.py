import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_privileged_access_requirements import (
    SourcePrivilegedAccessRequirement,
    SourcePrivilegedAccessRequirementsReport,
    build_source_privileged_access_requirements,
    derive_source_privileged_access_requirements,
    extract_source_privileged_access_requirements,
    generate_source_privileged_access_requirements,
    source_privileged_access_requirements_to_dict,
    source_privileged_access_requirements_to_dicts,
    source_privileged_access_requirements_to_markdown,
    summarize_source_privileged_access_requirements,
)


def test_structured_source_payload_extracts_privileged_access_categories_in_order():
    result = build_source_privileged_access_requirements(
        _source_brief(
            source_payload={
                "privileged_access": {
                    "approval": "Privileged access requires manager approval and security approval before grant.",
                    "time_bound": "Admin elevation must be time-bound with a 4 hour expiry window.",
                    "audit": "Audit trail records actor, reason code, approver, and timestamp.",
                    "break_glass": "Break-glass emergency access is required for Sev 1 production incidents.",
                    "routine": "Routine role elevation grants support admin role for approved maintenance.",
                    "revocation": "Privileged access revocation must remove access and terminate sessions.",
                    "monitoring": "Monitoring must alert on risky admin actions and privileged sessions.",
                }
            }
        )
    )

    assert isinstance(result, SourcePrivilegedAccessRequirementsReport)
    assert all(isinstance(record, SourcePrivilegedAccessRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "approver_workflow",
        "time_bound_elevation",
        "audit_trail",
        "emergency_access",
        "routine_role_elevation",
        "revocation",
        "monitoring",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["emergency_access"].severity == "critical"
    assert by_category["routine_role_elevation"].severity == "medium"
    assert by_category["time_bound_elevation"].confidence == "high"
    assert by_category["audit_trail"].source_field == "source_payload.privileged_access.audit"
    assert "Break-glass emergency access" in by_category["emergency_access"].evidence[0]
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["emergency_access"] == 1
    assert result.summary["severity_counts"]["critical"] == 1
    assert result.summary["status"] == "ready_for_privileged_access_planning"


def test_prose_brief_distinguishes_break_glass_from_routine_role_elevation():
    result = build_source_privileged_access_requirements(
        _source_brief(
            source_payload={
                "body": """
# Privileged access rollout

- Break-glass emergency admin access must be available during production incidents and alert security operations.
- Routine role elevation should grant temporary support admin privileges for maintenance tickets.
- Access approval requires business justification and security approval.
- Elevated access must auto-revoke when the access window ends.
"""
            }
        )
    )

    by_category = {record.category: record for record in result.records}
    assert [record.category for record in result.records] == [
        "approver_workflow",
        "time_bound_elevation",
        "emergency_access",
        "routine_role_elevation",
        "revocation",
        "monitoring",
    ]
    assert "emergency admin access" in by_category["emergency_access"].requirement_text
    assert "Routine role elevation" in by_category["routine_role_elevation"].requirement_text
    assert by_category["emergency_access"].severity == "critical"
    assert by_category["routine_role_elevation"].severity == "medium"
    assert by_category["monitoring"].matched_terms


def test_implementation_brief_object_and_plain_string_inputs_are_supported_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Privileged access requests require approver workflow and business justification.",
            "Just-in-time admin elevation must expire after 2 hours.",
        ],
        definition_of_done=[
            "Audit logs record privileged access grant and revocation events.",
            "Monitoring alerts on emergency access and risky admin actions.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = generate_source_privileged_access_requirements(
        ImplementationBrief.model_validate(implementation_payload)
    )
    object_result = build_source_privileged_access_requirements(
        SimpleNamespace(
            id="object-privileged-access",
            security="Break-glass access must require audit trail and post-access review.",
        )
    )
    text_result = build_source_privileged_access_requirements(
        "Privileged access must support time-bound elevation and revocation."
    )

    assert implementation_payload == original
    assert implementation.source_id == "implementation-privileged-access"
    assert [record.category for record in implementation.records] == [
        "approver_workflow",
        "time_bound_elevation",
        "audit_trail",
        "emergency_access",
        "routine_role_elevation",
        "revocation",
        "monitoring",
    ]
    assert [record.category for record in object_result.records] == [
        "audit_trail",
        "emergency_access",
        "monitoring",
    ]
    assert [record.category for record in text_result.records] == ["time_bound_elevation", "revocation"]


def test_empty_negated_malformed_and_unrelated_inputs_return_stable_empty_reports():
    generic = build_source_privileged_access_requirements(
        _source_brief(
            title="Admin page copy",
            summary="Admin dashboard layout and role description copy should be updated.",
            source_payload={"requirements": ["Update high priority banner text and monitor size labels."]},
        )
    )
    negated = build_source_privileged_access_requirements(
        _source_brief(
            summary="No privileged access, admin elevation, break-glass, revocation, or monitoring work is required.",
            source_payload={"requirements": ["Update help text only."]},
        )
    )
    no_scope = build_source_privileged_access_requirements(
        _source_brief(summary="Break-glass access is out of scope and no privileged access support is planned.")
    )
    malformed = build_source_privileged_access_requirements({"source_payload": {"privileged_access": {"notes": object()}}})
    blank = build_source_privileged_access_requirements("")
    invalid = build_source_privileged_access_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "approver_workflow": 0,
            "time_bound_elevation": 0,
            "audit_trail": 0,
            "emergency_access": 0,
            "routine_role_elevation": 0,
            "revocation": 0,
            "monitoring": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "severity_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0},
        "status": "no_privileged_access_language",
    }
    assert generic.records == ()
    assert negated.records == ()
    assert no_scope.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert generic.summary == expected_summary
    assert "No source privileged access requirements were inferred." in generic.to_markdown()


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="privileged-access-model",
        source_payload={
            "requirements": [
                "Privileged access approval must require security | manager approval.",
                "Privileged access approval must require security | manager approval.",
                "Admin elevation must expire after 8 hours and audit logs record approver evidence.",
            ],
            "acceptance_criteria": [
                "Privileged access approval must require security | manager approval.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_privileged_access_requirements(source)
    model_result = extract_source_privileged_access_requirements(model)
    derived = derive_source_privileged_access_requirements(model)
    generated = generate_source_privileged_access_requirements(model)
    payload = source_privileged_access_requirements_to_dict(model_result)
    markdown = source_privileged_access_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_privileged_access_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_privileged_access_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_privileged_access_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_privileged_access_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "required_capability",
        "requirement_text",
        "evidence",
        "confidence",
        "severity",
        "source_field",
        "source_fields",
        "matched_terms",
    ]
    approval = model_result.records[0]
    assert approval.requirement_category == "approver_workflow"
    assert approval.requirement_type == "approver_workflow"
    assert approval.access_dimension == "approver_workflow"
    assert approval.evidence == (
        "source_payload.acceptance_criteria[0]: Privileged access approval must require security | manager approval.",
        "source_payload.requirements[2]: Admin elevation must expire after 8 hours and audit logs record approver evidence.",
    )
    assert approval.source_fields == (
        "source_payload.acceptance_criteria[0]",
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
        "source_payload.requirements[2]",
    )
    assert [record.category for record in model_result.records] == [
        "approver_workflow",
        "time_bound_elevation",
        "audit_trail",
        "routine_role_elevation",
    ]
    assert "security \\| manager approval" in markdown
    assert markdown.startswith("# Source Privileged Access Requirements Report: privileged-access-model")


def _source_brief(
    *,
    source_id="source-privileged-access",
    title="Privileged access requirements",
    domain="security",
    summary="General privileged access requirements.",
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
        "id": "implementation-privileged-access",
        "source_brief_id": "source-privileged-access",
        "title": "Privileged access rollout",
        "domain": "security",
        "target_user": "platform operator",
        "buyer": "security",
        "workflow_context": "Teams need privileged access requirements before implementation planning.",
        "problem_statement": "Privileged access requirements need to be extracted early.",
        "mvp_goal": "Plan privileged access and break-glass controls from source briefs.",
        "product_surface": "admin console",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run privileged access extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
