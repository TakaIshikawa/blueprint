import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_admin_console_requirements import (
    SourceAdminConsoleEvidenceGap,
    SourceAdminConsoleRequirement,
    SourceAdminConsoleRequirementsReport,
    build_source_admin_console_requirements,
    derive_source_admin_console_requirements,
    extract_source_admin_console_requirements,
    generate_source_admin_console_requirements,
    source_admin_console_requirements_to_dict,
    source_admin_console_requirements_to_dicts,
    source_admin_console_requirements_to_markdown,
    summarize_source_admin_console_requirements,
)


def test_structured_admin_console_payload_extracts_categories_in_order():
    result = build_source_admin_console_requirements(
        _source_brief(
            source_payload={
                "admin_console": {
                    "roles": "Admin roles must include super admin, tenant admin, and support agent boundaries.",
                    "permissions": "Permission scopes require RBAC, read-only access, and tenant scoped manage users rights.",
                    "audit": "Audit logging must record admin action log actor, target record, timestamp, and reason.",
                    "impersonation": "Impersonation constraints require login-as ticket id, approval required, and timeboxed session limit.",
                    "bulk": "Bulk actions need select all, preview step, confirmation, partial failure, and rollback handling.",
                    "search": "Admin console search and filters must support tenant id, email lookup, status filter, and saved views.",
                    "support": "Support workflows require support queue, case management, escalation, notes, and resolution state.",
                    "visibility": "Sensitive-data visibility must mask PII and require field-level access before reveal.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAdminConsoleRequirementsReport)
    assert all(isinstance(record, SourceAdminConsoleRequirement) for record in result.records)
    assert result.gaps == ()
    assert [record.category for record in result.records] == [
        "admin_roles",
        "permission_scopes",
        "audit_logging",
        "impersonation_constraints",
        "bulk_actions",
        "search_filters",
        "support_workflows",
        "sensitive_data_visibility",
    ]
    assert by_category["admin_roles"].value == "super admin, tenant admin, support agent"
    assert by_category["permission_scopes"].value == "RBAC, read-only, tenant scoped"
    assert by_category["audit_logging"].source_field == "source_payload.admin_console.audit"
    assert by_category["impersonation_constraints"].value == "login-as, ticket id, approval required"
    assert by_category["bulk_actions"].suggested_owners == ("product", "operations")
    assert by_category["sensitive_data_visibility"].suggested_owners == ("security", "privacy")
    assert result.summary["requirement_count"] == 8
    assert result.summary["evidence_gap_count"] == 0
    assert result.summary["status"] == "ready_for_planning"


def test_partial_admin_console_brief_flags_missing_role_boundaries_and_audit_expectations():
    result = build_source_admin_console_requirements(
        _source_brief(
            summary=(
                "Internal admin console must support bulk update of customer subscriptions. "
                "Support agents need search filters by tenant id and support ticket status."
            ),
        )
    )

    assert [record.category for record in result.records] == [
        "bulk_actions",
        "search_filters",
        "support_workflows",
    ]
    assert [gap.category for gap in result.evidence_gaps] == [
        "missing_role_boundaries",
        "missing_audit_expectations",
    ]
    assert all(isinstance(gap, SourceAdminConsoleEvidenceGap) for gap in result.evidence_gaps)
    assert result.summary["status"] == "needs_admin_console_detail"
    assert result.summary["evidence_gap_count"] == 2


def test_implementation_brief_plain_text_and_unrelated_end_user_brief_are_supported_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Admin console roles require super admin and read-only support agent access.",
            "Permission scopes must limit tenant scoped write access.",
        ],
        definition_of_done=[
            "Audit log records admin actions with actor, timestamp, target record, and reason.",
            "Sensitive data visibility masks PII unless a security admin approves reveal.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_admin_console_requirements(
        """
# Admin console

- Support workflow needs case management and escalation from the support queue.
- Impersonation must require login as customer with ticket id and reason capture.
"""
    )
    implementation_result = generate_source_admin_console_requirements(implementation)
    unrelated = build_source_admin_console_requirements(
        _source_brief(
            title="End-user profile improvements",
            summary="Customers need search filters for their order history and profile settings.",
        )
    )

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "impersonation_constraints",
        "support_workflows",
    ]
    assert {
        "admin_roles",
        "permission_scopes",
        "audit_logging",
        "sensitive_data_visibility",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.source_id == "implementation-admin-console"
    assert implementation_result.title == "Admin console implementation"
    assert unrelated.records == ()
    assert unrelated.evidence_gaps == ()


def test_sourcebrief_serialization_markdown_aliases_and_object_input_are_stable():
    source = _source_brief(
        source_id="admin-console-model",
        summary="Admin console requires super admin roles and audit log evidence.",
        source_payload={
            "requirements": [
                "Permission scopes must include RBAC and read-only support agent access.",
                "Support workflows need ticket id case management.",
            ]
        },
    )
    model = SourceBrief.model_validate(source)
    object_result = build_source_admin_console_requirements(
        SimpleNamespace(
            id="object-admin-console",
            summary="Internal console must show masked PII and support bulk export with confirmation.",
        )
    )

    result = build_source_admin_console_requirements(model)
    extracted = extract_source_admin_console_requirements(model)
    derived = derive_source_admin_console_requirements(model)
    payload = source_admin_console_requirements_to_dict(result)
    markdown = source_admin_console_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_admin_console_requirements(result) == result.summary
    assert source_admin_console_requirements_to_dicts(result) == payload["requirements"]
    assert source_admin_console_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.gaps == result.evidence_gaps
    assert list(payload) == [
        "source_id",
        "title",
        "requirements",
        "evidence_gaps",
        "summary",
        "records",
        "findings",
        "gaps",
    ]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "suggested_plan_impacts",
    ]
    assert "bulk_actions" in {record.category for record in object_result.records}
    assert "sensitive_data_visibility" in {record.category for record in object_result.records}
    assert markdown.startswith("# Source Admin Console Requirements Report: admin-console-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown


def test_irrelevant_negated_invalid_and_repeat_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-admin-console"
        summary = "Admin console requirements are out of scope and no audit log work is needed."

    empty = build_source_admin_console_requirements(
        _source_brief(source_id="empty-admin-console", summary="Update end-user dashboard copy and button labels only.")
    )
    repeat = build_source_admin_console_requirements(
        _source_brief(source_id="empty-admin-console", summary="Update end-user dashboard copy and button labels only.")
    )
    negated = build_source_admin_console_requirements(BriefLike())
    invalid = build_source_admin_console_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "admin_roles": 0,
            "permission_scopes": 0,
            "audit_logging": 0,
            "impersonation_constraints": 0,
            "bulk_actions": 0,
            "search_filters": 0,
            "support_workflows": 0,
            "sensitive_data_visibility": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "evidence_gap_count": 0,
        "evidence_gaps": [],
        "status": "no_admin_console_requirements_found",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-admin-console"
    assert empty.requirements == ()
    assert empty.evidence_gaps == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No admin console requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert negated.summary == expected_summary
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-admin-console",
    title="Admin console requirements",
    domain="operations",
    summary="General admin console requirements.",
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


def _implementation_brief(**overrides):
    payload = {
        "id": "implementation-admin-console",
        "source_brief_id": "source-admin-console",
        "title": "Admin console implementation",
        "domain": "operations",
        "target_user": "support admin",
        "buyer": "operations leadership",
        "workflow_context": "Internal admin console controls",
        "problem_statement": "Implement source-backed admin console planning support.",
        "mvp_goal": "Capture admin console requirements before task generation.",
        "product_surface": "admin console",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run admin console extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
    payload.update(overrides)
    return payload
