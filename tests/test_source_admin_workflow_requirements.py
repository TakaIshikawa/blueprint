import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_admin_workflow_requirements import (
    SourceAdminWorkflowRequirement,
    SourceAdminWorkflowRequirementsReport,
    build_source_admin_workflow_requirements,
    derive_source_admin_workflow_requirements,
    extract_source_admin_workflow_requirements,
    generate_source_admin_workflow_requirements,
    source_admin_workflow_requirements_to_dict,
    source_admin_workflow_requirements_to_dicts,
    source_admin_workflow_requirements_to_markdown,
    summarize_source_admin_workflow_requirements,
)


def test_explicit_payload_fields_extract_admin_workflow_categories():
    result = build_source_admin_workflow_requirements(
        _source_brief(
            source_payload={
                "admin_workflows": {
                    "moderation": "Moderators must review flagged content and ban users.",
                    "impersonation": (
                        "Support login as user requires ticket id, reason capture, timeboxed session, and audit log."
                    ),
                    "approval": "Refund requests require manager approval before payout.",
                    "override": "Admin override must capture justification and expire after 1 hour.",
                    "dashboard": "Ops dashboard needs filters for tenant support cases.",
                    "bulk_action": (
                        "Bulk approve flagged posts with preview step and rollback recovery."
                    ),
                    "support_operation": "Support agents resolve disputes from linked support tickets.",
                    "audit_expectation": (
                        "Audit trail records who performed each admin action with UTC timestamp."
                    ),
                }
            }
        )
    )

    assert isinstance(result, SourceAdminWorkflowRequirementsReport)
    assert result.source_id == "sb-admin"
    assert all(isinstance(record, SourceAdminWorkflowRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "moderation",
        "impersonation",
        "approval",
        "override",
        "dashboard",
        "bulk_action",
        "support_operation",
        "audit_expectation",
    ]

    by_category = {record.category: record for record in result.records}
    assert by_category["moderation"].confidence == "high"
    assert by_category["impersonation"].subject_scope == "user"
    assert "reason capture" not in by_category["impersonation"].missing_details
    assert "session limit" not in by_category["impersonation"].missing_details
    assert "override reason" not in by_category["override"].missing_details
    assert "filters" not in by_category["dashboard"].missing_details
    assert "preview step" not in by_category["bulk_action"].missing_details
    assert "undo or recovery" not in by_category["bulk_action"].missing_details
    assert "ticket linkage" not in by_category["support_operation"].missing_details
    assert "actor identity" not in by_category["audit_expectation"].missing_details
    assert "timestamp source" not in by_category["audit_expectation"].missing_details
    assert any(
        "source_payload.admin_workflows.impersonation" in item
        for item in by_category["impersonation"].evidence
    )
    assert result.summary["requirement_count"] == len(result.records)
    assert result.summary["category_counts"]["bulk_action"] == 1
    assert result.summary["categories"] == [
        "moderation",
        "impersonation",
        "approval",
        "override",
        "dashboard",
        "bulk_action",
        "support_operation",
        "audit_expectation",
    ]


def test_natural_language_brief_extracts_source_level_admin_requirements():
    result = extract_source_admin_workflow_requirements(
        _source_brief(
            summary=(
                "Before launch, build an internal console for support agents. "
                "Customer success needs delegated access to customer accounts with a ticket id. "
                "High value refunds go through an approval queue. "
                "Admins need bulk update of subscription settings."
            )
        )
    )

    assert [(record.category, record.subject_scope) for record in result.records] == [
        ("impersonation", "customer accounts"),
        ("approval", "approval queue"),
        ("dashboard", "internal console"),
        ("bulk_action", "subscription settings"),
        ("support_operation", "support agents"),
    ]
    assert result.records[0].confidence == "high"


def test_empty_malformed_string_and_object_like_inputs_do_not_raise():
    class BriefLike:
        id = "object-admin"
        summary = "Support dashboard should show ticket workflow status."

    empty = build_source_admin_workflow_requirements(
        _source_brief(summary="No admin workflow or support operation changes are in scope.")
    )
    malformed = build_source_admin_workflow_requirements({"source_payload": {"notes": object()}})
    text = build_source_admin_workflow_requirements(
        "Moderation dashboard must support bulk reject actions."
    )
    object_like = build_source_admin_workflow_requirements(BriefLike())

    assert empty.source_id == "sb-admin"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "moderation": 0,
            "impersonation": 0,
            "approval": 0,
            "override": 0,
            "dashboard": 0,
            "bulk_action": 0,
            "support_operation": 0,
            "audit_expectation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No admin workflow requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert [record.category for record in text.records] == [
        "moderation",
        "dashboard",
        "bulk_action",
    ]
    assert [record.category for record in object_like.records] == [
        "dashboard",
        "support_operation",
    ]


def test_sourcebrief_model_input_aliases_json_serialization_and_no_mutation():
    source = _source_brief(
        source_id="admin-model",
        summary="Admin console must support policy override workflow with justification.",
        source_payload={
            "requirements": [
                "Support agents need login as user from a linked support ticket.",
                "Audit log must record admin actions.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_admin_workflow_requirements(source)
    model_result = generate_source_admin_workflow_requirements(model)
    derived = derive_source_admin_workflow_requirements(model)
    payload = source_admin_workflow_requirements_to_dict(model_result)

    assert source == original
    assert payload == source_admin_workflow_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_admin_workflow_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_admin_workflow_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_admin_workflow_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "subject_scope",
        "missing_details",
        "confidence",
        "evidence",
    ]


def test_stable_ordering_confidence_summary_and_markdown_escaping():
    result = build_source_admin_workflow_requirements(
        [
            _source_brief(
                source_id="brief-b",
                summary="Support | operation needs an admin dashboard for ticket workflow.",
            ),
            _source_brief(
                source_id="brief-a",
                summary=(
                    "Moderation | queue must support bulk action review. "
                    "Admin actions need audit trail."
                ),
            ),
        ]
    )
    markdown = source_admin_workflow_requirements_to_markdown(result)

    assert [(record.source_brief_id, record.category) for record in result.records] == [
        ("brief-a", "moderation"),
        ("brief-a", "bulk_action"),
        ("brief-a", "audit_expectation"),
        ("brief-b", "dashboard"),
        ("brief-b", "support_operation"),
    ]
    assert result.source_id is None
    assert result.summary["source_count"] == 2
    assert result.summary["confidence_counts"]["high"] == 5
    assert result.summary["confidence_counts"]["medium"] == 0
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Admin Workflow Requirements Report")
    assert "| Source Brief | Category | Scope | Confidence | Missing Details | Evidence |" in markdown
    assert "Moderation \\| queue" in markdown
    assert "Support \\| operation" in markdown


def _source_brief(
    *,
    source_id="sb-admin",
    title="Admin workflow requirements",
    domain="operations",
    summary="General admin workflow requirements.",
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
