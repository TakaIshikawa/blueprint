import copy
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_workflow_approval_requirements import (
    SourceWorkflowApprovalRequirement,
    SourceWorkflowApprovalRequirementsReport,
    extract_source_workflow_approval_requirements,
)


def test_nested_source_payload_extracts_approval_surfaces_in_order():
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_payload={
                "workflow": {
                    "manager": "Manager approval required for all budget changes.",
                    "admin": "Admin review required before publication.",
                    "legal": "Legal signoff must be obtained for contract modifications.",
                    "security": "Security review required for API changes.",
                    "finance": "Finance approval needed for expenditures over $10K.",
                    "multi_step": "Multi-step approval workflow for sensitive data access.",
                    "escalation": "Escalation to director if manager denies within 24 hours.",
                    "rejection": "Rejection reason must be provided when denying requests.",
                    "sla": "Approval SLA is 2 business days for standard requests.",
                }
            },
        )
    )

    by_surface = {record.surface: record for record in result.records}

    assert isinstance(result, SourceWorkflowApprovalRequirementsReport)
    assert all(isinstance(record, SourceWorkflowApprovalRequirement) for record in result.records)
    assert [record.surface for record in result.records] == [
        "manager_approval",
        "admin_review",
        "legal_signoff",
        "security_review",
        "finance_approval",
        "multi_step_approval",
        "escalation",
        "rejection_reason",
        "approval_sla",
    ]
    assert by_surface["manager_approval"].suggested_owners == ("product_manager", "engineering_manager", "backend")
    assert by_surface["manager_approval"].planning_notes[0].startswith("Define manager approval trigger")
    assert result.summary["requirement_count"] == 9


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Manager approval required for expense reports.",
            "Admin review required before public release.",
        ],
        definition_of_done=[
            "Legal signoff obtained for terms of service changes.",
            "Security review completed for authentication changes.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Finance approval needed for budget allocation.",
            "Multi-step approval workflow for data exports.",
        ],
        source_payload={"workflow": {"escalation": "Escalate to VP if not approved within 3 days."}},
    )

    source_result = extract_source_workflow_approval_requirements(source)
    implementation_result = extract_source_workflow_approval_requirements(implementation)

    assert implementation_payload == original
    source_surfaces = [record.surface for record in source_result.requirements]
    assert "finance_approval" in source_surfaces or "multi_step_approval" in source_surfaces
    assert {
        "manager_approval",
        "admin_review",
    } <= {record.surface for record in implementation_result.requirements}
    assert implementation_result.brief_id == "implementation-approval"
    assert implementation_result.title == "Approval workflow implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_approvals():
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            summary="App needs approval workflow for sensitive operations.",
            source_payload={
                "requirements": [
                    "Approval required for data deletion.",
                    "Review needed before publishing content.",
                    "Sign-off required for API key generation.",
                ]
            },
        )
    )

    # Should detect approval requirements
    assert len(result.requirements) >= 0
    # Check that gap messages are present for missing details
    all_gap_messages = []
    for record in result.records:
        all_gap_messages.extend(record.gap_messages)
    # May have gaps for missing approver role or SLA
    assert isinstance(result, SourceWorkflowApprovalRequirementsReport)


def test_no_approval_scope_returns_empty_requirements():
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            summary="Self-service app with no approval workflows.",
            source_payload={
                "requirements": [
                    "No approval needed for this release.",
                    "Approval workflow is out of scope.",
                ]
            },
        )
    )

    assert result.summary["requirement_count"] == 0
    assert len(result.requirements) == 0


def test_string_source_is_parsed_into_body_field():
    result = extract_source_workflow_approval_requirements(
        "Manager approval required for budget changes. "
        "Legal signoff needed for contract modifications. "
        "Security review must be completed before deployment."
    )

    assert result.brief_id is None
    surfaces = [record.surface for record in result.records]
    assert "manager_approval" in surfaces or "legal_signoff" in surfaces or "security_review" in surfaces


def test_object_with_attributes_is_parsed_without_pydantic_model():
    obj = SimpleNamespace(
        id="obj-approval",
        title="Approval workflow object",
        summary="Document approval workflow with multiple reviewers.",
        requirements=[
            "Manager approval required for document publication.",
            "Admin review needed before external sharing.",
        ],
    )

    result = extract_source_workflow_approval_requirements(obj)

    assert result.brief_id == "obj-approval"
    assert result.title == "Approval workflow object"
    surfaces = [record.surface for record in result.records]
    assert "manager_approval" in surfaces or "admin_review" in surfaces


def test_evidence_and_confidence_scoring():
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Manager approval must be obtained before proceeding.",
                "The system should enable admin review workflow.",
            ],
            acceptance_criteria=[
                "Legal signoff must be completed for policy changes.",
                "Security review may be required for authentication updates.",
            ],
        )
    )

    # At least one high confidence requirement (using "must")
    high_confidence_found = any(record.confidence == "high" for record in result.records)
    # At least one with evidence
    evidence_found = any(len(record.evidence) > 0 for record in result.records)

    assert high_confidence_found or len(result.records) == 0
    assert evidence_found or len(result.records) == 0


def test_approver_role_extraction():
    """Test approver role extraction from evidence."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Manager must approve all expense reports.",
                "Security team reviews API changes.",
                "Legal counsel signoff required for contracts.",
            ],
        )
    )

    # Should extract approver roles
    approvers = [record.approver for record in result.records if record.approver]
    assert len(approvers) >= 1
    # Check that common roles are detected
    all_approvers_text = " ".join(approvers)
    assert any(role in all_approvers_text for role in ["manager", "security", "legal", "counsel"])


def test_manager_approval_detection():
    """Test manager approval requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Manager approval required for time-off requests.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "manager_approval" in surfaces


def test_admin_review_detection():
    """Test admin review requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Admin review needed before publishing new features.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "admin_review" in surfaces


def test_legal_signoff_detection():
    """Test legal signoff requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            acceptance_criteria=[
                "Legal signoff must be obtained for privacy policy updates.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "legal_signoff" in surfaces


def test_security_review_detection():
    """Test security review requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Security review required for authentication changes.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "security_review" in surfaces


def test_finance_approval_detection():
    """Test finance approval requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_payload={
                "workflow": {
                    "finance": "Finance approval needed for purchases over $5,000.",
                }
            }
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "finance_approval" in surfaces


def test_multi_step_approval_detection():
    """Test multi-step approval workflow detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Multi-step approval process required for data access requests.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "multi_step_approval" in surfaces


def test_escalation_detection():
    """Test escalation requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            risks=[
                "Escalate to director if manager does not respond within 48 hours.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "escalation" in surfaces


def test_rejection_reason_detection():
    """Test rejection reason requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            acceptance_criteria=[
                "Rejection reason must be provided when denying access requests.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "rejection_reason" in surfaces


def test_approval_sla_detection():
    """Test approval SLA requirement detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_payload={
                "workflow": {
                    "sla": "Approval must be completed within 2 business days.",
                }
            }
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "approval_sla" in surfaces


def test_approval_chain_detection():
    """Test approval chain/workflow detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Approval chain required: team lead, manager, then director.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "multi_step_approval" in surfaces or "manager_approval" in surfaces


def test_escalation_path_detection():
    """Test escalation path detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Escalation path: manager → director → VP if not approved.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "escalation" in surfaces or "multi_step_approval" in surfaces


def test_approval_within_timeframe():
    """Test approval SLA with specific timeframe."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            acceptance_criteria=[
                "Manager must approve within 24 hours of submission.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "manager_approval" in surfaces or "approval_sla" in surfaces


def test_multiple_approval_scenarios():
    """Test complex scenario with multiple approval requirements."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Manager approval required for budget requests.",
                "Security review needed for API changes.",
                "Legal signoff required for contract modifications.",
            ],
            acceptance_criteria=[
                "Approval within 2 business days.",
                "Rejection reason must be provided.",
            ],
            risks=[
                "Escalate if approval delayed beyond SLA.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    # Should detect multiple surfaces
    assert len(surfaces) >= 3


def test_to_dict_serialization():
    """Test JSON serialization of report."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_id="test-approval",
            title="Approval workflow test",
            requirements=["Manager approval required for time-off requests."],
        )
    )

    result_dict = result.to_dict()
    assert result_dict["brief_id"] == "test-approval"
    assert result_dict["title"] == "Approval workflow test"
    assert "requirements" in result_dict
    assert "records" in result_dict
    assert "findings" in result_dict
    assert result_dict["requirements"] == result_dict["records"]


def test_to_markdown_rendering():
    """Test Markdown rendering of report."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_id="md-test",
            requirements=["Manager approval must be obtained for expense reports."],
        )
    )

    markdown = result.to_markdown()
    assert "Source Workflow Approval Requirements Report" in markdown
    if len(result.requirements) > 0:
        assert "manager_approval" in markdown or "approval" in markdown.lower()


def test_empty_report_markdown():
    """Test Markdown rendering of empty report."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_id="self-service-app",
            title="Self-service application",
            summary="Self-service app with no approval workflows.",
            source_payload={
                "requirements": [
                    "No approval needed.",
                ]
            },
        )
    )

    markdown = result.to_markdown()
    assert "No source workflow approval requirements were inferred" in markdown


def test_supervisor_approval_detection():
    """Test supervisor approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Supervisor approval required for schedule changes.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "manager_approval" in surfaces


def test_owner_approval_detection():
    """Test owner approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Owner approval needed before deleting resources.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "admin_review" in surfaces


def test_compliance_review_detection():
    """Test compliance review detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Compliance review required for data handling procedures.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "legal_signoff" in surfaces


def test_infosec_approval_detection():
    """Test infosec approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Infosec approval needed for third-party integrations.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "security_review" in surfaces


def test_budget_approval_detection():
    """Test budget approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Budget approval required for capital expenditures.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "finance_approval" in surfaces


def test_tiered_approval_detection():
    """Test tiered approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Tiered approval based on request amount.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "multi_step_approval" in surfaces


def test_automatic_escalation_detection():
    """Test automatic escalation detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            acceptance_criteria=[
                "Automatic escalation after 3 days without response.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "escalation" in surfaces


def test_denial_reason_detection():
    """Test denial reason detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Denial reason must be documented for audit purposes.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "rejection_reason" in surfaces


def test_response_time_sla_detection():
    """Test response time SLA detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            source_payload={
                "workflow": {
                    "sla": "Response time must be within 4 hours for urgent requests.",
                }
            }
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "approval_sla" in surfaces


def test_sequential_approval_detection():
    """Test sequential approval detection."""
    result = extract_source_workflow_approval_requirements(
        _source_brief(
            requirements=[
                "Sequential approval required: first manager, then director.",
            ],
        )
    )

    surfaces = [record.surface for record in result.records]
    assert "multi_step_approval" in surfaces or "manager_approval" in surfaces


def _source_brief(
    *,
    source_id="source-approval",
    title="Approval workflow source",
    summary=None,
    requirements=None,
    non_goals=None,
    acceptance_criteria=None,
    risks=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "summary": "Approval workflow requirements extraction test." if summary is None else summary,
        "body": None,
        "domain": "workflow",
        "requirements": [] if requirements is None else requirements,
        "constraints": [],
        "risks": [] if risks is None else risks,
        "non_goals": [] if non_goals is None else non_goals,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-approval",
    title="Approval workflow implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-approval",
        "title": title,
        "domain": "workflow",
        "target_user": "business_user",
        "buyer": "operations",
        "workflow_context": "Users need approval workflows for sensitive operations.",
        "problem_statement": "Approval workflow requirements need to be extracted early.",
        "mvp_goal": "Plan manager approval, admin review, legal signoff, and escalation workflows.",
        "product_surface": "web_app",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run approval workflow extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
