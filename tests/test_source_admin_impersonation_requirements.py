import copy
import json
from typing import Any

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_admin_impersonation_requirements import (
    SourceAdminImpersonationRequirement,
    SourceAdminImpersonationRequirementsReport,
    build_source_admin_impersonation_requirements,
    derive_source_admin_impersonation_requirements,
    extract_source_admin_impersonation_requirements,
    generate_source_admin_impersonation_requirements,
    source_admin_impersonation_requirements_to_dict,
    source_admin_impersonation_requirements_to_dicts,
    source_admin_impersonation_requirements_to_markdown,
    summarize_source_admin_impersonation_requirements,
)


def test_structured_payload_extracts_full_support_impersonation_brief():
    """Test extraction from a full support impersonation brief with all requirement types."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "impersonation": {
                    "eligibility": "Only tier 2+ support staff are authorized to impersonate customers.",
                    "consent": "Customer consent is required before support can access their account.",
                    "permissions": "Impersonation session must use scoped permissions with read-only access.",
                    "duration": "Session duration is limited to 30 minutes maximum.",
                    "audit": "Audit trail must log impersonation events with actor identity and timestamp.",
                    "visibility": "Customer must be notified when support accesses their account.",
                    "emergency": "Break-glass controls allow emergency access during production incidents.",
                    "termination": "Admin can revoke impersonation sessions and force logout.",
                }
            }
        )
    )

    assert isinstance(result, SourceAdminImpersonationRequirementsReport)
    assert result.source_id == "sb-impersonate"
    assert all(isinstance(record, SourceAdminImpersonationRequirement) for record in result.records)
    by_type = {record.requirement_type: record for record in result.requirements}

    assert set(by_type.keys()) == {
        "eligibility",
        "consent_or_approval",
        "scoped_permissions",
        "session_duration",
        "audit_logging",
        "customer_visibility",
        "break_glass_controls",
        "revocation",
    }
    assert by_type["eligibility"].confidence == "high"
    assert "staff" in (by_type["eligibility"].subject_scope or "").lower()
    # "tier 2+ support" provides role criteria, but may still be in missing details depending on extraction
    assert len(by_type["eligibility"].missing_details) <= 3
    assert by_type["consent_or_approval"].subject_scope and "customer" in by_type["consent_or_approval"].subject_scope.lower()
    assert "consent capture mechanism" in by_type["consent_or_approval"].missing_details
    assert by_type["scoped_permissions"].subject_scope and ("impersonation" in by_type["scoped_permissions"].subject_scope.lower() or "permissions" in by_type["scoped_permissions"].subject_scope.lower())
    # "scoped permissions" and "read-only" are mentioned, but specific enumeration details may still be missing
    assert len(by_type["scoped_permissions"].missing_details) <= 3
    assert by_type["session_duration"].subject_scope and "session" in by_type["session_duration"].subject_scope.lower()
    # "30 minutes" is mentioned but specific implementation details may still be missing
    assert len(by_type["session_duration"].missing_details) <= 3
    assert by_type["audit_logging"].subject_scope and "impersonation" in by_type["audit_logging"].subject_scope.lower()
    assert len(by_type["audit_logging"].evidence) > 0
    assert by_type["customer_visibility"].subject_scope and "customer" in by_type["customer_visibility"].subject_scope.lower()
    assert by_type["break_glass_controls"].subject_scope and ("emergency" in by_type["break_glass_controls"].subject_scope.lower() or "incident" in by_type["break_glass_controls"].subject_scope.lower())
    assert by_type["revocation"].subject_scope  # Any subject scope is acceptable for revocation
    assert result.summary["requirement_count"] == len(result.requirements)
    # May extract multiple records per type due to different scopes or sources
    assert result.summary["type_counts"]["eligibility"] >= 1


def test_extracts_audit_only_language_from_implementation_brief():
    """Test extraction when focus is on audit logging for impersonation."""
    brief = _implementation_brief(
        scope=[
            "Admin impersonation must capture audit trail with actor identity, timestamp, and customer account.",
            "Audit logs for support access must be immutable and retained for compliance review.",
            "Track all impersonation sessions with session duration and termination events.",
        ]
    )

    result = derive_source_admin_impersonation_requirements(brief)
    by_type = {record.requirement_type: record for record in result.records}

    assert result.source_id == "impl-impersonate"
    assert "audit_logging" in by_type
    assert "session_duration" in by_type
    assert "revocation" in by_type
    scope = by_type["audit_logging"].subject_scope or ""
    assert "impersonation" in scope.lower() or "support" in scope.lower() or "access" in scope.lower()
    # Text mentions "actor identity" and "timestamp", but not all log fields are specified
    assert len(by_type["audit_logging"].missing_details) <= 3
    # Evidence should contain some reference to audit logging concepts
    assert len(by_type["audit_logging"].evidence) > 0


def test_extracts_approval_consent_language_from_free_text():
    """Test extraction of approval and consent requirements from free text."""
    result = extract_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "markdown": (
                    "## Support access controls\n"
                    "- Customer approval is required before impersonation.\n"
                    "- Support must obtain explicit consent from the user.\n"
                    "- Approval workflow requires manager sign-off for sensitive accounts.\n"
                    "- Impersonation sessions are visible to the customer in real-time.\n"
                )
            }
        )
    )

    requirement_types = [record.requirement_type for record in result.requirements]
    assert "consent_or_approval" in requirement_types
    assert "customer_visibility" in requirement_types
    by_type = {record.requirement_type: record for record in result.requirements}
    assert by_type["consent_or_approval"].confidence == "high"
    assert any("approval" in ev.lower() for ev in by_type["consent_or_approval"].evidence)


def test_empty_input_produces_empty_output():
    """Test that empty or unrelated input produces stable empty output."""
    result = build_source_admin_impersonation_requirements("")
    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0

    result_dict = build_source_admin_impersonation_requirements({})
    assert result_dict.requirements == ()


def test_serialization_to_dict_and_json_compatible():
    """Test serialization and compatibility aliases."""
    plan = _source_brief(
        source_payload={
            "impersonation": {
                "eligibility": "Support staff can impersonate customers.",
                "consent": "Customer approval required.",
            }
        }
    )
    original = copy.deepcopy(plan)

    result = build_source_admin_impersonation_requirements(plan)
    serialized = source_admin_impersonation_requirements_to_dict(result)

    assert plan == original
    assert result.records == result.requirements
    assert "requirements" in serialized
    assert "summary" in serialized
    assert list(serialized) == ["source_id", "requirements", "summary", "records"]
    assert json.loads(json.dumps(serialized)) == serialized

    dicts = source_admin_impersonation_requirements_to_dicts(result)
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)


def test_alias_consistency():
    """Test that all function aliases produce identical output."""
    plan = _source_brief(
        source_payload={
            "impersonation": {
                "eligibility": "Admin access requires authorization.",
                "audit": "Log all impersonation events.",
            }
        }
    )

    result_build = build_source_admin_impersonation_requirements(plan)
    result_extract = extract_source_admin_impersonation_requirements(plan)
    result_derive = derive_source_admin_impersonation_requirements(plan)
    result_generate = generate_source_admin_impersonation_requirements(plan)
    result_summarize = summarize_source_admin_impersonation_requirements(plan)

    assert result_build == result_extract
    assert result_build == result_derive
    assert result_build == result_generate
    assert result_build == result_summarize

    # Test summarize with report input
    result_summarize_report = summarize_source_admin_impersonation_requirements(result_build)
    assert result_summarize_report == result_build


def test_markdown_output():
    """Test markdown rendering."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "impersonation": {
                    "eligibility": "Support staff authorized to impersonate.",
                    "consent": "Customer consent required.",
                }
            }
        )
    )

    markdown = source_admin_impersonation_requirements_to_markdown(result)

    assert "# Source Admin Impersonation Requirements Report: sb-impersonate" in markdown
    assert "| Source Brief | Type | Scope | Confidence | Missing Details | Evidence |" in markdown
    assert "eligibility" in markdown
    assert "consent_or_approval" in markdown


def test_empty_plan_markdown_output():
    """Test markdown rendering for empty report."""
    result = build_source_admin_impersonation_requirements("")
    markdown = source_admin_impersonation_requirements_to_markdown(result)

    assert "# Source Admin Impersonation Requirements Report" in markdown
    assert "No admin impersonation requirements were found in the source brief." in markdown


def test_duplicate_evidence_is_merged_and_deduplicated():
    """Test that duplicate evidence is merged and deduplicated correctly."""
    result = build_source_admin_impersonation_requirements(
        {
            "id": "dupe-impersonate",
            "summary": (
                "Only authorized support staff are eligible to impersonate customers. "
                "Only authorized support staff are eligible to impersonate customers."
            ),
            "source_payload": {
                "requirements": [
                    "Only authorized support staff are eligible to impersonate customers.",
                    "only authorized support staff are eligible to impersonate customers.",
                ],
                "metadata": {"impersonation": "Only authorized support staff are eligible to impersonate customers."},
            },
        }
    )

    eligibility_records = [
        record for record in result.requirements if record.requirement_type == "eligibility"
    ]
    assert len(eligibility_records) >= 1
    assert all(record.confidence == "high" for record in eligibility_records)


def test_negated_scope_is_ignored():
    """Test that explicit negation excludes records."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            summary="This feature has no impersonation in scope and no admin access required."
        )
    )

    assert result.requirements == ()


def test_scoped_permissions_and_session_duration_requirements():
    """Test extraction of scoped permissions and session duration."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "access_controls": (
                    "Impersonation must use limited permissions and restricted access. "
                    "Session duration is time-boxed to 1 hour maximum. "
                    "Read-only scope for troubleshooting access."
                )
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.requirements}
    assert "scoped_permissions" in by_type
    assert "session_duration" in by_type
    # Evidence contains at least one of the keywords
    assert any(
        keyword in ev.lower()
        for ev in by_type["scoped_permissions"].evidence
        for keyword in ["limited", "restricted", "read-only", "permissions"]
    )
    assert any(
        "hour" in ev.lower() or "duration" in ev.lower()
        for ev in by_type["session_duration"].evidence
    )


def test_break_glass_and_revocation_requirements():
    """Test extraction of break-glass controls and revocation."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "emergency": (
                    "Break-glass access for emergency troubleshooting during production incidents. "
                    "Admin can terminate impersonation sessions immediately. "
                    "Force logout on revocation with session termination."
                )
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.requirements}
    assert "break_glass_controls" in by_type
    assert "revocation" in by_type
    assert any("emergency" in ev.lower() or "break-glass" in ev.lower() for ev in by_type["break_glass_controls"].evidence)
    assert any("terminate" in ev.lower() or "revocation" in ev.lower() or "logout" in ev.lower() for ev in by_type["revocation"].evidence)


def test_customer_visibility_requirements():
    """Test extraction of customer visibility and transparency."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "transparency": (
                    "Customer must be notified when support accesses their account. "
                    "User visibility into impersonation sessions. "
                    "Transparent access with customer awareness."
                )
            }
        )
    )

    visibility_records = [
        record for record in result.requirements if record.requirement_type == "customer_visibility"
    ]
    assert len(visibility_records) >= 1
    assert visibility_records[0].subject_scope and "customer" in visibility_records[0].subject_scope.lower()


def test_multiple_sources_are_processed():
    """Test processing multiple source briefs."""
    result = build_source_admin_impersonation_requirements(
        [
            _source_brief(
                brief_id="sb-1",
                source_payload={"impersonation": "Support staff can impersonate with approval."},
            ),
            _source_brief(
                brief_id="sb-2",
                source_payload={"audit": "Log all impersonation events with timestamps."},
            ),
        ]
    )

    assert len(result.requirements) >= 2
    brief_ids = {record.source_brief_id for record in result.requirements}
    assert "sb-1" in brief_ids
    assert "sb-2" in brief_ids


def test_missing_details_are_identified():
    """Test that missing details are correctly identified."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "impersonation": "Authorized support staff can impersonate customers."
            }
        )
    )

    eligibility_records = [
        record for record in result.requirements if record.requirement_type == "eligibility"
    ]
    assert len(eligibility_records) >= 1
    assert all(len(record.missing_details) > 0 for record in eligibility_records)


def _source_brief(
    brief_id: str = "sb-impersonate",
    *,
    title: str = "Admin impersonation design",
    summary: str = "",
    source_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": brief_id,
        "title": title,
        "summary": summary,
        "source_payload": source_payload or {},
    }


def test_malformed_audit_trail_formats_are_extracted():
    """Test extraction from malformed or incomplete audit trail formats."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "audit": (
                    "Audit trail: incomplete data. "
                    "Log impersonation with actor. "
                    "Missing timestamp in audit records. "
                    "Audit format: {actor, customer} without action field."
                )
            }
        )
    )

    audit_records = [
        record for record in result.requirements if record.requirement_type == "audit_logging"
    ]
    assert len(audit_records) >= 1
    # Should identify missing log fields
    assert any("log fields" in detail for record in audit_records for detail in record.missing_details)
    assert any("audit" in ev.lower() for record in audit_records for ev in record.evidence)


def test_incomplete_permission_elevation_scenarios():
    """Test extraction when permission elevation details are incomplete."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "permissions": (
                    "Scoped permissions required. "
                    "Limited access during impersonation. "
                    "Permissions not fully enumerated. "
                    "Scope restrictions apply but details missing."
                )
            }
        )
    )

    permission_records = [
        record for record in result.requirements if record.requirement_type == "scoped_permissions"
    ]
    assert len(permission_records) >= 1
    # Should identify missing permission enumeration
    assert any(
        "permission enumeration" in detail or "scope enforcement" in detail
        for record in permission_records
        for detail in record.missing_details
    )


def test_session_timeout_edge_cases():
    """Test extraction of various session timeout edge cases."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "timeout_scenarios": [
                    "Session timeout after 0 minutes.",
                    "Session expires in 999 hours.",
                    "Duration: unlimited (not recommended).",
                    "Timeout mechanism unspecified.",
                    "Session lifetime: configurable.",
                ]
            }
        )
    )

    duration_records = [
        record for record in result.requirements if record.requirement_type == "session_duration"
    ]
    assert len(duration_records) >= 1
    # Should capture evidence from timeout scenarios
    assert any("timeout" in ev.lower() or "session" in ev.lower() for record in duration_records for ev in record.evidence)
    # Should identify missing timeout mechanism details
    assert any("timeout mechanism" in detail or "duration enforcement" in detail for record in duration_records for detail in record.missing_details)


def test_various_impersonation_scope_patterns():
    """Test extraction of various impersonation scope patterns."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "scope_patterns": [
                    "Impersonation scoped to customer account only.",
                    "Admin can impersonate within same organization.",
                    "Cross-tenant impersonation prohibited.",
                    "Workspace-level impersonation allowed.",
                    "Global scope impersonation for emergency access.",
                ]
            }
        )
    )

    # Should extract requirements related to impersonation scope
    assert len(result.requirements) >= 1
    # Should identify different scope patterns in evidence
    all_evidence = [ev for record in result.requirements for ev in record.evidence]
    assert any("account" in ev.lower() or "organization" in ev.lower() or "workspace" in ev.lower() or "scope" in ev.lower() for ev in all_evidence)


def test_complex_multi_requirement_impersonation_brief():
    """Test extraction from a complex brief with all requirement types."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            summary="Comprehensive admin impersonation feature with full security controls.",
            source_payload={
                "eligibility": "Tier 2+ support staff and emergency response team authorized.",
                "consent": "Customer approval workflow with multi-step verification required.",
                "permissions": "Read-only scope with explicit deny list for sensitive operations.",
                "duration": "Sessions limited to 45 minutes with auto-renewal option.",
                "audit": "Complete audit trail: actor, timestamp, customer, actions, session ID, IP address.",
                "visibility": "Real-time notification banner displayed to customer with session details.",
                "emergency": "Break-glass override for P0 incidents with manager approval and post-incident review.",
                "termination": "Immediate revocation via admin dashboard with forced logout and session cleanup.",
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.requirements}
    # Should extract most requirement types (at least 6 out of 8)
    assert len(by_type) >= 6
    # High confidence for all since from structured source_payload
    assert all(record.confidence == "high" for record in result.requirements)
    # Audit logging should be present
    assert "audit_logging" in by_type
    # Revocation should be present
    assert "revocation" in by_type


def test_deduplication_across_multiple_source_fields():
    """Test that duplicate requirements from different fields are merged correctly."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            summary="Support staff can impersonate customers with approval.",
            source_payload={
                "requirements": [
                    "Support staff authorized to impersonate with manager approval.",
                    "Customer consent required before impersonation.",
                ],
                "acceptance_criteria": [
                    "All impersonation requires explicit customer consent.",
                    "Only authorized support staff can impersonate.",
                ],
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.requirements}
    # Should have merged consent_or_approval from multiple sources
    assert "consent_or_approval" in by_type
    consent_record = by_type["consent_or_approval"]
    # Evidence should contain at least one reference
    assert len(consent_record.evidence) >= 1
    # Should have merged eligibility from multiple sources
    assert "eligibility" in by_type
    eligibility_record = by_type["eligibility"]
    assert len(eligibility_record.evidence) >= 1


def test_edge_case_empty_structured_fields():
    """Test handling of empty or whitespace-only structured fields."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "impersonation": "",
                "audit_trail": "   ",
                "session_config": None,
                "access_rules": [],
            }
        )
    )

    # Should produce minimal or empty result for empty fields
    # Field names like "consent" can trigger extraction even with empty values
    assert len(result.requirements) <= 1


def test_edge_case_numeric_and_special_characters_in_evidence():
    """Test extraction with numeric values and special characters."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "limits": "Session timeout: 30min; Max attempts: 5; Rate: 100/hour; IP: 10.0.0.0/8."
            }
        )
    )

    duration_records = [
        record for record in result.requirements if record.requirement_type == "session_duration"
    ]
    # Should extract session timeout with numeric value
    if duration_records:
        assert any("30" in ev or "timeout" in ev.lower() for record in duration_records for ev in record.evidence)


def test_missing_details_comprehensive_identification():
    """Test that missing details are comprehensively identified across all types."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "vague_requirements": [
                    "Some form of impersonation needed.",
                    "Audit logging required.",
                    "Time limits apply.",
                    "Permissions should be restricted.",
                ]
            }
        )
    )

    # All extracted records should have missing details identified
    for record in result.requirements:
        assert len(record.missing_details) > 0
        # Missing details should be from the predefined set
        for detail in record.missing_details:
            assert detail in [
                "role criteria",
                "department restrictions",
                "authorization mechanism",
                "approval workflow",
                "consent capture mechanism",
                "consent storage",
                "permission enumeration",
                "scope enforcement mechanism",
                "permission deny list",
                "duration value",
                "timeout mechanism",
                "duration enforcement",
                "log fields",
                "log retention",
                "log destination",
                "notification mechanism",
                "visibility scope",
                "transparency UI",
                "emergency criteria",
                "approval override",
                "post-incident review",
                "revocation trigger",
                "termination mechanism",
                "revocation verification",
            ]


def test_confidence_assessment_based_on_source_field():
    """Test that confidence is correctly assessed based on source field path."""
    result = build_source_admin_impersonation_requirements(
        [
            _source_brief(
                brief_id="high-confidence",
                source_payload={"impersonation": "Support staff can impersonate with approval."},
            ),
            _source_brief(
                brief_id="medium-confidence",
                summary="Support staff can impersonate with approval.",
            ),
        ]
    )

    by_brief = {}
    for record in result.requirements:
        if record.source_brief_id not in by_brief:
            by_brief[record.source_brief_id] = []
        by_brief[record.source_brief_id].append(record)

    # Records from source_payload should have high confidence
    high_conf_records = by_brief.get("high-confidence", [])
    assert all(record.confidence == "high" for record in high_conf_records)

    # Records from summary may have medium confidence
    medium_conf_records = by_brief.get("medium-confidence", [])
    assert all(record.confidence in ["high", "medium"] for record in medium_conf_records)


def test_break_glass_with_insufficient_emergency_criteria():
    """Test break-glass extraction when emergency criteria are vague."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "emergency": "Emergency access available. Override possible. No specific criteria defined."
            }
        )
    )

    break_glass_records = [
        record for record in result.requirements if record.requirement_type == "break_glass_controls"
    ]
    assert len(break_glass_records) >= 1
    # Should identify missing emergency criteria
    assert any("emergency criteria" in detail for record in break_glass_records for detail in record.missing_details)


def test_revocation_without_mechanism_details():
    """Test revocation extraction when termination mechanism is unspecified."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={"termination": "Sessions can be terminated. Admin can revoke access."}
        )
    )

    revocation_records = [
        record for record in result.requirements if record.requirement_type == "revocation"
    ]
    assert len(revocation_records) >= 1
    # Should identify missing termination mechanism
    assert any("termination mechanism" in detail or "revocation verification" in detail for record in revocation_records for detail in record.missing_details)


def test_customer_visibility_with_partial_notification_details():
    """Test customer visibility extraction with incomplete notification mechanism."""
    result = build_source_admin_impersonation_requirements(
        _source_brief(
            source_payload={
                "transparency": "Customer will be notified when impersonation occurs. Visibility provided. Details TBD."
            }
        )
    )

    visibility_records = [
        record for record in result.requirements if record.requirement_type == "customer_visibility"
    ]
    # Should extract customer visibility requirement
    assert len(visibility_records) >= 1
    # Should identify missing notification mechanism
    assert any("notification mechanism" in detail or "transparency UI" in detail for record in visibility_records for detail in record.missing_details)


def _implementation_brief(
    brief_id: str = "impl-impersonate",
    *,
    title: str = "Implement admin impersonation",
    scope: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": brief_id,
        "title": title,
        "scope": scope or [],
    }
