import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_escalation_policy_requirements import (
    SourceEscalationPolicyRequirement,
    SourceEscalationPolicyRequirementsReport,
    build_source_escalation_policy_requirements,
    derive_source_escalation_policy_requirements,
    extract_source_escalation_policy_requirements,
    generate_source_escalation_policy_requirements,
    source_escalation_policy_requirements_to_dict,
    source_escalation_policy_requirements_to_dicts,
    source_escalation_policy_requirements_to_markdown,
    summarize_source_escalation_policy_requirements,
)


def test_structured_fields_extract_escalation_policy_requirements_in_stable_order():
    result = build_source_escalation_policy_requirements(
        _source_brief(
            source_payload={
                "support": {
                    "severity": "Severity levels must include Sev1 for production down and P2 for degraded customer workflows.",
                    "routing": "Escalate Sev1 to PagerDuty on-call and the incident commander.",
                    "handoff": "Support handoff must transfer ownership to Engineering Operations after triage.",
                },
                "incident": {
                    "response": "First response target is within 15 minutes and status updates every hour.",
                    "customer_notice": "Customer notification must use status page and customer email for Sev1 incidents.",
                },
                "operations": {
                    "policy_gap": "Open question: who owns after-hours escalation when on-call is unavailable?",
                },
            }
        )
    )

    assert isinstance(result, SourceEscalationPolicyRequirementsReport)
    assert all(isinstance(record, SourceEscalationPolicyRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "severity_level",
        "routing_path",
        "ownership_handoff",
        "response_target",
        "customer_notification",
        "unresolved_policy_gap",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["severity_level"].source_field == "source_payload.support.severity"
    assert by_type["severity_level"].matched_terms == ("severity levels", "sev1", "p2")
    assert by_type["routing_path"].value == "pagerduty"
    assert by_type["ownership_handoff"].value == "Engineering Operations after triage"
    assert by_type["response_target"].confidence == "high"
    assert by_type["customer_notification"].suggested_plan_impacts[0].startswith(
        "Plan customer-facing notification triggers"
    )
    assert result.summary["requirement_count"] == 6
    assert result.summary["requirement_type_counts"]["routing_path"] == 1
    assert result.summary["confidence_counts"]["high"] == 6
    assert result.summary["status"] == "ready_for_escalation_policy_planning"


def test_markdown_bullets_extract_severity_support_handoff_and_dedupe_evidence():
    result = build_source_escalation_policy_requirements(
        _source_brief(
            source_id="escalation-bullets",
            source_payload={
                "body": """
# Escalation policy

- Critical customer-impacting incidents must be classified as Sev1.
- Critical customer-impacting incidents must be classified as Sev1.
- Support handoff should route tier 2 tickets to the on-call operations owner.
- Response target should acknowledge escalations within 30 minutes.
- Customer comms should post status page updates for Sev1 | outage events.
"""
            },
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "severity_level",
        "routing_path",
        "ownership_handoff",
        "response_target",
        "customer_notification",
    ]
    severity = result.records[0]
    assert severity.evidence == (
        "source_payload.body: Critical customer-impacting incidents must be classified as Sev1.",
    )
    assert "sev1" in severity.matched_terms
    assert result.records[1].matched_terms == ("route tier", "on-call")
    markdown = result.to_markdown()
    assert "| Requirement Type | Value | Confidence | Source Field | Matched Terms | Evidence | Suggested Plan Impacts |" in markdown
    assert "Sev1 \\| outage events" in markdown


def test_structured_metadata_fields_are_scanned_and_confidence_prefers_requirement_fields():
    result = build_source_escalation_policy_requirements(
        {
            "id": "structured-escalation",
            "title": "Support escalation",
            "summary": "Escalation policy needs support routing and response targets.",
            "metadata": {
                "support": {
                    "routing": "Tier 3 support queue routes to OpsGenie on-call.",
                },
                "incident": {
                    "response_target": "Incident acknowledgement SLA is 10 minutes.",
                },
            },
            "acceptance_criteria": [
                "Escalation policy must notify customers through status page for Sev1 outages.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["routing_path"].source_field == "metadata.support.routing"
    assert by_type["routing_path"].confidence == "high"
    assert by_type["response_target"].value == "sla"
    assert by_type["customer_notification"].source_field == "acceptance_criteria[0]"
    assert by_type["customer_notification"].confidence == "high"


def test_non_escalation_negated_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-escalation"
        summary = "No escalation, incident, support handoff, or customer notification work is required for this release."

    empty = build_source_escalation_policy_requirements(
        _source_brief(
            title="Copy update",
            summary="No escalation, incident, support handoff, or customer notification work is required for this release.",
            source_payload={"requirements": ["Update onboarding copy and invoice labels."]},
        )
    )
    repeat = build_source_escalation_policy_requirements(
        _source_brief(
            title="Copy update",
            summary="No escalation, incident, support handoff, or customer notification work is required for this release.",
            source_payload={"requirements": ["Update onboarding copy and invoice labels."]},
        )
    )
    negated = build_source_escalation_policy_requirements(BriefLike())
    malformed = build_source_escalation_policy_requirements({"source_payload": {"notes": object()}})
    blank = build_source_escalation_policy_requirements("")

    expected_summary = {
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "severity_level": 0,
            "routing_path": 0,
            "ownership_handoff": 0,
            "response_target": 0,
            "customer_notification": 0,
            "unresolved_policy_gap": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_escalation_policy_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert negated.summary == expected_summary
    assert malformed.records == ()
    assert blank.records == ()
    assert "No source escalation policy requirements were inferred." in empty.to_markdown()


def test_serialization_aliases_json_markdown_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="escalation-model",
        title="Escalation source",
        summary="Escalation policy requirements include severity, routing, and customer comms.",
        source_payload={
            "requirements": [
                "Severity matrix must define Sev2 for partial outage.",
                "Escalate to #support-escalations and PagerDuty on-call.",
                "Customer notification must use status page for customer | partner incidents.",
            ],
            "metadata": {
                "policy_gap": "TBD: clarify which team owns partner escalation.",
            },
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    result = build_source_escalation_policy_requirements(model)
    mapping_result = generate_source_escalation_policy_requirements(source)
    derived = derive_source_escalation_policy_requirements(model)
    extracted = extract_source_escalation_policy_requirements(model)
    payload = source_escalation_policy_requirements_to_dict(result)
    markdown = source_escalation_policy_requirements_to_markdown(result)
    object_result = build_source_escalation_policy_requirements(
        SimpleNamespace(
            id="object-escalation",
            metadata={"support": "Support handoff must route Tier 2 escalations to on-call."},
        )
    )

    assert source == original
    assert mapping_result.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert extracted == result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert source_escalation_policy_requirements_to_dicts(result) == payload["requirements"]
    assert source_escalation_policy_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_escalation_policy_requirements(result) == result.summary
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "value",
        "suggested_plan_impacts",
    ]
    assert [record.requirement_type for record in result.records] == [
        "severity_level",
        "routing_path",
        "customer_notification",
        "unresolved_policy_gap",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Escalation Policy Requirements Report: escalation-model")
    assert "customer \\| partner incidents" in markdown
    assert object_result.records[0].requirement_type == "routing_path"


def test_implementation_brief_domain_model_inputs_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Escalation severity levels must include Sev1 and Sev2 definitions.",
                "Support handoff routes critical incidents to PagerDuty on-call and the incident commander.",
            ],
            definition_of_done=[
                "Response target acknowledges Sev1 incidents within 15 minutes.",
                "Customer notification uses status page updates before external support briefs.",
                "Open question: who owns regional operations escalation?",
            ],
        )
    )

    result = build_source_escalation_policy_requirements(implementation)

    assert result.brief_id == "implementation-escalation-policy"
    assert result.title == "Escalation policy implementation"
    assert [record.requirement_type for record in result.records] == [
        "severity_level",
        "routing_path",
        "ownership_handoff",
        "response_target",
        "customer_notification",
        "unresolved_policy_gap",
    ]
    assert result.records[0].source_field == "scope[0]"
    assert result.records[1].matched_terms == ("pagerduty", "on-call", "incident commander")


def _source_brief(
    *,
    source_id="source-escalation-policy",
    title="Escalation policy requirements",
    domain="support",
    summary="General escalation policy requirements.",
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
    brief_id="implementation-escalation-policy",
    title="Escalation policy implementation",
    problem_statement="Implement source-backed escalation policy planning support.",
    mvp_goal="Ship escalation policy extraction.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-escalation-policy",
        "title": title,
        "domain": "support",
        "target_user": "support agent",
        "buyer": "operations",
        "workflow_context": "Support and incident escalation",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "operations",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run escalation policy extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
