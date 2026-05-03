import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_breach_notification_requirements import (
    SourceBreachNotificationRequirement,
    SourceBreachNotificationRequirementsReport,
    build_source_breach_notification_requirements,
    derive_source_breach_notification_requirements,
    extract_source_breach_notification_requirements,
    generate_source_breach_notification_requirements,
    source_breach_notification_requirements_to_dict,
    source_breach_notification_requirements_to_dicts,
    source_breach_notification_requirements_to_markdown,
    summarize_source_breach_notification_requirements,
)


def test_structured_source_payload_extracts_all_breach_notification_categories():
    result = build_source_breach_notification_requirements(
        _source_brief(
            source_payload={
                "breach_notification": {
                    "detection": "Detection threshold must treat unauthorized access to personal data as a reportable breach.",
                    "deadline": "Notification deadline must notify affected users within 72 hours after confirmation.",
                    "users": "Affected user notice must be sent to impacted users and data subjects by email.",
                    "regulator": "Regulator notice must be filed with the supervisory authority within 72 hours.",
                    "customer": "Customer contract notice must notify enterprise customers and security contacts within 24 hours.",
                    "evidence": "Evidence preservation must retain forensic evidence, audit logs, and chain of custody.",
                    "approval": "Communications approval requires legal approval before external customer statements.",
                    "postmortem": "Postmortem requirement must complete root cause analysis and corrective actions.",
                }
            }
        )
    )

    assert isinstance(result, SourceBreachNotificationRequirementsReport)
    assert result.source_id == "source-breach-notification"
    assert all(isinstance(record, SourceBreachNotificationRequirement) for record in result.records)
    assert {
        "detection_threshold",
        "notification_deadline",
        "affected_user_notice",
        "regulator_notice",
        "customer_contract_notice",
        "evidence_preservation",
        "communications_approval",
        "postmortem_requirement",
    } == {record.category for record in result.records}
    by_category = {record.category: record for record in result.records}
    deadline = next(
        record
        for record in result.records
        if record.category == "notification_deadline" and record.source_field == "source_payload.breach_notification.deadline"
    )
    assert deadline.notification_deadline == "within 72 hours"
    assert deadline.affected_party == "affected users"
    assert by_category["regulator_notice"].affected_party == "supervisory authority"
    assert by_category["customer_contract_notice"].notification_deadline == "within 24 hours"
    assert by_category["customer_contract_notice"].affected_party == "enterprise customers"
    assert by_category["evidence_preservation"].owner_suggestion == "security"
    assert by_category["communications_approval"].planning_note.startswith("Define legal")
    assert deadline.source_field == "source_payload.breach_notification.deadline"
    assert any("within 72 hours" in item for item in deadline.evidence)
    assert result.summary["requirement_count"] >= 8
    assert result.summary["category_counts"]["regulator_notice"] == 1
    assert result.summary["requires_deadline_tracking"] is True
    assert result.summary["requires_affected_user_notice"] is True
    assert result.summary["requires_regulator_notice"] is True
    assert result.summary["requires_customer_contract_notice"] is True
    assert result.summary["requires_evidence_preservation"] is True
    assert result.summary["requires_communications_approval"] is True
    assert result.summary["requires_postmortem"] is True
    assert result.summary["status"] == "ready_for_breach_notification_planning"


def test_prose_implementation_and_object_inputs_extract_breach_notification_work():
    text_result = build_source_breach_notification_requirements(
        """
# Breach notification

- Security incidents must trigger breach notification when personal data is exposed.
- Notify regulators and affected users without undue delay after a reportable breach.
- Preserve forensic evidence and audit logs for chain of custody.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Customer contract notice must notify customer admins within 48 hours.",
                "Communications approval requires legal approval for any external statement.",
            ],
            definition_of_done=[
                "Post-incident review produces an RCA and corrective actions.",
                "Regulatory filing evidence is retained for the data protection authority.",
            ],
        )
    )
    object_result = build_source_breach_notification_requirements(
        SimpleNamespace(
            id="object-breach",
            metadata={
                "breach_notification": "Notification deadline: notify affected users within 72 hours | legal note."
            },
        )
    )

    assert {
        "detection_threshold",
        "notification_deadline",
        "affected_user_notice",
        "regulator_notice",
        "evidence_preservation",
    } <= {record.category for record in text_result.records}
    implementation_result = generate_source_breach_notification_requirements(implementation)
    assert implementation_result.source_id == "implementation-breach-notification"
    assert {
        "customer_contract_notice",
        "communications_approval",
        "regulator_notice",
        "postmortem_requirement",
    } <= {record.category for record in implementation_result.records}
    assert [record.category for record in object_result.records] == [
        "notification_deadline",
        "affected_user_notice",
    ]


def test_no_signal_no_impact_malformed_and_invalid_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-breach"
        summary = "No breach notification or security incident response work is required for this release."

    unrelated = build_source_breach_notification_requirements(
        _source_brief(summary="Admin copy update has no customer impact and no personal data was accessed.")
    )
    negated = build_source_breach_notification_requirements(BriefLike())
    out_of_scope = build_source_breach_notification_requirements(
        _source_brief(non_goals=["Breach notification requirements are out of scope for this release."])
    )
    malformed = build_source_breach_notification_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_breach_notification_requirements(42)
    blank = build_source_breach_notification_requirements("")

    assert unrelated.records == ()
    assert unrelated.findings == ()
    assert unrelated.to_dicts() == []
    assert unrelated.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "detection_threshold": 0,
            "notification_deadline": 0,
            "affected_user_notice": 0,
            "regulator_notice": 0,
            "customer_contract_notice": 0,
            "evidence_preservation": 0,
            "communications_approval": 0,
            "postmortem_requirement": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "affected_parties": [],
        "notification_deadlines": [],
        "requires_deadline_tracking": False,
        "requires_affected_user_notice": False,
        "requires_regulator_notice": False,
        "requires_customer_contract_notice": False,
        "requires_evidence_preservation": False,
        "requires_communications_approval": False,
        "requires_postmortem": False,
        "status": "no_breach_notification_language",
    }
    assert "No breach notification requirements were found" in unrelated.to_markdown()
    assert negated.records == ()
    assert out_of_scope.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert blank.records == ()


def test_aliases_json_serialization_ordering_markdown_escaping_and_no_mutation():
    source = _source_brief(
        source_id="breach-model",
        summary="Breach notification must notify affected users within 72 hours.",
        source_payload={
            "requirements": [
                "Notification deadline must notify affected users within 72 hours after confirmation.",
                "Notification deadline must notify affected users within 72 hours after confirmation.",
                "Customer contract notice must notify enterprise customers within 24 hours | legal note.",
                "Evidence preservation must retain audit logs and forensic evidence.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_breach_notification_requirements(source)
    model_result = extract_source_breach_notification_requirements(model)
    generated = generate_source_breach_notification_requirements(model)
    derived = derive_source_breach_notification_requirements(model)
    payload = source_breach_notification_requirements_to_dict(model_result)
    markdown = source_breach_notification_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_breach_notification_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_breach_notification_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_breach_notification_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "notification_deadline",
        "affected_party",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "owner_suggestion",
        "planning_note",
    ]
    assert {
        "notification_deadline",
        "affected_user_notice",
        "customer_contract_notice",
        "evidence_preservation",
    } <= {record.category for record in model_result.records}
    deadline = next(
        record
        for record in model_result.records
        if record.category == "notification_deadline" and record.source_field == "summary"
    )
    assert deadline.evidence == (
        "summary: Breach notification must notify affected users within 72 hours.",
    )
    assert deadline.requirement_category == "notification_deadline"
    assert deadline.planning_notes == (deadline.planning_note,)
    assert deadline.owner_suggestions == (deadline.owner_suggestion,)
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Requirement | Deadline | Affected Party |" in markdown
    assert "24 hours \\| legal note" in markdown


def test_deterministic_sorting_across_multiple_sources():
    result = build_source_breach_notification_requirements(
        [
            _source_brief(
                source_id="z-source",
                source_payload={
                    "incident_response": "Regulator notice must notify the supervisory authority within 72 hours."
                },
            ),
            _source_brief(
                source_id="a-source",
                source_payload={
                    "incident_response": [
                        "Postmortem requirement must complete root cause analysis.",
                        "Detection threshold must define reportable incidents.",
                    ]
                },
            ),
        ]
    )

    keys = [
        (record.source_brief_id, record.category, record.notification_deadline or "")
        for record in result.records
    ]
    assert keys == [
        ("a-source", "detection_threshold", ""),
        ("a-source", "postmortem_requirement", ""),
        ("z-source", "notification_deadline", "within 72 hours"),
        ("z-source", "regulator_notice", "within 72 hours"),
    ]
    assert result.source_id is None


def _source_brief(
    *,
    source_id="source-breach-notification",
    title="Breach notification requirements",
    domain="security",
    summary="General breach notification requirements.",
    source_payload=None,
    non_goals=None,
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
    } | ({"non_goals": non_goals} if non_goals is not None else {})


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-breach-notification",
        "source_brief_id": "source-breach-notification",
        "title": "Breach notification rollout",
        "domain": "security",
        "target_user": "security and legal teams",
        "buyer": None,
        "workflow_context": "Security incident response needs explicit breach notification requirements.",
        "problem_statement": "Breach planning needs source-backed tasks.",
        "mvp_goal": "Plan breach notification behavior from source briefs.",
        "product_surface": "security operations",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for breach notification coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
