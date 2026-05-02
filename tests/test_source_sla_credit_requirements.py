import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_sla_credit_requirements import (
    SourceSlaCreditRequirement,
    SourceSlaCreditRequirementsReport,
    build_source_sla_credit_requirements,
    derive_source_sla_credit_requirements,
    extract_source_sla_credit_requirements,
    generate_source_sla_credit_requirements,
    source_sla_credit_requirements_to_dict,
    source_sla_credit_requirements_to_dicts,
    source_sla_credit_requirements_to_markdown,
    summarize_source_sla_credit_requirements,
)


def test_markdown_bullets_extract_sla_credit_dimensions_deterministically():
    result = build_source_sla_credit_requirements(
        _source_brief(
            source_payload={
                "body": """
                ## SLA Credits
                - Uptime credits trigger when monthly availability falls below 99.9%.
                - Service credits equal 10% of monthly fees for each missed SLA tier.
                - Claim window requires customers to submit claims within 30 days.
                - Exclusions include scheduled maintenance and force majeure events.
                """
            }
        )
    )

    assert isinstance(result, SourceSlaCreditRequirementsReport)
    assert result.source_id == "sb-sla-credit"
    assert all(isinstance(record, SourceSlaCreditRequirement) for record in result.records)
    assert [record.credit_trigger for record in result.records] == [
        "monthly availability falls below 99.9%",
        None,
        None,
        None,
    ]
    assert result.records[1].credit_formula == "10% of monthly fees for each missed SLA tier"
    assert result.records[2].claim_window == "customers to submit claims within 30 days"
    assert result.records[3].exclusions == "scheduled maintenance and force majeure events"
    assert result.summary["requirement_count"] == 4
    assert result.summary["dimension_counts"] == {
        "credit_trigger": 1,
        "credit_formula": 1,
        "customer_segment": 0,
        "claim_window": 1,
        "exclusions": 1,
        "approval_evidence": 0,
        "notification_evidence": 0,
    }


def test_structured_payload_fields_and_implementation_brief_are_supported():
    structured = build_source_sla_credit_requirements(
        _source_brief(
            source_payload={
                "sla_credits": {
                    "service_credit_policy": "Enterprise customers receive a 25% service credit if uptime drops below 99.5%.",
                    "claim_window": "Claims must be filed within 15 days after the outage month.",
                    "exclusions": "Excluded downtime includes planned maintenance.",
                    "approval": "Finance approval is required before applying SLA credits.",
                    "notification": "Notify customers by email when credits are approved.",
                }
            }
        )
    )
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Service credit policy must support paid customers when API availability misses the SLA.",
            ],
            definition_of_done=[
                "Support review approval evidence is logged before issuing uptime credits.",
            ],
        )
    )
    object_result = build_source_sla_credit_requirements(
        SimpleNamespace(
            id="object-sla-credit",
            summary="Claim window requires enterprise customers to submit a credit request within 10 days.",
        )
    )

    by_field = {record.source_field: record for record in structured.records}

    assert "source_payload.sla_credits.service_credit_policy" in by_field
    assert by_field["source_payload.sla_credits.service_credit_policy"].customer_segment == "enterprise customers"
    assert by_field["source_payload.sla_credits.claim_window"].claim_window == "filed within 15 days after the outage month"
    assert by_field["source_payload.sla_credits.exclusions"].exclusions == "planned maintenance"
    assert by_field["source_payload.sla_credits.approval"].approval_evidence == "finance approval is required before applying SLA credits"
    assert by_field["source_payload.sla_credits.notification"].notification_evidence == "notify customers by email when credits are approved"
    assert all(record.evidence and record.source_field in record.evidence[0] for record in structured.records)

    model_result = extract_source_sla_credit_requirements(brief)
    assert model_result.source_id == "impl-sla-credit"
    assert [record.source_field for record in model_result.records] == [
        "scope[0]",
        "definition_of_done[0]",
    ]
    assert object_result.records[0].claim_window == "enterprise customers to submit a credit request within 10 days"


def test_duplicate_negated_invalid_and_no_signal_inputs_return_stable_empty_reports():
    duplicate = build_source_sla_credit_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Service credits must equal 10% of monthly fees after an SLA miss.",
                    "Service credits must equal 10% of monthly fees after an SLA miss.",
                ],
                "service_credit_policy": "Service credits must equal 10% of monthly fees after an SLA miss.",
            }
        )
    )
    empty = build_source_sla_credit_requirements(
        _source_brief(
            summary="Legal copy update.",
            source_payload={"body": "No SLA credit changes are in scope and no service credit work is needed."},
        )
    )
    repeat = build_source_sla_credit_requirements(
        _source_brief(
            summary="Legal copy update.",
            source_payload={"body": "No SLA credit changes are in scope and no service credit work is needed."},
        )
    )
    malformed = build_source_sla_credit_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_sla_credit_requirements(42)

    assert len(duplicate.records) == 1
    assert len(duplicate.records[0].evidence) == 1
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-sla-credit"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "dimension_counts": {
            "credit_trigger": 0,
            "credit_formula": 0,
            "customer_segment": 0,
            "claim_window": 0,
            "exclusions": 0,
            "approval_evidence": 0,
            "notification_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
    }
    assert "No SLA credit requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="sla-credit-model",
        source_payload={
            "requirements": [
                "Enterprise service credits must equal 20% of invoice fees | capped monthly.",
                "Claims must be submitted within 30 days.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_sla_credit_requirements(source)
    model_result = generate_source_sla_credit_requirements(model)
    derived = derive_source_sla_credit_requirements(model)
    extracted = extract_source_sla_credit_requirements(model)
    text_result = build_source_sla_credit_requirements("Uptime credits require finance approval before invoice application.")
    payload = source_sla_credit_requirements_to_dict(model_result)
    markdown = source_sla_credit_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_sla_credit_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_sla_credit_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_sla_credit_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_sla_credit_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_text",
        "credit_trigger",
        "credit_formula",
        "customer_segment",
        "claim_window",
        "exclusions",
        "approval_evidence",
        "notification_evidence",
        "source_field",
        "evidence",
        "confidence",
        "planning_notes",
    ]
    assert [record.source_field for record in model_result.records] == [
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Requirement | Trigger | Formula | Segment | Claim Window | Exclusions | Approval | Notification | Source Field | Confidence | Planning Notes | Evidence |" in markdown
    assert "invoice fees \\| capped monthly" in markdown
    assert text_result.records[0].approval_evidence == "finance approval before invoice application"


def _source_brief(
    *,
    source_id="sb-sla-credit",
    title="SLA credit requirements",
    domain="billing",
    summary="General SLA credit requirements.",
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
        "id": "impl-sla-credit",
        "source_brief_id": "source-sla-credit",
        "title": "SLA credit rollout",
        "domain": "billing",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need SLA credit requirements before task generation.",
        "problem_statement": "SLA credit requirements need to be extracted early.",
        "mvp_goal": "Plan SLA credit work from source briefs.",
        "product_surface": "billing",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run SLA credit validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
