import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_export_control_requirements import (
    SourceExportControlRequirement,
    SourceExportControlRequirementsReport,
    build_source_export_control_requirements,
    derive_source_export_control_requirements,
    extract_source_export_control_requirements,
    generate_source_export_control_requirements,
    source_export_control_requirements_to_dict,
    source_export_control_requirements_to_dicts,
    source_export_control_requirements_to_markdown,
    summarize_source_export_control_requirements,
)


def test_nested_source_payload_extracts_export_control_categories_and_details():
    result = build_source_export_control_requirements(
        _source_brief(
            source_payload={
                "compliance": {
                    "export_control": {
                        "requirements": [
                            "OFAC sanctions screening must use ComplyAdvantage, block matches, jurisdiction: US, review_owner: compliance, renewal_cadence: daily rescreen.",
                            "Restricted countries must block onboarding from Iran, Cuba, Syria, and North Korea; jurisdiction: US; review_owner: legal.",
                            "Export classification must capture EAR ECCN and license exception; jurisdiction: BIS EAR; review_owner: export control counsel; renewal_cadence: annually.",
                            "Denied-party checks require Dow Jones screening provider and hold watchlist matches for manual review; review_owner: compliance ops; renewal_cadence: monthly.",
                            "Restricted technology transfer should prevent access to controlled technical data by foreign nationals; jurisdiction: ITAR; denied_party_action: prevent access; review_owner: security.",
                            "Country of residence gating must block signup by billing country and IP geolocation; jurisdiction: US; denied_party_action: block signup; review_owner: risk.",
                            "Audit evidence must retain screening logs and provider response records; review_owner: compliance; renewal_cadence: quarterly.",
                        ]
                    }
                }
            }
        )
    )

    assert isinstance(result, SourceExportControlRequirementsReport)
    assert result.source_id == "sb-export-control"
    assert all(isinstance(record, SourceExportControlRequirement) for record in result.records)
    assert [record.requirement_category for record in result.records] == [
        "sanctions_screening",
        "restricted_country",
        "export_classification",
        "denied_party_check",
        "technology_transfer",
        "residence_gating",
        "audit_evidence",
    ]
    by_category = {record.requirement_category: record for record in result.records}
    assert by_category["sanctions_screening"].screening_provider == "complyadvantage"
    assert by_category["sanctions_screening"].denied_party_action == "block"
    assert by_category["sanctions_screening"].readiness == "ready"
    assert by_category["restricted_country"].source_field == "source_payload.compliance.export_control.requirements[1]"
    assert by_category["export_classification"].jurisdiction == "bis ear"
    assert by_category["denied_party_check"].screening_provider == "dow jones screening provider"
    assert by_category["technology_transfer"].denied_party_action == "prevent access"
    assert by_category["residence_gating"].denied_party_action == "block signup"
    assert by_category["audit_evidence"].renewal_cadence == "quarterly"
    assert all(record.evidence and record.source_field in record.evidence[0] for record in result.records)
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"] == {category: 1 for category in result.summary["categories"]}


def test_plain_text_and_implementation_brief_inputs_are_supported():
    text_result = build_source_export_control_requirements(
        """
        Export controls:
        - Sanctions screening must screen OFAC SDN list matches before onboarding.
        - Country of residence gating should deny customers in embargoed countries.
        - Audit evidence must retain export control records.
        """
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "EAR export classification must store ECCN for encryption export features.",
                "Denied-party checks must freeze payouts when restricted-party screening matches.",
            ],
            definition_of_done=[
                "Technology transfer review prevents source code export for controlled technology.",
            ],
        )
    )
    object_result = build_source_export_control_requirements(
        SimpleNamespace(
            id="object-export",
            summary="OFAC sanctions screening must block checkout when the user appears on the SDN list.",
        )
    )

    assert [record.requirement_category for record in text_result.records] == [
        "sanctions_screening",
        "restricted_country",
        "residence_gating",
        "audit_evidence",
    ]
    model_result = generate_source_export_control_requirements(implementation)
    assert model_result.source_id == "impl-export-control"
    assert [record.requirement_category for record in model_result.records] == [
        "export_classification",
        "denied_party_check",
        "technology_transfer",
    ]
    assert model_result.records[0].source_field == "scope[0]"
    assert object_result.records[0].requirement_category == "sanctions_screening"
    assert object_result.records[0].confidence == "medium"


def test_dedupe_sorting_missing_details_and_markdown_escaping_are_stable():
    result = build_source_export_control_requirements(
        _source_brief(
            source_id="export-dedupe",
            source_payload={
                "requirements": [
                    "Denied-party checks must hold matches for manual review | compliance note.",
                    "Denied-party checks must hold matches for manual review | compliance note.",
                    "Export classification notes for ECCN.",
                    "OFAC sanctions screening.",
                ],
                "export_classification": "Export classification notes for ECCN.",
            },
        )
    )

    assert [record.requirement_category for record in result.records] == [
        "sanctions_screening",
        "export_classification",
        "denied_party_check",
    ]
    sanctions = result.records[0]
    classification = result.records[1]
    denied = result.records[2]
    assert sanctions.confidence == "low"
    assert sanctions.missing_details == (
        "jurisdiction",
        "screening_provider",
        "denied_party_action",
        "review_owner",
        "renewal_cadence",
    )
    assert sanctions.readiness == "needs_detail"
    assert classification.confidence == "medium"
    assert denied.evidence == (
        "source_payload.requirements[0]: Denied-party checks must hold matches for manual review | compliance note.",
    )
    assert denied.denied_party_action == "hold"
    assert "screening_provider" in denied.missing_details
    markdown = result.to_markdown()
    assert "| Source Brief | Category | Requirement | Jurisdiction | Provider | Action | Owner | Renewal | Source Field | Missing Details | Confidence | Readiness | Planning Note | Evidence |" in markdown
    assert "manual review \\| compliance note" in markdown


def test_duplicate_negated_malformed_and_no_signal_inputs_return_stable_empty_reports():
    empty = build_source_export_control_requirements(
        _source_brief(
            summary="Billing copy update.",
            source_payload={"body": "No export controls, sanctions, OFAC, EAR, ITAR, or denied party work is required."},
        )
    )
    repeat = build_source_export_control_requirements(
        _source_brief(
            summary="Billing copy update.",
            source_payload={"body": "No export controls, sanctions, OFAC, EAR, ITAR, or denied party work is required."},
        )
    )
    malformed = build_source_export_control_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_export_control_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-export-control"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "sanctions_screening": 0,
            "restricted_country": 0,
            "export_classification": 0,
            "denied_party_check": 0,
            "technology_transfer": 0,
            "residence_gating": 0,
            "audit_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "readiness_counts": {"ready": 0, "needs_detail": 0},
        "categories": [],
    }
    assert "No export-control requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_serialization_aliases_json_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="export-model",
        source_payload={
            "requirements": [
                "Sanctions screening must use Refinitiv and block OFAC matches | escalation.",
                "Audit evidence must retain screening result records quarterly.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_export_control_requirements(source)
    model_result = extract_source_export_control_requirements(model)
    generated = generate_source_export_control_requirements(model)
    derived = derive_source_export_control_requirements(model)
    payload = source_export_control_requirements_to_dict(model_result)
    markdown = source_export_control_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_export_control_requirements_to_dict(mapping_result)
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_export_control_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_export_control_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_export_control_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_category",
        "requirement_text",
        "jurisdiction",
        "screening_provider",
        "denied_party_action",
        "review_owner",
        "renewal_cadence",
        "source_field",
        "evidence",
        "missing_details",
        "confidence",
        "readiness",
        "planning_note",
    ]
    assert model_result.records[0].category == "sanctions_screening"
    assert model_result.records[0].export_control_category == "sanctions_screening"
    assert model_result.records[0].planning_notes == (model_result.records[0].planning_note,)
    assert markdown == model_result.to_markdown()
    assert "OFAC matches \\| escalation" in markdown


def _source_brief(
    *,
    source_id="sb-export-control",
    title="Export control requirements",
    domain="compliance",
    summary="General export control requirements.",
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
        "id": "impl-export-control",
        "source_brief_id": "source-export-control",
        "title": "Export control rollout",
        "domain": "compliance",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need export control requirements before task generation.",
        "problem_statement": "Export control requirements need to be extracted early.",
        "mvp_goal": "Plan export control work from source briefs.",
        "product_surface": "compliance",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for export control coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
