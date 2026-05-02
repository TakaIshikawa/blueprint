import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import SourceBrief
from blueprint.source_age_verification_requirements import (
    SourceAgeVerificationRequirement,
    SourceAgeVerificationRequirementsReport,
    build_source_age_verification_requirements,
    extract_source_age_verification_requirements,
    generate_source_age_verification_requirements,
    source_age_verification_requirements_to_dict,
    source_age_verification_requirements_to_dicts,
    source_age_verification_requirements_to_markdown,
    summarize_source_age_verification_requirements,
)


def test_detects_age_requirements_across_source_brief_fields_with_paths():
    result = build_source_age_verification_requirements(
        _source_brief(
            title="Age verification and minor account requirements",
            summary="Registration must verify age and collect date of birth before account access.",
            source_payload={
                "requirements": [
                    "Users under 13 require verifiable parental consent before signup.",
                    "Minor accounts must disable public messaging until approved.",
                ],
                "acceptance_criteria": [
                    "COPPA review is complete for children under 13 before launch.",
                    "Age-gated access blocks restricted content for underage users.",
                ],
                "constraints": "Guardian consent records must be auditable.",
                "data_requirements": "Store DOB, consent status, and consent timestamp.",
            },
        )
    )

    assert isinstance(result, SourceAgeVerificationRequirementsReport)
    assert result.source_brief_id == "sb-age"
    assert all(isinstance(record, SourceAgeVerificationRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "age_verification",
        "parental_consent",
        "minor_account",
        "coppa",
        "age_gated_access",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["parental_consent"].severity == "blocker"
    assert by_type["coppa"].severity == "blocker"
    assert by_type["age_verification"].readiness == "needs_clarification"
    assert by_type["minor_account"].readiness == "needs_clarification"
    assert any(
        "source_payload.requirements[0]" in item for item in by_type["parental_consent"].evidence
    )
    assert "source_payload.acceptance_criteria[1]" in by_type["age_gated_access"].source_field_paths
    assert any(
        "capabilities differ" in item for item in by_type["minor_account"].follow_up_questions
    )
    assert result.summary["requirement_count"] == 5
    assert result.summary["type_counts"]["coppa"] == 1
    assert result.summary["severity_counts"]["blocker"] == 2
    assert result.summary["requirement_types"] == [
        record.requirement_type for record in result.records
    ]


def test_follow_up_questions_cover_unclear_threshold_consent_owner_and_surfaces():
    result = build_source_age_verification_requirements(
        {
            "id": "unclear-age",
            "requirements": [
                "Age gated access is required.",
                "Parental consent must be captured.",
                "Minor accounts are supported.",
            ],
        }
    )
    by_type = {record.requirement_type: record for record in result.records}

    assert any(
        "age threshold" in question for question in by_type["age_gated_access"].follow_up_questions
    )
    assert any(
        "product surfaces" in question
        for question in by_type["age_gated_access"].follow_up_questions
    )
    assert any(
        "authorized to grant" in question
        for question in by_type["parental_consent"].follow_up_questions
    )
    assert any(
        "workflow surfaces" in question
        for question in by_type["parental_consent"].follow_up_questions
    )
    assert by_type["minor_account"].readiness == "needs_clarification"
    assert result.summary["follow_up_question_count"] >= 5


def test_duplicate_signals_merge_deterministically():
    result = build_source_age_verification_requirements(
        {
            "id": "dupe-age",
            "summary": "Age verification must collect DOB at signup.",
            "source_payload": {
                "requirements": [
                    "Age verification must collect DOB at signup.",
                    "age verification must collect DOB at signup.",
                    "COPPA compliance is required for children under 13.",
                ],
                "compliance": {"coppa": "COPPA compliance is required for children under 13."},
            },
        }
    )
    by_type = {record.requirement_type: record for record in result.records}

    assert by_type["age_verification"].evidence == (
        "summary: Age verification must collect DOB at signup.",
    )
    assert by_type["age_verification"].source_field_paths == (
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
        "summary",
    )
    assert by_type["coppa"].evidence == (
        "source_payload.compliance.coppa: COPPA compliance is required for children under 13.",
    )
    assert by_type["coppa"].matched_terms == ("coppa",)


def test_sourcebrief_mapping_model_object_serialization_and_markdown_are_stable():
    source = _source_brief(
        source_id="age-model",
        summary="Signup must verify age for users 13+.",
        source_payload={
            "requirements": ["Minor accounts must require guardian approval on registration."],
            "acceptance_criteria": ["Age-gated access blocks restricted content in the app."],
            "compliance": "COPPA review must be complete.",
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_age_verification_requirements(source)
    model_result = generate_source_age_verification_requirements(model)
    extracted = extract_source_age_verification_requirements(model)
    object_result = build_source_age_verification_requirements(
        SimpleNamespace(
            id="object-age",
            summary="Age verification must check DOB during onboarding.",
            data_requirements="Parental consent records are required.",
        )
    )
    payload = source_age_verification_requirements_to_dict(model_result)
    markdown = source_age_verification_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert [record.requirement_type for record in object_result.records] == [
        "age_verification",
        "parental_consent",
    ]
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_age_verification_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_age_verification_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_age_verification_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "evidence",
        "source_field_paths",
        "follow_up_questions",
        "severity",
        "readiness",
        "matched_terms",
    ]
    assert markdown.startswith("# Source Age Verification Requirements Report: age-model")
    assert (
        "| Type | Severity | Readiness | Source Field Paths | Evidence | Follow-up Questions |"
        in markdown
    )


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_age_verification_requirements(
        _source_brief(
            title="Profile copy",
            summary="Improve profile copy.",
            source_payload={"requirements": "No age verification changes are required."},
        )
    )
    repeat = build_source_age_verification_requirements(
        _source_brief(
            title="Profile copy",
            summary="Improve profile copy.",
            source_payload={"requirements": "No age verification changes are required."},
        )
    )
    invalid = build_source_age_verification_requirements("not a source brief")

    expected_summary = {
        "requirement_count": 0,
        "type_counts": {
            "age_verification": 0,
            "parental_consent": 0,
            "minor_account": 0,
            "coppa": 0,
            "age_gated_access": 0,
        },
        "severity_counts": {"blocker": 0, "high": 0, "medium": 0},
        "readiness_counts": {"ready_for_planning": 0, "needs_clarification": 0},
        "requirement_types": [],
        "follow_up_question_count": 0,
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_brief_id == "sb-age"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No age verification or minor access requirements were found" in empty.to_markdown()
    assert invalid.source_brief_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="sb-age",
    title="Age verification requirements",
    domain="compliance",
    summary="General age verification requirements.",
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
