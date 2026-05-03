import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_free_trial_conversion_requirements import (
    SourceFreeTrialConversionRequirement,
    SourceFreeTrialConversionRequirementsReport,
    build_source_free_trial_conversion_requirements,
    derive_source_free_trial_conversion_requirements,
    extract_source_free_trial_conversion_requirements,
    generate_source_free_trial_conversion_requirements,
    source_free_trial_conversion_requirements_to_dict,
    source_free_trial_conversion_requirements_to_dicts,
    source_free_trial_conversion_requirements_to_markdown,
    summarize_source_free_trial_conversion_requirements,
)


def test_markdown_brief_extracts_all_free_trial_conversion_categories():
    result = build_source_free_trial_conversion_requirements(
        _source_brief(
            source_payload={
                "body": """
# Free Trial Conversion Requirements

- Trial start eligibility must allow first-time workspace owners to start one free trial.
- Trial length requires a 14-day free trial with timestamps stored in UTC.
- Conversion trigger must auto-convert trial accounts to paid when the trial ends.
- Payment method requirement should collect a credit card before conversion.
- Expiration reminder sends email three days before the trial expires.
- Grace period access keeps read-only access for seven days after expiration.
- Trial entitlements must include full access during trial except premium seats.
- Cancellation before conversion must let admins cancel before billing starts.
""",
                "metadata": {
                    "expiration_reminder": "Trial ending reminder must also notify account owners in app.",
                },
            }
        )
    )

    assert isinstance(result, SourceFreeTrialConversionRequirementsReport)
    assert all(isinstance(record, SourceFreeTrialConversionRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "trial_start_eligibility",
        "trial_length",
        "conversion_trigger",
        "payment_method_requirement",
        "expiration_reminder",
        "grace_period_access",
        "trial_entitlements",
        "cancellation_before_conversion",
    ]
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["trial_start_eligibility"].evidence)
    assert any(
        "source_payload.metadata.expiration_reminder" in item
        for item in by_category["expiration_reminder"].evidence
    )
    assert by_category["trial_entitlements"].suggested_owner == "entitlements_engineering"
    assert "billing state transitions" in by_category["conversion_trigger"].suggested_planning_note
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["grace_period_access"] == 1
    assert result.summary["high_confidence_count"] >= 6


def test_structured_metadata_and_free_text_contribute_without_mutation():
    source = _source_brief(
        source_id="trial-model",
        summary="Free trial signup must check eligibility before starting the trial.",
        source_payload={
            "trial": {
                "length": "Trial duration requires 30 days for qualified self-serve customers.",
                "conversion": "Trial-to-paid conversion must charge when the trial ends.",
            },
            "metadata": {
                "payment_method_requirement": "Card on file is required before trial conversion.",
                "trial_entitlements": "Trial plan should provide limited trial features by plan tier.",
            },
        },
    )
    original = copy.deepcopy(source)

    mapping_result = build_source_free_trial_conversion_requirements(source)
    model_result = generate_source_free_trial_conversion_requirements(SourceBrief.model_validate(source))

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert [record.category for record in model_result.records] == [
        "trial_start_eligibility",
        "trial_length",
        "conversion_trigger",
        "payment_method_requirement",
        "trial_entitlements",
    ]
    by_category = {record.category: record for record in model_result.records}
    assert any("summary" in item for item in by_category["trial_start_eligibility"].evidence)
    assert any("source_payload.trial.length" in item for item in by_category["trial_length"].evidence)
    assert any(
        "source_payload.metadata.payment_method_requirement" in item
        for item in by_category["payment_method_requirement"].evidence
    )


def test_implementation_brief_object_and_plain_text_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Trial start eligibility must restrict free trials to new accounts.",
                "Grace period access should keep read-only access after the trial expires.",
            ],
            risks=[
                "Payment method requirement needs payments review before launch.",
                "Cancellation before conversion can fail if cancel trial state is not retained.",
            ],
            definition_of_done=[
                "Expiration reminder sends an in-app notice before the trial ends.",
                "Trial entitlements record trial seats and feature limits.",
            ],
        )
    )
    object_result = build_source_free_trial_conversion_requirements(
        SimpleNamespace(
            id="object-trial",
            summary="Trial length must be 21 days before paid conversion.",
            metadata={"conversion": "Convert to paid when the trial period ends."},
        )
    )
    text_result = build_source_free_trial_conversion_requirements(
        "Customers should cancel before conversion to avoid the first paid charge."
    )

    model_result = build_source_free_trial_conversion_requirements(brief)

    assert model_result.source_id == "impl-trial"
    assert [record.category for record in model_result.records] == [
        "trial_start_eligibility",
        "payment_method_requirement",
        "expiration_reminder",
        "grace_period_access",
        "trial_entitlements",
        "cancellation_before_conversion",
    ]
    assert model_result.records[0].evidence == (
        "scope[0]: Trial start eligibility must restrict free trials to new accounts.",
    )
    assert [record.category for record in object_result.records] == [
        "trial_length",
        "conversion_trigger",
    ]
    assert text_result.records[0].category == "cancellation_before_conversion"


def test_duplicate_evidence_merges_deterministically_and_limits_categories():
    result = build_source_free_trial_conversion_requirements(
        {
            "id": "dupe-trial",
            "source_payload": {
                "acceptance_criteria": [
                    "Expiration reminder must send a trial ending email.",
                    "Expiration reminder must send a trial ending email.",
                    "Grace period access must extend read-only access after expiration.",
                ],
                "metadata": {
                    "same_reminder": "Expiration reminder must send a trial ending email.",
                    "same_grace": "Grace period access must extend read-only access after expiration.",
                },
            },
        }
    )

    assert [record.category for record in result.records] == [
        "expiration_reminder",
        "grace_period_access",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Expiration reminder must send a trial ending email.",
    )
    assert result.records[1].evidence == (
        "source_payload.acceptance_criteria[2]: Grace period access must extend read-only access after expiration.",
    )
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95
    assert result.summary["categories"] == ["expiration_reminder", "grace_period_access"]


def test_serialization_markdown_summary_helpers_and_iterable_inputs_are_stable():
    source = _source_brief(
        source_id="source-trial-json",
        summary="Trial length must be 14 days before conversion to paid.",
        source_payload={
            "requirements": [
                "Payment method requirement must collect card on file before conversion.",
                "Expiration reminder must escape lifecycle | billing notes.",
            ],
        },
    )
    second = {
        "id": "source-trial-json-2",
        "source_payload": {
            "requirements": [
                "Trial entitlements must include premium feature limits.",
                "Cancellation before conversion must cancel trial access before billing.",
            ]
        },
    }

    model_result = build_source_free_trial_conversion_requirements(SourceBrief.model_validate(source))
    iterable_result = derive_source_free_trial_conversion_requirements([source, second])
    extracted = extract_source_free_trial_conversion_requirements(source)
    payload = source_free_trial_conversion_requirements_to_dict(model_result)
    markdown = source_free_trial_conversion_requirements_to_markdown(model_result)

    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_free_trial_conversion_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_free_trial_conversion_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_free_trial_conversion_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert iterable_result.source_id is None
    assert "trial_entitlements" in iterable_result.summary["categories"]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith(
        "# Source Free Trial Conversion Requirements Report: source-trial-json"
    )
    assert "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |" in markdown
    assert "lifecycle \\| billing notes" in markdown
    assert model_result.records[0].requirement_category == "trial_length"
    assert model_result.records[0].planning_note == model_result.records[0].suggested_planning_note


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_free_trial_conversion_requirements(
        _source_brief(source_id="empty-trial", summary="Update checkout settings copy only.")
    )
    repeat = build_source_free_trial_conversion_requirements(
        _source_brief(source_id="empty-trial", summary="Update checkout settings copy only.")
    )
    negated = build_source_free_trial_conversion_requirements(
        {
            "id": "negated-trial",
            "summary": "No free trial conversion requirements are needed for this checkout copy update.",
            "source_payload": {
                "non_goals": [
                    "Trial reminders and grace period access are out of scope for this release."
                ]
            },
        }
    )
    invalid = build_source_free_trial_conversion_requirements(42)
    malformed = build_source_free_trial_conversion_requirements({"source_payload": {"notes": object()}})

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "trial_start_eligibility": 0,
            "trial_length": 0,
            "conversion_trigger": 0,
            "payment_method_requirement": 0,
            "expiration_reminder": 0,
            "grace_period_access": 0,
            "trial_entitlements": 0,
            "cancellation_before_conversion": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "suggested_owner_counts": {},
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-trial"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No free trial conversion requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert malformed.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-trial",
    title="Free trial conversion requirements",
    domain="billing",
    summary="General trial conversion requirements.",
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


def _implementation_brief(*, scope=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-trial",
        "source_brief_id": "source-trial",
        "title": "Free trial conversion",
        "domain": "billing",
        "target_user": "trial users",
        "buyer": None,
        "workflow_context": "Free trial conversion before task generation.",
        "problem_statement": "Customers need predictable trial conversion behavior.",
        "mvp_goal": "Ship trial conversion lifecycle planning.",
        "product_surface": "billing settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": "Validate free trial conversion scenarios.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
