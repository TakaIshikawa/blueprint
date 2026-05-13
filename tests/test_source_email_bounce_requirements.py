import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_email_bounce_requirements import (
    SourceEmailBounceRequirement,
    SourceEmailBounceRequirementsReport,
    build_source_email_bounce_requirements,
    derive_source_email_bounce_requirements,
    extract_source_email_bounce_requirements,
    generate_source_email_bounce_requirements,
    source_email_bounce_requirements_to_dict,
    source_email_bounce_requirements_to_dicts,
    source_email_bounce_requirements_to_markdown,
    summarize_source_email_bounce_requirements,
)


def test_extracts_email_bounce_categories_in_stable_order():
    result = build_source_email_bounce_requirements(
        _source_brief(
            source_payload={
                "bounce": {
                    "hard": "Hard bounce handling must suppress invalid addresses after permanent delivery failure.",
                    "soft": "Soft bounce handling should retry temporary delivery failures with backoff.",
                    "suppression": "Suppression list writes must block future email after bounces.",
                    "complaint": "Spam complaint feedback loop events must suppress recipients.",
                    "retry": "Retry policy must cap delivery retry attempts before terminal failure.",
                    "webhook": "Provider webhook events from SendGrid must be validated and mapped.",
                    "metrics": "Deliverability metrics must monitor bounce rate and complaint rate alerts.",
                    "notify": "User notification must prompt users to update email address after repeated bounces.",
                    "retention": "Compliance retention must retain bounce evidence for 90 days before purge.",
                }
            }
        )
    )

    assert isinstance(result, SourceEmailBounceRequirementsReport)
    assert all(isinstance(record, SourceEmailBounceRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "hard_bounce",
        "soft_bounce",
        "suppression_list",
        "complaint_feedback",
        "retry_policy",
        "provider_webhook",
        "deliverability_metrics",
        "user_notification",
        "retention",
    ]
    assert result.summary["requirement_count"] == 9
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"
    assert result.records[0].suggested_owners == ("email_platform", "backend")


def test_models_objects_strings_and_missing_summary_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Hard bounce handling is required for invalid email addresses.",
                "Provider webhook events must feed deliverability metrics.",
            ]
        )
    )
    source = SourceBrief.model_validate(_source_brief(summary="Soft bounce retry policy must retry temporary failures."))
    object_result = build_source_email_bounce_requirements(
        SimpleNamespace(id="object-bounce", summary="Complaint feedback loop handling must update the suppression list.")
    )
    text_result = build_source_email_bounce_requirements("Email bounce provider webhook must record deliverability metrics.")

    implementation_result = generate_source_email_bounce_requirements(implementation)
    source_result = derive_source_email_bounce_requirements(source)

    assert {"hard_bounce", "provider_webhook", "deliverability_metrics"} <= {record.category for record in implementation_result.records}
    assert source_result.summary["missing_detail_flags"] == ["missing_suppression", "missing_webhook", "missing_metrics"]
    assert [record.category for record in object_result.records] == ["suppression_list", "complaint_feedback"]
    assert [record.category for record in text_result.records] == ["provider_webhook", "deliverability_metrics"]


def test_serialization_aliases_and_markdown_are_deterministic():
    source = _source_brief(summary="Hard bounce handling must retain evidence and alert deliverability metrics.")
    model = SourceBrief.model_validate(source)

    result = build_source_email_bounce_requirements(source)
    extracted = extract_source_email_bounce_requirements(model)
    payload = source_email_bounce_requirements_to_dict(result)
    markdown = source_email_bounce_requirements_to_markdown(result)

    assert extracted == result.records
    assert summarize_source_email_bounce_requirements(result) == result.summary
    assert source_email_bounce_requirements_to_dicts(result) == payload["requirements"]
    assert source_email_bounce_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert markdown.startswith("# Source Email Bounce Requirements Report: source-bounce")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |" in markdown


def test_negated_and_unrelated_bounce_mentions_return_empty_reports():
    negated = build_source_email_bounce_requirements(
        _source_brief(summary="Email bounce handling is out of scope and no suppression changes are required.")
    )
    unrelated = build_source_email_bounce_requirements(
        _source_brief(summary="Marketing page bounce animation timing must be updated.")
    )
    blank = build_source_email_bounce_requirements("")
    invalid = build_source_email_bounce_requirements(42)

    assert negated.records == ()
    assert unrelated.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary["requirement_count"] == 0
    assert unrelated.summary["status"] == "no_email_bounce_language"
    assert "No source email bounce requirements were inferred" in unrelated.to_markdown()


def _source_brief(*, source_id="source-bounce", summary="General email requirements.", source_payload=None):
    return {
        "id": source_id,
        "title": "Email bounce requirements",
        "domain": "email",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None):
    return {
        "id": "implementation-bounce",
        "source_brief_id": "source-bounce",
        "title": "Email bounce implementation",
        "domain": "email",
        "target_user": "operator",
        "buyer": "platform",
        "workflow_context": "Operators need email delivery recovery.",
        "problem_statement": "Failed email delivery needs deterministic handling.",
        "mvp_goal": "Plan bounce handling.",
        "product_surface": "email",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run Bounce extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
