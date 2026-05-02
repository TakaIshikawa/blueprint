import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_email_template_requirements import (
    SourceEmailTemplateRequirement,
    SourceEmailTemplateRequirementsReport,
    build_source_email_template_requirements,
    derive_source_email_template_requirements,
    extract_source_email_template_requirements,
    generate_source_email_template_requirements,
    source_email_template_requirements_to_dict,
    source_email_template_requirements_to_dicts,
    source_email_template_requirements_to_markdown,
    summarize_source_email_template_requirements,
)


def test_structured_source_payload_extracts_template_content_requirements():
    result = build_source_email_template_requirements(
        _source_brief(
            source_payload={
                "email_template": {
                    "subject": "Subject line must be \"Your receipt is ready\".",
                    "variables": "Template variables must include {{first_name}}, {{order_id}}, and {{receipt_url}}.",
                    "localization": "Localized copy is required for en-US, fr-FR, and ja-JP.",
                    "footer": "Legal footer must include unsubscribe, physical mailing address, privacy link, and terms link.",
                    "plain_text": "Plain-text fallback is required as a multipart alternative.",
                    "sender": "Sender identity must use from name Billing Team and billing@example.com.",
                    "approval": "Approval workflow requires legal approval and brand sign-off before launch.",
                    "preview": "Preview text: View your latest receipt.",
                    "dark_mode": "Dark mode rendering must preserve logo and link contrast.",
                    "reply_to": "Reply-To must route to support@example.com.",
                    "owner": "Template owner: Lifecycle Messaging.",
                }
            },
        )
    )

    by_concern = {record.concern: record for record in result.records}

    assert isinstance(result, SourceEmailTemplateRequirementsReport)
    assert all(isinstance(record, SourceEmailTemplateRequirement) for record in result.records)
    assert [record.concern for record in result.records] == [
        "subject",
        "variables",
        "localization",
        "legal_footer",
        "plain_text_fallback",
        "sender_identity",
        "approval_workflow",
        "preview_text",
        "dark_mode",
        "reply_to",
        "template_ownership",
    ]
    assert by_concern["subject"].value == "Your receipt is ready"
    assert by_concern["variables"].value == "first_name, order_id, receipt_url"
    assert by_concern["variables"].missing_details == ()
    assert by_concern["localization"].value == "en-US, fr-FR, ja-JP"
    assert by_concern["localization"].missing_details == ()
    assert by_concern["sender_identity"].value == "billing@example.com"
    assert by_concern["reply_to"].value == "support@example.com"
    assert by_concern["approval_workflow"].value == "legal approval"
    assert by_concern["template_ownership"].value == "Lifecycle Messaging"
    assert by_concern["legal_footer"].source_field == "source_payload.email_template.footer"
    assert by_concern["dark_mode"].confidence == "high"
    assert by_concern["variables"].suggested_plan_impacts[0].startswith("Model required template variables")
    assert result.summary["requirement_count"] == 11
    assert result.summary["concern_counts"]["plain_text_fallback"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_markdown_and_implementation_brief_support_content_requirements():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Email template subject should be defined for account recovery notifications.",
                "Template variables must include {{reset_url}} and {{expires_at}}.",
            ],
            definition_of_done=[
                "Preview text and plain text version are approved by product.",
                "Reply-To handling routes support replies to help@example.com.",
            ],
        )
    )
    text_result = build_source_email_template_requirements(
        """
# Password reset email template

- Subject line: Reset your password.
- Localization requires English, French, and Spanish.
- Legal footer must include privacy link and company address.
"""
    )
    implementation_result = generate_source_email_template_requirements(implementation)

    assert [record.concern for record in text_result.records] == [
        "subject",
        "localization",
        "legal_footer",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "subject",
        "variables",
        "plain_text_fallback",
        "preview_text",
        "reply_to",
    } <= {record.concern for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-email-template"
    assert implementation_result.title == "Email template implementation"


def test_missing_details_are_removed_when_evidence_supplies_values_and_input_is_not_mutated():
    source = _source_brief(
        source_id="email-template-details",
        source_payload={
            "templates": {
                "requirements": [
                    "Email template localization is required.",
                    "Email template variables are required.",
                    "Locales must include en-US and de-DE.",
                    "Template variables must include {{user_name}} and {{account_url}}.",
                ]
            }
        },
    )
    original = copy.deepcopy(source)

    result = build_source_email_template_requirements(source)
    by_concern = {record.concern: record for record in result.records}

    assert source == original
    assert by_concern["localization"].missing_details == ()
    assert by_concern["localization"].value == "en-US, de-DE"
    assert by_concern["variables"].missing_details == ()
    assert by_concern["variables"].value == "user_name, account_url"
    assert by_concern["localization"].evidence == (
        "source_payload.templates.requirements[0]: Email template localization is required.",
        "source_payload.templates.requirements[2]: Locales must include en-US and de-DE.",
    )


def test_deliverability_only_language_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-template"
        summary = "No email templates or notification template copy are required for this release."

    object_result = build_source_email_template_requirements(
        SimpleNamespace(
            id="object-template",
            summary="Email template copy must include subject line and legal footer content.",
            metadata={"reply_to": "Reply-To must use support@example.com."},
        )
    )
    negated = build_source_email_template_requirements(BriefLike())
    deliverability = build_source_email_template_requirements(
        _source_brief(
            title="Email deliverability readiness",
            summary="Configure SPF, DKIM, DMARC, bounce handling, suppression lists, and IP warmup.",
            source_payload={"requirements": ["Monitor complaint rate and inbox placement."]},
        )
    )
    malformed = build_source_email_template_requirements({"source_payload": {"notes": object()}})
    blank = build_source_email_template_requirements("")

    expected_summary = {
        "requirement_count": 0,
        "concerns": [],
        "concern_counts": {
            "subject": 0,
            "variables": 0,
            "localization": 0,
            "legal_footer": 0,
            "plain_text_fallback": 0,
            "sender_identity": 0,
            "approval_workflow": 0,
            "preview_text": 0,
            "dark_mode": 0,
            "reply_to": 0,
            "template_ownership": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_email_template_language",
    }
    assert [record.concern for record in object_result.records] == [
        "subject",
        "legal_footer",
        "reply_to",
    ]
    assert negated.records == ()
    assert deliverability.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert deliverability.summary == expected_summary
    assert deliverability.to_dicts() == []
    assert "No source email template requirements were inferred." in deliverability.to_markdown()
    assert summarize_source_email_template_requirements(deliverability) == expected_summary


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="email-template-model",
        title="Receipt email source",
        summary="Receipt email template requirements include subject, approval, and preview text.",
        source_payload={
            "requirements": [
                "Preview text: Receipt and next steps | saved.",
                "Subject line: Your receipt.",
                "Approval workflow requires brand approval.",
            ]
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_email_template_requirements(model)
    extracted = extract_source_email_template_requirements(model)
    derived = derive_source_email_template_requirements(model)
    payload = source_email_template_requirements_to_dict(result)
    markdown = source_email_template_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_email_template_requirements(result) == result.summary
    assert source_email_template_requirements_to_dicts(result) == payload["requirements"]
    assert source_email_template_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "concern",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "missing_details",
        "suggested_plan_impacts",
    ]
    assert [record["concern"] for record in payload["requirements"]] == [
        "subject",
        "approval_workflow",
        "preview_text",
    ]
    assert markdown.startswith("# Source Email Template Requirements Report: email-template-model")
    assert "| Concern | Value | Missing Details | Confidence | Source Field | Evidence | Suggested Plan Impacts |" in markdown
    assert "Receipt and next steps \\| saved" in markdown


def _source_brief(
    *,
    source_id="source-email-template",
    title="Transactional email template requirements",
    domain="notifications",
    summary="General email template requirements.",
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
    brief_id="implementation-email-template",
    title="Email template implementation",
    problem_statement="Implement source-backed email template requirements.",
    mvp_goal="Ship transactional email template planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-email-template",
        "title": title,
        "domain": "notifications",
        "target_user": "customer",
        "buyer": "product",
        "workflow_context": "Transactional notifications",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "notifications",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run email template extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
