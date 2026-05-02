import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_business_rules import (
    SourceBusinessRule,
    SourceBusinessRulesReport,
    build_source_business_rules,
    derive_source_business_rules,
    extract_source_business_rules,
    generate_source_business_rules,
    source_business_rules_to_dict,
    source_business_rules_to_dicts,
    source_business_rules_to_markdown,
    summarize_source_business_rules,
)


def test_markdown_text_extracts_rule_language_and_escapes_pipes():
    result = build_source_business_rules(
        """
        # Partner onboarding

        ## Business Rules
        - Enterprise customers must receive finance approval before invoice billing is enabled.
        - Trial workspaces cannot export reports after expiration unless an admin grants an exception.
        - Eligibility | partners are eligible only if compliance status is approved.

        ## Notes
        Update the empty-state copy.
        """
    )

    by_category = {rule.category: rule for rule in result.business_rules}
    markdown = source_business_rules_to_markdown(result)

    assert isinstance(result, SourceBusinessRulesReport)
    assert all(isinstance(rule, SourceBusinessRule) for rule in result.records)
    assert {"approval", "lifecycle", "eligibility"} <= set(by_category)
    assert by_category["approval"].confidence == "high"
    assert by_category["approval"].source_field.startswith("body.business_rules")
    assert "finance approval before invoice billing" in by_category["approval"].rule_text
    assert "Eligibility \\| partners" in markdown
    assert markdown.startswith("# Source Business Rules Report")
    assert "| Category | Rule | Confidence | Source Field | Evidence |" in markdown


def test_structured_sections_and_models_are_supported_without_source_mutation():
    source = _source_brief(
        source_id="rules-source",
        summary="Account limits are documented in the payload.",
        source_payload={
            "business_rules": [
                {
                    "category": "threshold",
                    "rule_text": "Orders over $5,000 require manager approval.",
                    "confidence": "high",
                },
                "Billing rules: refunds must be issued to the original payment method.",
            ],
            "metadata": {
                "lifecycle_rules": [
                    "Subscriptions must transition to suspended after 3 failed payments."
                ]
            },
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_business_rules(source)
    model_result = summarize_source_business_rules(model)
    generated = generate_source_business_rules(model)
    derived = derive_source_business_rules(model)
    extracted = extract_source_business_rules(model)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.business_rules
    assert model_result.brief_id == "rules-source"
    assert model_result.summary["category_counts"]["threshold"] == 1
    assert model_result.summary["category_counts"]["billing"] == 1
    assert model_result.summary["category_counts"]["lifecycle"] == 1
    assert next(rule for rule in model_result.records if rule.category == "threshold").source_field == (
        "source_payload.business_rules[0]"
    )


def test_implementation_brief_and_objects_extract_business_rules():
    implementation_result = build_source_business_rules(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Only account owners may approve vendor payouts.",
                    "Uploaded contracts must include an effective date.",
                ],
                data_requirements="Status must move from draft to active only after validation passes.",
            )
        )
    )
    object_result = build_source_business_rules(
        SimpleNamespace(
            id="object-rules",
            title="Partner access",
            body="Partners should be blocked when compliance review fails.",
        )
    )

    assert implementation_result.brief_id == "impl-brief"
    assert {rule.category for rule in implementation_result.records} >= {
        "approval",
        "validation",
        "lifecycle",
    }
    assert next(rule for rule in implementation_result.records if rule.category == "approval").source_field == (
        "scope[0]"
    )
    assert object_result.brief_id == "object-rules"
    assert object_result.records[0].confidence == "medium"


def test_duplicate_and_near_duplicate_rules_collapse_deterministically():
    result = build_source_business_rules(
        {
            "id": "dupes",
            "summary": "Rule: Users must verify email before checkout.",
            "constraints": [
                "Users must verify email before checkout.",
                "users shall verify email before checkout",
                "Orders over $1,000 require approval.",
            ],
            "metadata": {"business_rules": ["Users must verify email before checkout."]},
        }
    )

    assert [rule.rule_text for rule in result.records] == [
        "Orders over $1,000 require approval.",
        "Users must verify email before checkout.",
    ]
    verified = next(rule for rule in result.records if "verify email" in rule.rule_text)
    assert verified.source_field == "metadata.business_rules[0]"
    assert verified.confidence == "high"
    assert len(verified.evidence) == 3
    assert result.summary["rule_count"] == 2


def test_empty_invalid_and_plain_text_inputs_have_stable_json_serialization():
    empty = build_source_business_rules(
        {"id": "empty", "title": "Copy update", "summary": "Update onboarding copy."}
    )
    invalid = build_source_business_rules(object())
    text = build_source_business_rules(
        "Customers can request refunds. Refunds must be approved within 5 business days."
    )
    payload = source_business_rules_to_dict(text)

    assert empty.brief_id == "empty"
    assert empty.records == ()
    assert empty.summary["rule_count"] == 0
    assert empty.summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert "No source business rules were found" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.records == ()
    assert json.loads(json.dumps(payload)) == payload
    assert text.to_dicts() == payload["business_rules"]
    assert source_business_rules_to_dicts(text) == payload["business_rules"]
    assert source_business_rules_to_dicts(text.records) == payload["records"]
    assert list(payload) == ["brief_id", "title", "summary", "business_rules", "records"]
    assert list(payload["business_rules"][0]) == [
        "rule_text",
        "category",
        "confidence",
        "source_field",
        "evidence",
    ]


def _source_brief(
    *,
    source_id="source-business-rules",
    title="Business rules",
    domain="operations",
    summary="General business rules.",
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


def _implementation_brief(*, scope=None, data_requirements=None):
    return {
        "id": "impl-brief",
        "source_brief_id": "source-brief",
        "title": "Operations controls",
        "domain": "operations",
        "target_user": "Ops",
        "buyer": "Finance",
        "workflow_context": "Approval workflow",
        "problem_statement": "Ops needs enforceable business rules.",
        "mvp_goal": "Ship rule extraction.",
        "product_surface": "Admin dashboard",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run rule extraction tests.",
        "definition_of_done": [],
        "status": "draft",
    }
