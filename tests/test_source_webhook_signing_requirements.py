import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_webhook_signing_requirements import (
    SourceWebhookSigningRequirement,
    SourceWebhookSigningRequirementsReport,
    build_source_webhook_signing_requirements,
    derive_source_webhook_signing_requirements,
    extract_source_webhook_signing_requirements,
    generate_source_webhook_signing_requirements,
    source_webhook_signing_requirements_to_dict,
    source_webhook_signing_requirements_to_dicts,
    source_webhook_signing_requirements_to_markdown,
    summarize_source_webhook_signing_requirements,
)


def test_nested_source_payload_extracts_webhook_signing_categories_in_order():
    result = build_source_webhook_signing_requirements(
        _source_brief(
            source_payload={
                "webhook_signing": {
                    "secret": "Webhook signing secret creation must generate a display-once secret.",
                    "verification": "Webhook signature verification must validate the signature header with HMAC-SHA256.",
                    "timestamp": "Webhook timestamp tolerance must reject signatures older than 5 minutes.",
                    "replay": "Webhook replay prevention must dedupe delivery id values.",
                    "rotation": "Webhook signing secret rotation must regenerate a new secret.",
                    "grace": "Webhook multi-secret grace period must accept both old and new secrets for 7 days.",
                    "failure": "Webhook invalid signature handling must reject the request with 401.",
                    "audit": "Audit log records webhook signature verification failure with actor and timestamp.",
                    "docs": "Customer documentation must include webhook verification examples.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceWebhookSigningRequirementsReport)
    assert all(isinstance(record, SourceWebhookSigningRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "signing_secret_creation",
        "signature_verification",
        "timestamp_tolerance",
        "replay_prevention",
        "secret_rotation",
        "multi_secret_grace_period",
        "failure_handling",
        "audit_logging",
        "customer_documentation",
    ]
    assert by_category["signature_verification"].value == "hmac-sha256"
    assert by_category["timestamp_tolerance"].value == "5 minutes"
    assert by_category["failure_handling"].value == "401"
    assert by_category["audit_logging"].source_field == "source_payload.webhook_signing.audit"
    assert by_category["signature_verification"].suggested_owners == ("security", "integrations")
    assert by_category["customer_documentation"].planning_notes[0].startswith("Provide customer-facing verification docs")
    assert result.summary["requirement_count"] == 9
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Webhook signing secret creation must allow developers to generate per-endpoint signing secrets.",
            "Webhook signature verification should validate HMAC SHA256 signatures from the signature header.",
        ],
        definition_of_done=[
            "Webhook secret rotation supports a dual-secret grace period for old and new secrets.",
            "Audit logging records webhook verification failures and invalid signature rejection.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Webhook timestamp tolerance must reject payloads after 10 minutes.",
            "Customer docs should explain webhook replay prevention with delivery id dedupe.",
        ],
        security={"audit": "Audit trail logs webhook signing events with endpoint and timestamp."},
        source_payload={"metadata": {"failure": "Webhook invalid signature failure returns 403."}},
    )

    source_result = build_source_webhook_signing_requirements(source)
    implementation_result = generate_source_webhook_signing_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in source_result.records] == [
        "timestamp_tolerance",
        "replay_prevention",
        "failure_handling",
        "audit_logging",
        "customer_documentation",
    ]
    assert source_result.records[0].source_field == "requirements[0]"
    assert {
        "signing_secret_creation",
        "signature_verification",
        "secret_rotation",
        "multi_secret_grace_period",
        "failure_handling",
        "audit_logging",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-webhook-signing"
    assert implementation_result.title == "Webhook signing implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_webhook_flow():
    result = build_source_webhook_signing_requirements(
        _source_brief(
            summary="Developers need webhook signing for integrations.",
            source_payload={
                "requirements": [
                    "Webhook signing secret creation must let users generate a signing secret.",
                    "Webhook failure handling should reject invalid signatures.",
                    "Customer documentation should describe webhook signing setup.",
                ]
            },
        )
    )

    assert [record.category for record in result.records] == [
        "signing_secret_creation",
        "failure_handling",
        "customer_documentation",
    ]
    assert result.summary["missing_detail_flags"] == [
        "missing_signature_verification",
        "missing_rotation_or_grace_period",
    ]
    assert "Specify webhook signature verification algorithm, headers, or comparison behavior." in result.summary["gap_messages"]
    assert "Specify signing secret rotation and any multi-secret grace period." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_rotation_or_grace_period"] == 3
    assert result.summary["status"] == "needs_webhook_signing_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="webhook-signing-model",
        title="Webhook signing source",
        summary="Webhook signing source.",
        source_payload={
            "webhook_signing": {
                "verification": "Webhook signature verification must use HMAC-SHA256 | constant-time compare.",
                "same_verification": "Webhook signature verification must use HMAC-SHA256 | constant-time compare.",
                "rotation": "Webhook secret rotation must support dual-secret grace period and audit logs record rotation events.",
            },
            "acceptance_criteria": [
                "Webhook signature verification must use HMAC-SHA256 | constant-time compare.",
                "Webhook invalid signature handling must reject requests with 401.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"requirements", "security"}
        }
    )

    result = build_source_webhook_signing_requirements(source)
    extracted = extract_source_webhook_signing_requirements(model)
    derived = derive_source_webhook_signing_requirements(model)
    payload = source_webhook_signing_requirements_to_dict(result)
    markdown = source_webhook_signing_requirements_to_markdown(result)
    verification = next(record for record in result.records if record.category == "signature_verification")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_webhook_signing_requirements(result) == result.summary
    assert source_webhook_signing_requirements_to_dicts(result) == payload["requirements"]
    assert source_webhook_signing_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
        "gap_messages",
    ]
    assert verification.evidence == (
        "source_payload.webhook_signing.same_verification: Webhook signature verification must use HMAC-SHA256 | constant-time compare.",
    )
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source Webhook Signing Requirements Report: webhook-signing-model")
    assert "HMAC-SHA256 \\| constant-time compare" in markdown


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-webhook-signing"
        summary = "No webhook signing or webhook signature verification work is required for this release."

    object_result = build_source_webhook_signing_requirements(
        SimpleNamespace(
            id="object-webhook-signing",
            summary="Webhook signing secret creation must generate a signing secret.",
            webhook_signing={"verification": "Webhook signature verification requires HMAC SHA256."},
        )
    )
    negated = build_source_webhook_signing_requirements(BriefLike())
    no_scope = build_source_webhook_signing_requirements(
        _source_brief(summary="Webhook signatures are out of scope and no webhook signing work is planned.")
    )
    unrelated = build_source_webhook_signing_requirements(
        _source_brief(
            title="Signature copy",
            summary="Document signature labels and sign in button copy should be updated.",
            source_payload={"requirements": ["Update cache key names and translation key copy."]},
        )
    )
    malformed = build_source_webhook_signing_requirements({"source_payload": {"webhook_signing": {"notes": object()}}})
    blank = build_source_webhook_signing_requirements("")
    invalid = build_source_webhook_signing_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "signing_secret_creation": 0,
            "signature_verification": 0,
            "timestamp_tolerance": 0,
            "replay_prevention": 0,
            "secret_rotation": 0,
            "multi_secret_grace_period": 0,
            "failure_handling": 0,
            "audit_logging": 0,
            "customer_documentation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_signature_verification": 0,
            "missing_rotation_or_grace_period": 0,
        },
        "gap_messages": [],
        "status": "no_webhook_signing_language",
    }
    assert [record.category for record in object_result.records] == [
        "signing_secret_creation",
        "signature_verification",
    ]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source webhook signing requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_webhook_signing_requirements(unrelated) == expected_summary


def _source_brief(
    *,
    source_id="source-webhook-signing",
    title="Webhook signing requirements",
    domain="integrations",
    summary="General webhook signing requirements.",
    requirements=None,
    security=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "security": {} if security is None else security,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-webhook-signing",
    title="Webhook signing implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-webhook-signing",
        "title": title,
        "domain": "integrations",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "Developer integrations need webhook signing planning.",
        "problem_statement": "Webhook signing requirements need to be extracted early.",
        "mvp_goal": "Plan webhook signing secret creation, verification, rotation, and failure handling.",
        "product_surface": "developer settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run webhook signing extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
