import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_key_management_requirements import (
    SourceApiKeyManagementRequirement,
    SourceApiKeyManagementRequirementsReport,
    build_source_api_key_management_requirements,
    derive_source_api_key_management_requirements,
    extract_source_api_key_management_requirements,
    generate_source_api_key_management_requirements,
    source_api_key_management_requirements_to_dict,
    source_api_key_management_requirements_to_dicts,
    source_api_key_management_requirements_to_markdown,
    summarize_source_api_key_management_requirements,
)


def test_structured_source_payload_api_keys_extracts_all_dimensions_in_order():
    result = build_source_api_key_management_requirements(
        _source_brief(
            source_payload={
                "api_keys": {
                    "creation": "Customers must create and generate API keys with a name and prefix.",
                    "one_time_secret_display": "The API key secret value must be shown once and never shown again.",
                    "scopes": "Scoped keys require read-only and write permissions per endpoint.",
                    "expiration": "API keys must support TTL and expires_at enforcement.",
                    "rotation": "API key rotation needs replacement keys and an overlap window.",
                    "revocation": "Users must revoke, disable, or delete API keys immediately.",
                    "audit_logging": "Audit logs record actor, timestamp, created_by, revoked_by, and key metadata.",
                    "rate_limits": "Each API key must associate with rate limits, quotas, and throttling policy.",
                    "environments": "Separate sandbox, staging, and production keys by environment.",
                    "customer_owner": "API keys are customer owned per tenant workspace and account.",
                    "admin_owner": "Platform admin and support operators manage keys in the admin console.",
                }
            }
        )
    )

    assert isinstance(result, SourceApiKeyManagementRequirementsReport)
    assert all(isinstance(record, SourceApiKeyManagementRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "key_creation",
        "one_time_secret_display",
        "scoped_permissions",
        "expiration",
        "rotation",
        "revocation",
        "audit_logging",
        "rate_limit_association",
        "environment_separation",
        "customer_ownership",
        "admin_ownership",
    ]
    by_category = {record.category: record for record in result.records}
    assert "create API keys" in by_category["key_creation"].required_capability
    assert by_category["rotation"].requirement_category == "rotation"
    assert by_category["revocation"].requirement_type == "revocation"
    assert by_category["audit_logging"].lifecycle_dimension == "audit_logging"
    assert by_category["audit_logging"].source_field == "source_payload.api_keys.audit_logging"
    assert by_category["one_time_secret_display"].confidence == "high"
    assert result.summary["requirement_count"] == 11
    assert result.summary["category_counts"]["rate_limit_association"] == 1
    assert result.summary["confidence_counts"]["high"] == 11
    assert result.summary["status"] == "ready_for_api_key_management_planning"


def test_prose_briefs_detect_required_api_key_lifecycle_dimensions():
    result = build_source_api_key_management_requirements(
        _source_brief(
            source_payload={
                "body": """
# Developer API key management

- Developer portal must create API keys for integrations.
- New API key secrets are displayed one time only.
- API keys require scoped permissions for read and write endpoints.
- Keys expire after 90 days and support rotation before expiration.
- Customers can revoke keys from workspace settings.
- Audit logging records API key creation, rotation, and revocation events.
- Rate limits are associated per API key and usage plan.
- Sandbox keys and production keys must stay separated by environment.
- Customer-owned keys belong to tenant accounts.
- Admin operators can disable keys from the admin console.
"""
            }
        )
    )

    assert [record.category for record in result.records] == [
        "key_creation",
        "one_time_secret_display",
        "scoped_permissions",
        "expiration",
        "rotation",
        "revocation",
        "audit_logging",
        "rate_limit_association",
        "environment_separation",
        "customer_ownership",
        "admin_ownership",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["scoped_permissions"].matched_terms
    assert by_category["one_time_secret_display"].evidence == (
        "source_payload.body: New API key secrets are displayed one time only.",
    )
    assert by_category["environment_separation"].source_field == "source_payload.body"


def test_implementation_brief_dict_object_and_plain_string_inputs_are_supported():
    implementation = generate_source_api_key_management_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "API key rotation and revocation must be supported for developer credentials.",
                    "Scoped permissions and per-key rate limits are required for API tokens.",
                ],
                definition_of_done=[
                    "One-time secret display and audit logs are implemented for generated API keys.",
                ],
            )
        )
    )
    structured = derive_source_api_key_management_requirements(
        {
            "id": "structured-api-keys",
            "metadata": {
                "api_keys": {
                    "creation": True,
                    "customer_owner": "Customer ownership is required per workspace.",
                    "admin_owner": "Admin ownership is required for support operators.",
                }
            },
        }
    )
    object_result = build_source_api_key_management_requirements(
        SimpleNamespace(id="object-api-keys", requirements="Production keys must be separate from sandbox keys.")
    )
    text_result = build_source_api_key_management_requirements(
        "API keys must expire after 30 days and support rotation."
    )

    assert implementation.source_id == "implementation-api-keys"
    assert [record.category for record in implementation.records] == [
        "one_time_secret_display",
        "scoped_permissions",
        "rotation",
        "revocation",
        "audit_logging",
        "rate_limit_association",
    ]
    assert [record.category for record in structured.records] == [
        "key_creation",
        "customer_ownership",
        "admin_ownership",
    ]
    assert structured.records[0].source_field == "metadata.api_keys.creation"
    assert object_result.records[0].category == "environment_separation"
    assert [record.category for record in text_result.records] == ["expiration", "rotation"]


def test_malformed_unrelated_and_negated_inputs_return_stable_empty_reports():
    generic = build_source_api_key_management_requirements(
        _source_brief(
            title="Keyboard settings",
            summary="Keyboard shortcuts and translation keys should be documented.",
            source_payload={"requirements": ["Foreign keys and cache keys are not developer API credentials."]},
        )
    )
    negated = build_source_api_key_management_requirements(
        _source_brief(
            title="Developer portal copy",
            summary="No API key management, scoped keys, rotation, revocation, or audit log work is required.",
            source_payload={"requirements": ["Update help text only."]},
        )
    )
    no_scope = build_source_api_key_management_requirements(
        _source_brief(summary="API keys are out of scope and no key management support is planned.")
    )
    malformed = build_source_api_key_management_requirements({"source_payload": {"api_keys": {"notes": object()}}})
    blank = build_source_api_key_management_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "key_creation": 0,
            "one_time_secret_display": 0,
            "scoped_permissions": 0,
            "expiration": 0,
            "rotation": 0,
            "revocation": 0,
            "audit_logging": 0,
            "rate_limit_association": 0,
            "environment_separation": 0,
            "customer_ownership": 0,
            "admin_ownership": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_api_key_management_language",
    }
    assert generic.records == ()
    assert negated.records == ()
    assert no_scope.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert generic.summary == expected_summary
    assert "No source API key management requirements were inferred." in generic.to_markdown()


def test_duplicate_suppression_preserves_source_fields_and_escapes_markdown():
    result = build_source_api_key_management_requirements(
        _source_brief(
            source_id="api-key-dupes",
            source_payload={
                "requirements": [
                    "Scoped API keys must support read | write endpoint permissions.",
                    "Scoped API keys must support read | write endpoint permissions.",
                    "API keys must enforce scoped permissions for webhook resources.",
                ],
                "acceptance_criteria": [
                    "Scoped API keys must support read | write endpoint permissions.",
                ],
            },
        )
    )

    assert [record.category for record in result.records] == ["scoped_permissions"]
    scoped = result.records[0]
    assert scoped.evidence == (
        "source_payload.acceptance_criteria[0]: Scoped API keys must support read | write endpoint permissions.",
        "source_payload.requirements[2]: API keys must enforce scoped permissions for webhook resources.",
    )
    assert scoped.source_fields == (
        "source_payload.acceptance_criteria[0]",
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
        "source_payload.requirements[2]",
    )
    assert "read \\| write endpoint permissions" in result.to_markdown()


def test_serialization_aliases_json_ordering_confidence_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="api-key-model",
        source_payload={
            "requirements": [
                "API key creation should generate a one-time secret display.",
                "API keys need audit logs for created_by and revoked_by.",
                "Customer-owned API keys can be revoked from account settings.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_api_key_management_requirements(source)
    model_result = extract_source_api_key_management_requirements(model)
    derived = derive_source_api_key_management_requirements(model)
    generated = generate_source_api_key_management_requirements(model)
    payload = source_api_key_management_requirements_to_dict(model_result)
    markdown = source_api_key_management_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_api_key_management_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_api_key_management_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_api_key_management_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_api_key_management_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "required_capability",
        "requirement_text",
        "evidence",
        "confidence",
        "source_field",
        "source_fields",
        "matched_terms",
    ]
    assert [record.category for record in model_result.records] == [
        "key_creation",
        "one_time_secret_display",
        "revocation",
        "audit_logging",
        "customer_ownership",
    ]
    assert [record.confidence for record in model_result.records] == ["high", "high", "high", "high", "high"]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source API Key Management Requirements Report: api-key-model")


def _source_brief(
    *,
    source_id="source-api-keys",
    title="API key management requirements",
    domain="api",
    summary="General API key management requirements.",
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
        "id": "implementation-api-keys",
        "source_brief_id": "source-api-keys",
        "title": "API key management rollout",
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "Teams need API key management requirements before developer integrations.",
        "problem_statement": "API key requirements need to be extracted early.",
        "mvp_goal": "Plan API key lifecycle work from source briefs.",
        "product_surface": "developer portal",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run API key management extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
