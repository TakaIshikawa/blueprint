import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_key_lifecycle_requirements import (
    SourceApiKeyLifecycleRequirement,
    SourceApiKeyLifecycleRequirementsReport,
    build_source_api_key_lifecycle_requirements,
    derive_source_api_key_lifecycle_requirements,
    extract_source_api_key_lifecycle_requirements,
    generate_source_api_key_lifecycle_requirements,
    source_api_key_lifecycle_requirements_to_dict,
    source_api_key_lifecycle_requirements_to_dicts,
    source_api_key_lifecycle_requirements_to_markdown,
    summarize_source_api_key_lifecycle_requirements,
)


def test_nested_source_payload_extracts_api_key_lifecycle_categories_in_order():
    result = build_source_api_key_lifecycle_requirements(
        _source_brief(
            source_payload={
                "api_keys": {
                    "creation": "Developers must create API keys with a name and show the secret once.",
                    "scopes": "API key scopes must support read-only, write, and webhook permissions.",
                    "expiration": "API key expiration requires keys to expire after 90 days.",
                    "rotation": "API key rotation must support regenerate with an overlap window.",
                    "revocation": "Admins can revoke and disable compromised API keys.",
                    "usage": "Last-used visibility must show last used timestamp for every key.",
                    "audit": "Audit log records API key events, actor, target key, and timestamp.",
                    "recovery": "Customer-facing recovery guidance explains lost secret and leaked key recovery.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceApiKeyLifecycleRequirementsReport)
    assert all(isinstance(record, SourceApiKeyLifecycleRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "key_creation",
        "key_scoping",
        "key_expiration",
        "key_rotation",
        "key_revocation",
        "last_used_visibility",
        "audit_logging",
        "recovery_guidance",
    ]
    assert by_category["key_scoping"].value == "read-only"
    assert by_category["key_expiration"].value == "90 days"
    assert by_category["key_revocation"].value == "revoke"
    assert by_category["audit_logging"].source_field == "source_payload.api_keys.audit"
    assert by_category["key_scoping"].suggested_owners == ("authorization", "security")
    assert by_category["audit_logging"].planning_notes[0].startswith("Record key lifecycle events")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API key creation must allow developers to generate named integration keys.",
            "API key scopes should restrict keys to project read-only or write permissions.",
        ],
        definition_of_done=[
            "API key rotation and revocation are documented for compromised keys.",
            "Audit logging records key lifecycle events and last-used visibility appears in settings.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "API key expiration must expire after 30 days.",
            "Recovery guidance should tell customers how to regenerate a lost secret.",
        ],
        security={"audit": "Audit trail logs API key events with actor and timestamp."},
        source_payload={"metadata": {"usage": "Last used visibility is required for stale API keys."}},
    )

    source_result = build_source_api_key_lifecycle_requirements(source)
    implementation_result = generate_source_api_key_lifecycle_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in source_result.records] == [
        "key_expiration",
        "last_used_visibility",
        "audit_logging",
        "recovery_guidance",
    ]
    assert source_result.records[0].source_field == "requirements[0]"
    assert {
        "key_creation",
        "key_scoping",
        "key_rotation",
        "key_revocation",
        "last_used_visibility",
        "audit_logging",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-api-keys"
    assert implementation_result.title == "API key lifecycle implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_key_flow():
    result = build_source_api_key_lifecycle_requirements(
        _source_brief(
            summary="Developers need API keys for integrations.",
            source_payload={
                "requirements": [
                    "API key creation must let users generate developer keys.",
                    "API key expiration should expire after 60 days.",
                    "Last-used visibility should show unused keys.",
                ]
            },
        )
    )

    assert [record.category for record in result.records] == [
        "key_creation",
        "key_expiration",
        "last_used_visibility",
    ]
    assert result.summary["missing_detail_flags"] == [
        "missing_key_scopes",
        "missing_rotation_or_revocation",
    ]
    assert "Specify API key scopes, permissions, or resource boundaries." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_key_scopes"] == 3
    assert result.summary["status"] == "needs_api_key_lifecycle_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="api-key-model",
        title="API key source",
        summary="API key requirements include key creation.",
        source_payload={
            "api_keys": {
                "scopes": "API key scopes must include read-only | write permissions.",
                "same_scopes": "API key scopes must include read-only | write permissions.",
                "rotation": "API key rotation must allow regenerate and audit logs record rotation events.",
            },
            "acceptance_criteria": [
                "API key scopes must include read-only | write permissions.",
                "API key revocation must disable compromised keys.",
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

    result = build_source_api_key_lifecycle_requirements(source)
    extracted = extract_source_api_key_lifecycle_requirements(model)
    derived = derive_source_api_key_lifecycle_requirements(model)
    payload = source_api_key_lifecycle_requirements_to_dict(result)
    markdown = source_api_key_lifecycle_requirements_to_markdown(result)
    scopes = next(record for record in result.records if record.category == "key_scoping")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_key_lifecycle_requirements(result) == result.summary
    assert source_api_key_lifecycle_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_key_lifecycle_requirements_to_dicts(result.records) == payload["records"]
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
    assert scopes.evidence == (
        "source_payload.api_keys.same_scopes: API key scopes must include read-only | write permissions.",
    )
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Key Lifecycle Requirements Report: api-key-model")
    assert "read-only \\| write permissions" in markdown


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-api-keys"
        summary = "No API key lifecycle, key rotation, or key revocation work is required for this release."

    object_result = build_source_api_key_lifecycle_requirements(
        SimpleNamespace(
            id="object-api-keys",
            summary="API key creation must generate integration keys.",
            api_keys={"scopes": "API key scopes require project read-only permissions."},
        )
    )
    negated = build_source_api_key_lifecycle_requirements(BriefLike())
    no_scope = build_source_api_key_lifecycle_requirements(
        _source_brief(summary="API keys are out of scope and no key lifecycle work is planned.")
    )
    unrelated = build_source_api_key_lifecycle_requirements(
        _source_brief(
            title="Keyboard shortcut copy",
            summary="Keyboard shortcut key labels and object key sorting should be updated.",
            source_payload={"requirements": ["Update cache key names and translation key copy."]},
        )
    )
    malformed = build_source_api_key_lifecycle_requirements({"source_payload": {"api_keys": {"notes": object()}}})
    blank = build_source_api_key_lifecycle_requirements("")
    invalid = build_source_api_key_lifecycle_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "key_creation": 0,
            "key_scoping": 0,
            "key_expiration": 0,
            "key_rotation": 0,
            "key_revocation": 0,
            "last_used_visibility": 0,
            "audit_logging": 0,
            "recovery_guidance": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_key_scopes": 0,
            "missing_rotation_or_revocation": 0,
        },
        "gap_messages": [],
        "status": "no_api_key_lifecycle_language",
    }
    assert [record.category for record in object_result.records] == ["key_creation", "key_scoping"]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API key lifecycle requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_key_lifecycle_requirements(unrelated) == expected_summary


def _source_brief(
    *,
    source_id="source-api-keys",
    title="API key lifecycle requirements",
    domain="integrations",
    summary="General API key lifecycle requirements.",
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
    brief_id="implementation-api-keys",
    title="API key lifecycle implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-api-keys",
        "title": title,
        "domain": "integrations",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "Developer integrations need API key lifecycle planning.",
        "problem_statement": "API key lifecycle requirements need to be extracted early.",
        "mvp_goal": "Plan API key creation, scoping, rotation, and revocation controls.",
        "product_surface": "developer settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run API key lifecycle extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
