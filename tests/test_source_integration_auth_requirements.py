import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_integration_auth_requirements import (
    SourceIntegrationAuthRequirement,
    SourceIntegrationAuthRequirementsReport,
    build_source_integration_auth_requirements,
    derive_source_integration_auth_requirements,
    extract_source_integration_auth_requirements,
    generate_source_integration_auth_requirements,
    source_integration_auth_requirements_to_dict,
    source_integration_auth_requirements_to_dicts,
    source_integration_auth_requirements_to_markdown,
    summarize_source_integration_auth_requirements,
)


def test_extracts_oauth_scope_and_token_requirements_with_evidence():
    result = build_source_integration_auth_requirements(
        _source_brief(
            summary=(
                "Slack integration must use OAuth 2.0 authorization code flow with "
                "least privilege scopes chat:write and channels:read. Refresh tokens "
                "expire after 90 days."
            ),
            source_payload={
                "requirements": [
                    "Bearer tokens must be sent to the partner API for webhook registration.",
                    "Webhook callbacks must validate an HMAC signature header.",
                ],
                "acceptance_criteria": [
                    "The OAuth consent screen only asks for the listed scopes.",
                ],
            },
        )
    )

    assert isinstance(result, SourceIntegrationAuthRequirementsReport)
    assert all(isinstance(record, SourceIntegrationAuthRequirement) for record in result.records)
    assert [record.finding_type for record in result.records] == [
        "auth_mechanism",
        "scope_constraints",
        "rotation_expiry",
    ]
    by_type = {record.finding_type: record for record in result.records}
    assert by_type["auth_mechanism"].confidence == "high"
    assert "oauth" in by_type["auth_mechanism"].auth_mechanisms
    assert "bearer_token" in by_type["auth_mechanism"].auth_mechanisms
    assert "webhook_signature" in by_type["auth_mechanism"].auth_mechanisms
    assert any("OAuth 2.0" in item for item in by_type["auth_mechanism"].evidence)
    assert "chat:write" in by_type["scope_constraints"].matched_terms
    assert "channels:read" in by_type["scope_constraints"].matched_terms
    assert any("least privilege scopes" in item for item in by_type["scope_constraints"].evidence)
    assert "summary" in by_type["scope_constraints"].source_field_paths
    assert by_type["rotation_expiry"].readiness == "ready_for_planning"
    assert result.summary["requirement_count"] == 3
    assert result.summary["type_counts"]["auth_mechanism"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_api_key_rotation_and_secret_storage_from_structured_metadata():
    result = build_source_integration_auth_requirements(
        _source_brief(
            source_payload={
                "integration_auth": {
                    "api_key": "Stripe API key must be stored in the secrets manager.",
                    "rotation": "Partner API keys must rotate every 30 days before expiry.",
                    "service_account": "Service account JSON key is required for Google sync.",
                }
            }
        )
    )

    by_type = {record.finding_type: record for record in result.records}
    assert [record.finding_type for record in result.records] == [
        "auth_mechanism",
        "secret_handling",
        "rotation_expiry",
    ]
    assert "api_key" in by_type["auth_mechanism"].auth_mechanisms
    assert "service_account" in by_type["auth_mechanism"].auth_mechanisms
    assert "source_payload.integration_auth.api_key" in by_type["secret_handling"].source_field_paths
    assert any("secrets manager" in item for item in by_type["secret_handling"].evidence)
    assert any("every 30 days" in term for term in by_type["rotation_expiry"].matched_terms)


def test_negated_non_scope_and_no_signal_language_stay_empty():
    result = build_source_integration_auth_requirements(
        _source_brief(
            title="Profile copy",
            summary="No external integration auth is required for this release.",
            source_payload={
                "requirements": [
                    "Keep the existing project scope unchanged.",
                    "This copy-only workflow has no OAuth, API key, or service account work needed.",
                ],
                "metadata": {"scope": "Only update the page scope and labels."},
            },
        )
    )
    repeat = build_source_integration_auth_requirements(
        _source_brief(
            title="Profile copy",
            summary="No external integration auth is required for this release.",
            source_payload={
                "requirements": [
                    "Keep the existing project scope unchanged.",
                    "This copy-only workflow has no OAuth, API key, or service account work needed.",
                ],
                "metadata": {"scope": "Only update the page scope and labels."},
            },
        )
    )

    assert result.to_dict() == repeat.to_dict()
    assert result.source_brief_id == "integration-auth-source"
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "auth_mechanism": 0,
            "secret_handling": 0,
            "scope_constraints": 0,
            "rotation_expiry": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "readiness_counts": {"ready_for_planning": 0, "needs_clarification": 0},
        "finding_types": [],
        "auth_mechanisms": [],
        "follow_up_question_count": 0,
        "status": "no_integration_auth_language",
    }
    assert "No source integration auth requirements were inferred" in result.to_markdown()


def test_iterable_object_model_serialization_markdown_and_no_mutation():
    source = _source_brief(
        source_id="integration-auth-model",
        summary="Connected app authentication requirements.",
        source_payload={
            "requirements": [
                "OAuth scopes must be limited to contacts.read for CRM import.",
                "Client secret must be stored encrypted and never logged.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            integration_points=[
                "Partner API uses API keys with credential rotation every 60 days."
            ]
        )
    )
    obj = SimpleNamespace(
        id="object-auth",
        summary="Webhook requests require signed HMAC authentication.",
    )

    mapping_result = build_source_integration_auth_requirements(source)
    model_result = generate_source_integration_auth_requirements(model)
    derived = derive_source_integration_auth_requirements(model)
    extracted = extract_source_integration_auth_requirements(model)
    iterable_result = build_source_integration_auth_requirements([model, brief, obj])
    payload = source_integration_auth_requirements_to_dict(model_result)
    markdown = source_integration_auth_requirements_to_markdown(model_result)
    invalid = build_source_integration_auth_requirements(42)

    assert source == original
    assert payload == source_integration_auth_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_integration_auth_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_integration_auth_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_integration_auth_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "finding_type",
        "auth_mechanisms",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "follow_up_questions",
        "confidence",
        "readiness",
    ]
    assert iterable_result.source_brief_id is None
    assert iterable_result.summary["source_count"] == 3
    assert "api_key" in iterable_result.summary["auth_mechanisms"]
    assert "webhook_signature" in iterable_result.summary["auth_mechanisms"]
    assert invalid.records == ()
    assert markdown == model_result.to_markdown()
    assert markdown.startswith(
        "# Source Integration Auth Requirements Report: integration-auth-model"
    )
    assert "| Type | Mechanisms | Confidence | Readiness | Source Field Paths | Evidence | Follow-up Questions |" in markdown


def _source_brief(
    *,
    source_id="integration-auth-source",
    title="Integration auth requirements",
    domain="platform",
    summary="General integration auth requirements.",
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


def _implementation_brief(*, integration_points=None):
    return {
        "id": "impl-auth",
        "source_brief_id": "source-auth",
        "title": "Partner auth rollout",
        "domain": "platform",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need integration auth requirements before planning.",
        "problem_statement": "Auth requirements need to be extracted early.",
        "mvp_goal": "Plan partner integration auth.",
        "product_surface": "integrations",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [],
        "validation_plan": "Review generated plan for auth coverage.",
        "definition_of_done": ["Integration auth requirements are represented."],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
