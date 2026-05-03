import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_authentication_requirements import (
    SourceApiAuthenticationRequirement,
    SourceApiAuthenticationRequirementsReport,
    build_source_api_authentication_requirements,
    derive_source_api_authentication_requirements,
    extract_source_api_authentication_requirements,
    generate_source_api_authentication_requirements,
    source_api_authentication_requirements_to_dict,
    source_api_authentication_requirements_to_dicts,
    source_api_authentication_requirements_to_markdown,
    summarize_source_api_authentication_requirements,
)


def test_comprehensive_api_authentication_requirements_extracted():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-api-auth",
            title="Add API authentication for developer portal",
            body=(
                "Implement API key and bearer token authentication for the developer API endpoints. "
                "API keys should be created via admin portal with bcrypt hashing and secure storage in vault. "
                "Bearer tokens should follow JWT format with exp claims for token expiry after 1 hour. "
                "Support OAuth client credentials flow with client ID and client secret validation. "
                "Implement credential rotation and manual revocation for compromised keys. "
                "Return 401 Unauthorized with WWW-Authenticate header for invalid or expired tokens. "
                "Add comprehensive tests for auth success, 401 failures, expiry, rotation, and revocation."
            ),
        )
    )

    assert isinstance(result, SourceApiAuthenticationRequirementsReport)
    assert result.brief_id == "brief-api-auth"
    assert result.title == "Add API authentication for developer portal"
    assert len(result.requirements) == 8
    assert result.records == result.requirements
    assert result.findings == result.requirements
    categories = [req.category for req in result.requirements]
    assert categories == [
        "api_key",
        "bearer_token",
        "oauth_client_credentials",
        "token_expiry",
        "credential_storage",
        "credential_rotation_revocation",
        "auth_failure_response",
        "test_coverage",
    ]
    storage_req = next(req for req in result.requirements if req.category == "credential_storage")
    assert storage_req.confidence in ("high", "medium")
    assert storage_req.value and ("bcrypt" in storage_req.value or "hash" in storage_req.value)
    assert len(storage_req.planning_notes) > 0
    assert result.summary["requirement_count"] == 8
    assert result.summary["status"] == "ready_for_planning"
    assert result.summary["missing_detail_flags"] == []


def test_negated_scope_produces_no_requirements():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-no-auth",
            title="Public API endpoint",
            scope="This API is public and unauthenticated. No API authentication or API keys are required.",
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_api_auth_language"


def test_partial_requirements_report_missing_detail_flags():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-partial",
            title="Add API key authentication",
            body=(
                "Create API keys for developer access. "
                "Return 401 Unauthorized for invalid API keys."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "api_key" in categories
    assert "auth_failure_response" in categories
    assert result.summary["requirement_count"] > 0
    assert "missing_credential_hashing_or_encryption" in result.summary["missing_detail_flags"]
    assert "missing_rotation_or_revocation" in result.summary["missing_detail_flags"]
    assert result.summary["status"] == "needs_api_auth_details"


def test_structured_metadata_fields_contribute_evidence():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-metadata",
            title="Developer API",
            metadata={
                "authentication": "Bearer tokens with JWT",
                "token_expiry": "Access tokens expire after 15 minutes",
                "credential_storage": "Hashed with argon2",
                "security": {"rotation": "Keys can be rotated manually", "revocation": "Support instant revocation"},
            },
        )
    )

    categories = [req.category for req in result.requirements]
    assert "bearer_token" in categories
    assert "token_expiry" in categories
    assert "credential_storage" in categories
    assert "credential_rotation_revocation" in categories

    expiry_req = next(req for req in result.requirements if req.category == "token_expiry")
    assert expiry_req.value and ("15 minutes" in expiry_req.value or "expire" in expiry_req.value)

    storage_req = next(req for req in result.requirements if req.category == "credential_storage")
    assert storage_req.value and ("argon2" in storage_req.value or "hash" in storage_req.value)


def test_deduplication_and_stable_ordering():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-dedup",
            title="API authentication",
            body="Use API keys. API keys should be hashed. Rotate API keys.",
            requirements=["Support API key authentication", "Hash API keys with bcrypt", "Allow API key rotation"],
        )
    )

    api_key_reqs = [req for req in result.requirements if req.category == "api_key"]
    assert len(api_key_reqs) == 1
    assert len(api_key_reqs[0].evidence) <= 5

    storage_reqs = [req for req in result.requirements if req.category == "credential_storage"]
    assert len(storage_reqs) == 1


def test_implementation_brief_input():
    brief = {
        "id": "impl-brief-api-auth",
        "source_brief_id": "source-1",
        "title": "API authentication implementation",
        "problem_statement": "Add API key and OAuth support",
        "mvp_goal": "Implement API key validation, OAuth client credentials, token expiry, 401 responses, and tests.",
        "scope": ["API authentication"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Test auth",
        "definition_of_done": ["Auth works"],
        "status": "draft",
    }

    result = build_source_api_authentication_requirements(brief)

    assert result.brief_id == "impl-brief-api-auth"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "api_key" in categories
    assert "oauth_client_credentials" in categories


def test_model_validation_and_serialization():
    brief = _brief(
        "brief-serial",
        title="Bearer token auth",
        body="Implement bearer token authentication with JWT, token expiry, bcrypt hashing, rotation, 401 errors, and tests.",
    )
    original = copy.deepcopy(brief)

    result = build_source_api_authentication_requirements(brief)
    payload = source_api_authentication_requirements_to_dict(result)
    dicts = source_api_authentication_requirements_to_dicts(result)
    markdown = source_api_authentication_requirements_to_markdown(result)

    assert brief == original
    assert derive_source_api_authentication_requirements(brief).to_dict() == result.to_dict()
    assert generate_source_api_authentication_requirements(brief).to_dict() == result.to_dict()
    assert extract_source_api_authentication_requirements(brief) == result.requirements
    assert summarize_source_api_authentication_requirements(brief) == result.summary
    assert summarize_source_api_authentication_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert dicts == payload["requirements"]
    assert source_api_authentication_requirements_to_dicts(result.requirements) == dicts
    assert "# Source API Authentication Requirements Report: brief-serial" in markdown
    assert "bearer_token" in markdown
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


def test_string_input():
    result = build_source_api_authentication_requirements(
        "API keys should be created with bcrypt hashing. "
        "Support bearer token authentication with OAuth client credentials and JWT tokens with exp claims. "
        "Tokens expire after 30 minutes with expiry validation. "
        "Return 401 Unauthorized for invalid tokens. "
        "Add auth tests."
    )

    categories = [req.category for req in result.requirements]
    assert "api_key" in categories
    assert "bearer_token" in categories
    assert "oauth_client_credentials" in categories
    assert "credential_storage" in categories
    assert "auth_failure_response" in categories


def test_object_with_attributes():
    obj = SimpleNamespace(
        id="obj-auth",
        title="API auth",
        description="Add API key and bearer token auth",
        body="Use bcrypt for credential hashing. Support token rotation and revocation. Return 401 for auth failures.",
        authentication="API keys and bearer tokens",
        security="Encrypted credentials with rotation support",
    )

    result = build_source_api_authentication_requirements(obj)

    assert result.brief_id == "obj-auth"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "api_key" in categories or "bearer_token" in categories
    assert "credential_storage" in categories
    assert "credential_rotation_revocation" in categories


def test_unrelated_authentication_not_detected():
    result = build_source_api_authentication_requirements(
        _brief(
            "brief-unrelated",
            title="User login",
            body=(
                "Improve user login page. "
                "Add password authentication with session cookies. "
                "Support single sign-on for browser-based authentication."
            ),
        )
    )

    assert result.requirements == ()
    assert result.summary["status"] == "no_api_auth_language"


def _brief(brief_id, *, title=None, body=None, scope=None, requirements=None, metadata=None):
    brief = {
        "id": brief_id,
        "title": title or brief_id,
        "body": body or "",
        "status": "draft",
    }
    if scope is not None:
        brief["scope"] = scope
    if requirements is not None:
        brief["requirements"] = requirements
    if metadata is not None:
        brief["metadata"] = metadata
    return brief
