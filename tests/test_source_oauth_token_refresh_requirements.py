import json

from blueprint.source_oauth_token_refresh_requirements import (
    build_source_oauth_token_refresh_requirements,
    derive_source_oauth_token_refresh_requirements,
    extract_source_oauth_token_refresh_requirements,
    generate_source_oauth_token_refresh_requirements,
    source_oauth_token_refresh_requirements_to_dict,
    source_oauth_token_refresh_requirements_to_dicts,
    source_oauth_token_refresh_requirements_to_markdown,
    summarize_source_oauth_token_refresh_requirements,
)


def test_extracts_all_oauth_refresh_categories():
    result = build_source_oauth_token_refresh_requirements(_source([
        "OAuth token refresh refresh token storage must encrypt tokens in a vault.",
        "OAuth token refresh rotation must replace refresh tokens and detect reuse.",
        "OAuth token refresh expiry handling must refresh before expiration with clock skew.",
        "OAuth token refresh retry backoff must retry 5xx failures with jitter.",
        "OAuth token refresh revocation must revoke tokens when users disconnect OAuth.",
        "OAuth token refresh consent scope changes must trigger reconsent for permission changes.",
        "OAuth token refresh error recovery must recover invalid_grant by asking users to reauthorize.",
        "OAuth token refresh audit logging must log client id, timestamp, success, and failure.",
    ]))

    assert [record.requirement_type for record in result.records] == [
        "refresh_token_storage",
        "rotation",
        "expiry_handling",
        "retry_backoff",
        "revocation",
        "consent_scope_changes",
        "error_recovery",
        "audit_logging",
    ]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_token_storage_rotation_and_revocation_details():
    result = derive_source_oauth_token_refresh_requirements(
        "OAuth token refresh refresh token storage is required. "
        "OAuth token refresh rotation is required. "
        "OAuth token refresh revocation is required."
    )

    assert result.summary["missing_detail_flags"] == [
        "missing_token_storage",
        "missing_rotation",
        "missing_revocation",
    ]


def test_aliases_and_serializers_are_consistent_with_neighboring_modules():
    result = extract_source_oauth_token_refresh_requirements(
        _source(["OAuth token refresh retry backoff must retry timeouts with backoff."], "oauth-refresh-1")
    )
    payload = source_oauth_token_refresh_requirements_to_dict(result)

    assert generate_source_oauth_token_refresh_requirements(
        "OAuth token refresh audit logging must log refresh audit events."
    ).summary["requirement_count"] == 1
    assert summarize_source_oauth_token_refresh_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "oauth-refresh-1"
    assert source_oauth_token_refresh_requirements_to_dicts(result) == payload["records"]
    assert source_oauth_token_refresh_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source OAuth Token Refresh Requirements Report: oauth-refresh-1" in source_oauth_token_refresh_requirements_to_markdown(result)
    assert build_source_oauth_token_refresh_requirements("No OAuth token refresh changes are required.").records == ()


def _source(lines, source_id="oauth-refresh-source"):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "title": "OAuth token refresh",
        "summary": "OAuth token refresh planning",
        "source_payload": {"requirements": lines},
        "source_links": {},
    }
