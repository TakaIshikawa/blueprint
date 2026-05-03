import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_password_reset_requirements import (
    SourcePasswordResetRequirement,
    SourcePasswordResetRequirementsReport,
    build_source_password_reset_requirements,
    derive_source_password_reset_requirements,
    extract_source_password_reset_requirements,
    generate_source_password_reset_requirements,
    source_password_reset_requirements_to_dict,
    source_password_reset_requirements_to_dicts,
    source_password_reset_requirements_to_markdown,
    summarize_source_password_reset_requirements,
)


def test_nested_source_payload_extracts_password_reset_categories_separately():
    result = build_source_password_reset_requirements(
        _source_brief(
            source_payload={
                "account_recovery": {
                    "reset": "Password reset must send a reset link to the verified email address.",
                    "ttl": "Reset token expiry must be 15 minutes.",
                    "reuse": "Reset tokens are single-use and invalidated after use.",
                    "email": "Email verification must confirm email ownership before reset.",
                    "mfa": "MFA recovery must support backup codes for lost devices.",
                    "lockout": "Lockout applies after 5 failed reset attempts with a cooldown.",
                    "support": "Support-assisted recovery requires identity verification and support approval.",
                    "audit": "Audit trail records reset events and recovery events.",
                    "abuse": "Abuse prevention requires rate limits, CAPTCHA, and enumeration protection.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourcePasswordResetRequirementsReport)
    assert all(isinstance(record, SourcePasswordResetRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "password_reset",
        "token_lifetime",
        "one_time_token",
        "email_verification",
        "mfa_recovery",
        "lockout",
        "support_recovery",
        "audit_trail",
        "abuse_prevention",
    ]
    assert by_category["token_lifetime"].value == "15 minutes"
    assert by_category["one_time_token"].value == "single-use"
    assert by_category["mfa_recovery"].value == "backup codes"
    assert by_category["support_recovery"].value == "identity verification"
    assert by_category["email_verification"].source_field == "source_payload.account_recovery.email"
    assert by_category["lockout"].source_field == "source_payload.account_recovery.lockout"
    assert by_category["support_recovery"].suggested_owners == ("support", "security", "identity")
    assert by_category["abuse_prevention"].suggested_owners == ("security", "identity")
    assert result.summary["requirement_count"] == 9
    assert result.summary["category_counts"]["mfa_recovery"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_brief_fields_and_nested_payload_evidence_paths_are_scanned():
    result = build_source_password_reset_requirements(
        _source_brief(
            title="Account recovery launch",
            summary="Password reset requirements include token lifetime and one-time token behavior.",
            description="Forgot password should send reset email verification.",
            requirements=[
                "Reset token expiry must be 30 minutes.",
                "One-time token behavior must prevent token reuse.",
            ],
            acceptance_criteria=[
                "MFA recovery supports backup codes for lost devices.",
                "Support recovery requires support ticket identity verification.",
            ],
            security={
                "lockout": "Account lockout should trigger after 6 failed reset attempts.",
                "audit": "Audit log records password reset and support recovery events.",
            },
            source_payload={
                "metadata": {
                    "abuse": "Abuse prevention must rate limit reset link resends by IP.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert {
        "password_reset",
        "token_lifetime",
        "one_time_token",
        "email_verification",
        "mfa_recovery",
        "lockout",
        "support_recovery",
        "audit_trail",
        "abuse_prevention",
    } <= set(by_category)
    assert by_category["token_lifetime"].source_field == "requirements[0]"
    assert by_category["mfa_recovery"].source_field == "acceptance_criteria[0]"
    assert by_category["abuse_prevention"].source_field == "source_payload.metadata.abuse"
    assert any(item.startswith("summary:") for item in by_category["password_reset"].evidence)


def test_plain_text_and_implementation_brief_support_multiple_categories():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Password reset must issue single-use reset tokens that expire after 20 minutes.",
                "MFA recovery should use backup codes and support-assisted recovery.",
            ],
            definition_of_done=[
                "Audit trail records reset events and support approvals.",
                "Abuse prevention rate limits reset requests and locks accounts after failed reset attempts.",
            ],
        )
    )
    text_result = build_source_password_reset_requirements(
        """
# Account recovery

- Forgot password sends reset links.
- Reset tokens expire after 10 minutes.
- Support recovery requires identity verification.
"""
    )
    implementation_result = generate_source_password_reset_requirements(implementation)

    assert [record.category for record in text_result.records] == [
        "password_reset",
        "token_lifetime",
        "support_recovery",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "password_reset",
        "token_lifetime",
        "one_time_token",
        "mfa_recovery",
        "lockout",
        "support_recovery",
        "audit_trail",
        "abuse_prevention",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-reset"
    assert implementation_result.title == "Password reset implementation"


def test_duplicate_evidence_merges_without_mutating_mapping():
    source = _source_brief(
        source_id="reset-dupes",
        source_payload={
            "recovery": {
                "ttl": "Reset token expiry must be 15 minutes.",
                "same_ttl": "Reset token expiry must be 15 minutes.",
                "lifetime": "Token lifetime is required for password reset.",
            },
            "acceptance_criteria": [
                "Reset token expiry must be 15 minutes.",
                "Audit trail must log reset events.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_password_reset_requirements(source)
    ttl = next(record for record in result.records if record.category == "token_lifetime")

    assert source == original
    assert ttl.evidence == (
        "source_payload.recovery.lifetime: Token lifetime is required for password reset.",
        "source_payload.recovery.ttl: Reset token expiry must be 15 minutes.",
    )
    assert ttl.confidence == "high"
    assert [record.category for record in result.records] == [
        "password_reset",
        "token_lifetime",
        "audit_trail",
    ]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="reset-model",
        title="Password reset source",
        summary="Password reset requirements include reset links and token lifetime.",
        source_payload={
            "requirements": [
                "Reset email must include email verification | ownership copy.",
                "MFA recovery supports recovery codes.",
                "Support recovery requires identity verification.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria", "security"}
        }
    )

    result = build_source_password_reset_requirements(model)
    extracted = extract_source_password_reset_requirements(model)
    derived = derive_source_password_reset_requirements(model)
    payload = source_password_reset_requirements_to_dict(result)
    markdown = source_password_reset_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_password_reset_requirements(result) == result.summary
    assert source_password_reset_requirements_to_dicts(result) == payload["requirements"]
    assert source_password_reset_requirements_to_dicts(result.records) == payload["records"]
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
        "suggested_plan_impacts",
    ]
    assert [record["category"] for record in payload["requirements"]] == [
        "password_reset",
        "token_lifetime",
        "email_verification",
        "mfa_recovery",
        "support_recovery",
    ]
    assert markdown.startswith("# Source Password Reset Requirements Report: reset-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown
    assert "email verification \\| ownership copy" in markdown


def test_negated_scope_empty_invalid_mapping_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-reset"
        summary = "No password reset or account recovery work is required for this release."

    object_result = build_source_password_reset_requirements(
        SimpleNamespace(
            id="object-reset",
            summary="Forgot password must send a reset email and support recovery handles escalations.",
            metadata={"mfa": "MFA recovery requires backup codes for lost devices."},
        )
    )
    negated = build_source_password_reset_requirements(BriefLike())
    no_password_scope = build_source_password_reset_requirements(
        _source_brief(summary="Passwordless only: no password or account recovery changes are in scope.")
    )
    unrelated_auth = build_source_password_reset_requirements(
        _source_brief(
            title="Login copy",
            summary="Authentication page copy should explain remember me labels.",
            source_payload={"requirements": ["Show login button and profile menu."]},
        )
    )
    malformed = build_source_password_reset_requirements({"source_payload": {"notes": object()}})
    blank = build_source_password_reset_requirements("")
    invalid = build_source_password_reset_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "password_reset": 0,
            "token_lifetime": 0,
            "one_time_token": 0,
            "email_verification": 0,
            "mfa_recovery": 0,
            "lockout": 0,
            "support_recovery": 0,
            "audit_trail": 0,
            "abuse_prevention": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_password_reset_language",
    }
    assert [record.category for record in object_result.records] == [
        "password_reset",
        "email_verification",
        "mfa_recovery",
        "support_recovery",
    ]
    assert negated.records == ()
    assert no_password_scope.records == ()
    assert unrelated_auth.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated_auth.summary == expected_summary
    assert unrelated_auth.to_dicts() == []
    assert "No source password reset requirements were inferred" in unrelated_auth.to_markdown()
    assert summarize_source_password_reset_requirements(unrelated_auth) == expected_summary


def _source_brief(
    *,
    source_id="source-reset",
    title="Password reset requirements",
    domain="authentication",
    summary="General password reset requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    security=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "description": description,
        "requirements": [] if requirements is None else requirements,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
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
    brief_id="implementation-reset",
    title="Password reset implementation",
    problem_statement="Implement source-backed password reset workflows.",
    mvp_goal="Ship password reset planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-reset",
        "title": title,
        "domain": "authentication",
        "target_user": "security operator",
        "buyer": "security",
        "workflow_context": "Account recovery operations",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "authentication",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run password reset extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
