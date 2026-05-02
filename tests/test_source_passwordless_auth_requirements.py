import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_passwordless_auth_requirements import (
    SourcePasswordlessAuthRequirement,
    SourcePasswordlessAuthRequirementsReport,
    build_source_passwordless_auth_requirements,
    derive_source_passwordless_auth_requirements,
    extract_source_passwordless_auth_requirements,
    generate_source_passwordless_auth_requirements,
    source_passwordless_auth_requirements_to_dict,
    source_passwordless_auth_requirements_to_dicts,
    source_passwordless_auth_requirements_to_markdown,
    summarize_source_passwordless_auth_requirements,
)


def test_extracts_passwordless_modes_from_markdown_with_evidence_confidence_and_notes():
    result = build_source_passwordless_auth_requirements(
        _source_brief(
            source_payload={
                "body": """
# Passwordless authentication

- Customers must receive magic links via email for sign-in, expires after 10 minutes, limited to 5 links per hour.
- Enterprise users should use WebAuthn passkeys before admin access.
- OTP code is required via sms when login from a new device, expires after 5 minutes, 3 attempts per user.
- Trusted devices must use device binding after successful sign-in.
- Session expiry must end passwordless auth sessions after 12 hours with idle timeout.
- Account recovery must support fallback for lost device cases.
- Rate limiting must throttle code resends with cooldown for 15 minutes.
- Audit events must be logged for passkey registration and recovery.
"""
            }
        )
    )

    assert isinstance(result, SourcePasswordlessAuthRequirementsReport)
    assert all(isinstance(record, SourcePasswordlessAuthRequirement) for record in result.records)
    assert [record.mode for record in result.records] == [
        "magic_link",
        "passkey_webauthn",
        "otp_code",
        "device_binding",
        "session_expiry",
        "fallback_recovery",
        "rate_limit",
        "audit_event",
    ]
    by_mode = {record.mode: record for record in result.records}
    assert by_mode["magic_link"].channel == "email for sign-in"
    assert by_mode["magic_link"].expiry == "10 minutes"
    assert by_mode["magic_link"].limit == "5 links per hour"
    assert by_mode["passkey_webauthn"].trigger == "admin access"
    assert by_mode["otp_code"].channel == "sms when login from a new device"
    assert by_mode["device_binding"].confidence == "high"
    assert by_mode["audit_event"].planning_note
    assert by_mode["magic_link"].evidence == (
        "source_payload.body: Customers must receive magic links via email for sign-in, expires after 10 minutes, limited to 5 links per hour.",
    )
    assert result.summary["requirement_count"] == 8
    assert result.summary["mode_counts"]["rate_limit"] == 1


def test_structured_payload_and_implementation_brief_are_supported():
    source = _source_brief(
        source_payload={
            "passwordless": [
                {
                    "mode": "passkey",
                    "audience": "privileged users",
                    "trigger": "admin access",
                    "fallback": "support reset and recovery email",
                    "audit": "security event export",
                },
                {
                    "method": "magic link",
                    "audience": "all users",
                    "channel": "email",
                    "expiry": "15 minutes",
                    "rate_limit": "4 links per hour",
                },
            ],
            "security": "Device binding must remember trusted devices for customers after login.",
        }
    )
    model_result = build_source_passwordless_auth_requirements(SourceBrief.model_validate(source))
    implementation_result = build_source_passwordless_auth_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "OTP code must verify admins via email when password reset starts, expires after 8 minutes.",
                    "Passwordless session expiry must be 12 hours for all users.",
                ],
                definition_of_done=[
                    "Audit events must be logged for magic link verification.",
                    "Fallback recovery must support lost device account recovery.",
                ],
            )
        )
    )

    assert [record.mode for record in model_result.records] == [
        "magic_link",
        "passkey_webauthn",
        "device_binding",
        "fallback_recovery",
        "audit_event",
    ]
    passkey = next(record for record in model_result.records if record.mode == "passkey_webauthn")
    assert passkey.audience == "privileged users"
    assert passkey.trigger == "admin access"
    assert passkey.source_field == "source_payload.passwordless[0]"
    assert [record.mode for record in implementation_result.records] == [
        "magic_link",
        "otp_code",
        "session_expiry",
        "fallback_recovery",
        "audit_event",
    ]
    by_mode = {record.mode: record for record in implementation_result.records}
    assert by_mode["otp_code"].trigger == "password reset starts, expires after 8 minutes"
    assert by_mode["session_expiry"].expiry == "12 hours for all users"
    assert by_mode["fallback_recovery"].detail == "Fallback recovery must support lost device account recovery"


def test_negated_blank_invalid_and_malformed_inputs_return_stable_empty_reports():
    empty = build_source_passwordless_auth_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={"body": "No passwordless, magic link, OTP, passkey, or audit changes are required."},
        )
    )
    repeat = build_source_passwordless_auth_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={"body": "No passwordless, magic link, OTP, passkey, or audit changes are required."},
        )
    )
    blank = build_source_passwordless_auth_requirements("")
    object_empty = build_source_passwordless_auth_requirements(
        SimpleNamespace(id="object-empty", summary="No passwordless auth impact or passkey changes are needed.")
    )
    invalid = build_source_passwordless_auth_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-passwordless"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "mode_counts": {
            "magic_link": 0,
            "passkey_webauthn": 0,
            "otp_code": 0,
            "device_binding": 0,
            "session_expiry": 0,
            "fallback_recovery": 0,
            "rate_limit": 0,
            "audit_event": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "modes": [],
        "missing_detail_flags": [],
    }
    assert "No passwordless authentication requirements were found" in empty.to_markdown()
    assert blank.records == ()
    assert object_empty.records == ()
    assert invalid.records == ()


def test_duplicate_candidates_merge_with_stable_source_field_and_missing_flags():
    result = build_source_passwordless_auth_requirements(
        {
            "id": "dupes",
            "source_payload": {
                "requirements": [
                    "Magic link sign-in is required via email for customers.",
                    "Magic link sign-in is required via email for customers.",
                ],
                "passwordless": {
                    "otp": "OTP code must verify customers via sms when risky login occurs.",
                },
            },
        }
    )

    assert [record.mode for record in result.records] == ["magic_link", "otp_code"]
    assert result.records[0].evidence == (
        "source_payload.requirements[0]: Magic link sign-in is required via email for customers.",
    )
    assert result.records[0].source_field == "source_payload.requirements[0]"
    assert "expiry" in result.records[0].missing_detail_flags
    assert result.records[1].trigger == "risky login occurs"


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="passwordless-model",
        source_payload={
            "requirements": [
                "Passkeys must be available to enterprise users before admin access.",
                "Magic links must escape plan | recovery details via email, expires after 20 minutes.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_passwordless_auth_requirements(source)
    model_result = generate_source_passwordless_auth_requirements(model)
    derived = derive_source_passwordless_auth_requirements(model)
    extracted = extract_source_passwordless_auth_requirements(model)
    text_result = build_source_passwordless_auth_requirements("OTP code must verify users via email on password reset.")
    object_result = build_source_passwordless_auth_requirements(
        SimpleNamespace(id="object-passwordless", metadata={"passwordless": "Audit events must be logged for passkey sign-in."})
    )
    payload = source_passwordless_auth_requirements_to_dict(model_result)
    markdown = source_passwordless_auth_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_passwordless_auth_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_passwordless_auth_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_passwordless_auth_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_passwordless_auth_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "mode",
        "requirement_mode",
        "detail",
        "trigger",
        "audience",
        "channel",
        "expiry",
        "limit",
        "evidence",
        "source_field",
        "confidence",
        "missing_detail_flags",
        "planning_note",
    ]
    assert [record.mode for record in model_result.records] == ["magic_link", "passkey_webauthn"]
    assert markdown == model_result.to_markdown()
    assert (
        "| Source Brief | Mode | Detail | Trigger | Audience | Channel | Expiry | Limit | Source Field | Confidence | Missing Details | Planning Note | Evidence |"
        in markdown
    )
    assert "plan \\| recovery details" in markdown
    assert text_result.records[0].mode == "otp_code"
    assert object_result.records[0].mode == "passkey_webauthn"
    assert object_result.records[1].mode == "audit_event"


def _source_brief(
    *,
    source_id="sb-passwordless",
    title="Passwordless auth requirements",
    domain="authentication",
    summary="General passwordless authentication requirements.",
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
        "id": "impl-passwordless",
        "source_brief_id": "source-passwordless",
        "title": "Passwordless auth rollout",
        "domain": "authentication",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need passwordless auth requirements before task generation.",
        "problem_statement": "Passwordless requirements need to be extracted early.",
        "mvp_goal": "Plan passwordless auth work from source briefs.",
        "product_surface": "authentication",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review passwordless auth challenge flows.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
