import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_mfa_requirements import (
    SourceMfaRequirement,
    SourceMfaRequirementsReport,
    build_source_mfa_requirements,
    derive_source_mfa_requirements,
    extract_source_mfa_requirements,
    generate_source_mfa_requirements,
    source_mfa_requirements_to_dict,
    source_mfa_requirements_to_dicts,
    source_mfa_requirements_to_markdown,
    summarize_source_mfa_requirements,
)


def test_extracts_mfa_methods_triggers_audience_fallback_and_evidence_from_markdown():
    result = build_source_mfa_requirements(
        _source_brief(
            source_payload={
                "body": """
# MFA requirements

- Admins must use TOTP when signing in to the admin console, with audit evidence logged.
- Enterprise users should use WebAuthn passkeys before sensitive export.
- SMS OTP is required for customers on new device login with email fallback.
- Email OTP must support password reset verification.
- Backup codes must be generated during MFA enrollment for account recovery.
- Remembered devices may skip MFA for 30 days after successful challenge.
"""
            }
        )
    )

    assert isinstance(result, SourceMfaRequirementsReport)
    assert all(isinstance(record, SourceMfaRequirement) for record in result.records)
    assert [record.method for record in result.records] == [
        "totp",
        "sms_otp",
        "email_otp",
        "webauthn_passkey",
        "backup_codes",
        "enrollment",
        "recovery",
        "remembered_devices",
    ]
    by_method = {record.method: record for record in result.records}
    assert by_method["totp"].audience == "Admins"
    assert by_method["totp"].trigger == "signing in to the admin console, with audit evidence logged"
    assert by_method["totp"].confidence == "high"
    assert by_method["sms_otp"].fallback_recovery == "email fallback"
    assert by_method["backup_codes"].fallback_recovery == "Backup codes must be generated during MFA enrollment for account recovery"
    assert by_method["webauthn_passkey"].trigger == "sensitive export"
    assert result.summary["requirement_count"] == 8
    assert result.summary["method_counts"]["remembered_devices"] == 1


def test_structured_payload_and_implementation_brief_are_supported():
    source = _source_brief(
        source_payload={
            "mfa_policy": [
                {
                    "method": "passkey",
                    "audience": "privileged users",
                    "trigger": "admin access",
                    "fallback": "backup codes and support reset",
                    "evidence": "security event export",
                },
                {
                    "method": "SMS OTP",
                    "audience": "all users",
                    "trigger": "new device login",
                },
            ],
            "admin": "MFA policy must allow admins to enforce mandatory MFA for operators.",
        }
    )
    model_result = build_source_mfa_requirements(SourceBrief.model_validate(source))
    implementation_result = build_source_mfa_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Step-up authentication must challenge privileged users before payment change.",
                    "MFA enrollment is required for new users before launch.",
                ],
                definition_of_done=[
                    "Account recovery must support factor reset for lost device cases.",
                ],
            )
        )
    )

    assert [record.method for record in model_result.records] == [
        "sms_otp",
        "webauthn_passkey",
        "admin_enforcement",
    ]
    passkey = next(record for record in model_result.records if record.method == "webauthn_passkey")
    assert passkey.audience == "privileged users"
    assert passkey.trigger == "admin access"
    assert passkey.fallback_recovery == "backup codes and support reset"
    assert passkey.source_field == "source_payload.mfa_policy[0]"
    assert [record.method for record in implementation_result.records] == [
        "step_up",
        "enrollment",
        "recovery",
    ]
    by_method = {record.method: record for record in implementation_result.records}
    assert by_method["step_up"].trigger == "payment change"
    assert by_method["recovery"].fallback_recovery == "Account recovery must support factor reset for lost device cases"


def test_no_mfa_signal_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_mfa_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={"body": "No MFA, 2FA, OTP, passkey, or step-up changes are required."},
        )
    )
    repeat = build_source_mfa_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={"body": "No MFA, 2FA, OTP, passkey, or step-up changes are required."},
        )
    )
    object_empty = build_source_mfa_requirements(
        SimpleNamespace(id="object-empty", summary="No MFA impact or 2FA changes are needed.")
    )
    invalid = build_source_mfa_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-mfa"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "method_counts": {
            "totp": 0,
            "sms_otp": 0,
            "email_otp": 0,
            "webauthn_passkey": 0,
            "backup_codes": 0,
            "step_up": 0,
            "enrollment": 0,
            "recovery": 0,
            "remembered_devices": 0,
            "admin_enforcement": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "methods": [],
    }
    assert "No MFA requirements were found" in empty.to_markdown()
    assert object_empty.records == ()
    assert invalid.records == ()


def test_duplicate_candidates_merge_with_stable_evidence_and_source_field():
    result = build_source_mfa_requirements(
        {
            "id": "dupes",
            "source_payload": {
                "requirements": [
                    "TOTP is required for admins when login happens from a new device.",
                    "TOTP is required for admins when login happens from a new device.",
                ],
                "mfa": {
                    "totp": "TOTP is required for admins when login happens from a new device.",
                    "sms": "SMS OTP must challenge customers on risky login.",
                },
            },
        }
    )

    assert [record.method for record in result.records] == ["totp", "sms_otp"]
    assert result.records[0].evidence == (
        "source_payload.requirements[0]: TOTP is required for admins when login happens from a new device.",
    )
    assert result.records[0].source_field == "source_payload.requirements[0]"
    assert result.records[1].trigger == "risky login"


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="mfa-model",
        source_payload={
            "requirements": [
                "Passkeys must be available to enterprise users before admin access.",
                "Backup codes must escape plan | recovery details during MFA enrollment.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_mfa_requirements(source)
    model_result = generate_source_mfa_requirements(model)
    derived = derive_source_mfa_requirements(model)
    extracted = extract_source_mfa_requirements(model)
    text_result = build_source_mfa_requirements("Email OTP must verify users on password reset.")
    object_result = build_source_mfa_requirements(
        SimpleNamespace(id="object-mfa", metadata={"mfa": "Admin enforcement must require MFA for admins."})
    )
    payload = source_mfa_requirements_to_dict(model_result)
    markdown = source_mfa_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_mfa_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_mfa_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_mfa_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_mfa_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "method",
        "trigger",
        "audience",
        "fallback_recovery",
        "evidence",
        "source_field",
        "confidence",
        "planning_note",
    ]
    assert [record.method for record in model_result.records] == [
        "webauthn_passkey",
        "backup_codes",
        "enrollment",
        "recovery",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Method | Trigger | Audience | Fallback/Recovery | Source Field | Confidence | Planning Note | Evidence |" in markdown
    assert "plan \\| recovery details" in markdown
    assert text_result.records[0].method == "email_otp"
    assert object_result.records[0].method == "admin_enforcement"


def _source_brief(
    *,
    source_id="sb-mfa",
    title="MFA requirements",
    domain="authentication",
    summary="General MFA requirements.",
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
        "id": "impl-mfa",
        "source_brief_id": "source-mfa",
        "title": "MFA rollout",
        "domain": "authentication",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need MFA policy requirements before task generation.",
        "problem_statement": "MFA requirements need to be extracted early.",
        "mvp_goal": "Plan MFA work from source briefs.",
        "product_surface": "authentication",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review authentication challenge flows.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
