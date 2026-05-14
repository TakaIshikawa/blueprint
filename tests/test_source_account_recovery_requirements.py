import json

from blueprint.domain.models import SourceBrief
from blueprint.source_account_recovery_requirements import (
    build_source_account_recovery_requirements,
    derive_source_account_recovery_requirements,
    extract_source_account_recovery_requirements,
    generate_source_account_recovery_requirements,
    source_account_recovery_requirements_to_dict,
    source_account_recovery_requirements_to_dicts,
    source_account_recovery_requirements_to_markdown,
    summarize_source_account_recovery_requirements,
)


def test_extracts_all_account_recovery_categories():
    result = build_source_account_recovery_requirements(_source([
        "Account recovery identity proofing must use verified device and document proof.",
        "Account recovery channel must support email and SMS recovery.",
        "Account recovery reset token lifecycle must expire one-time links after 15 minutes.",
        "Account recovery MFA recovery must use backup codes for lost authenticator devices.",
        "Account recovery abuse rate limiting must throttle attempts with captcha and lockout.",
        "Account recovery notification must send security notification email messages.",
        "Account recovery audit logging must record timestamp, actor, device, and IP address.",
        "Account recovery support escalation must open a support ticket for manual review.",
    ]))

    assert [record.requirement_type for record in result.records] == ["identity_proofing", "recovery_channel", "reset_token_lifecycle", "mfa_recovery", "abuse_rate_limiting", "notification", "audit_logging", "support_escalation"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_proofing_token_lifecycle_and_abuse_controls():
    result = derive_source_account_recovery_requirements("Account recovery identity proofing is required. Account recovery reset token lifecycle is required. Account recovery abuse controls are required.")

    assert result.summary["missing_detail_flags"] == ["missing_proofing", "missing_token_lifecycle", "missing_abuse_controls"]


def test_model_mapping_string_blank_and_malformed_inputs_are_stable():
    model = SourceBrief.model_validate(_source(["Account recovery notification must send email messages."], "acct-model"))
    payload = source_account_recovery_requirements_to_dict(extract_source_account_recovery_requirements(model))

    assert generate_source_account_recovery_requirements(_source(["Account recovery support escalation must create a support ticket."])).summary["requirement_count"] == 1
    assert summarize_source_account_recovery_requirements("Account recovery audit logging must record audit events.")["requirement_count"] == 1
    assert build_source_account_recovery_requirements("").records == ()
    assert build_source_account_recovery_requirements(3.14).records == ()
    assert build_source_account_recovery_requirements("No account recovery changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "acct-model"
    assert source_account_recovery_requirements_to_dicts(extract_source_account_recovery_requirements(model)) == payload["records"]
    assert "Source Account Recovery Requirements Report" in source_account_recovery_requirements_to_markdown(extract_source_account_recovery_requirements(model))


def _source(lines, source_id="acct-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Account recovery", "summary": "Account recovery planning", "source_payload": {"requirements": lines}, "source_links": {}}
