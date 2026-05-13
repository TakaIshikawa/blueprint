import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_webhook_authentication_requirements import (
    build_source_webhook_authentication_requirements,
    derive_source_webhook_authentication_requirements,
    extract_source_webhook_authentication_requirements,
    source_webhook_authentication_requirements_to_dict,
    source_webhook_authentication_requirements_to_dicts,
    source_webhook_authentication_requirements_to_markdown,
)


def test_extracts_all_webhook_authentication_categories():
    result = build_source_webhook_authentication_requirements(
        _source_brief(
            source_payload={
                "webhook_authentication": [
                    "Webhook sender identity must include provider id and tenant id.",
                    "Webhook secret provisioning must use vault lifecycle with revoke support.",
                    "Webhook signature algorithm negotiation must support HMAC SHA256 version headers.",
                    "Webhook replay prevention must validate timestamp, nonce, ttl, and clock skew.",
                    "Webhook secret rotation must use dual secret overlap window coordination.",
                    "Receiver verification must validate signature header, timestamp, and canonical payload body.",
                    "Invalid signature verification failure must reject webhook with 401 and audit log.",
                    "Webhook authentication audit logging must include sender, timestamp, event id, and failure reason.",
                ]
            }
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "sender_identity",
        "credential_provisioning",
        "signature_algorithm_negotiation",
        "timestamp_nonce_replay_prevention",
        "secret_rotation_coordination",
        "receiver_verification",
        "failure_response",
        "audit_logging",
    ]
    assert all(record.confidence == "high" for record in result.records)
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_missing_detail_guidance():
    result = derive_source_webhook_authentication_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                [
                    "Webhook authentication needs a shared secret.",
                    "Webhook receiver verification should be implemented.",
                    "Webhook verification failure must be handled.",
                ]
            )
        )
    )

    assert result.summary["missing_detail_flags"] == [
        "missing_credential_lifecycle",
        "missing_verification_failure_behavior",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["credential_provisioning"].missing_details == ("credential lifecycle",)
    assert by_type["failure_response"].missing_details == ("verification failure behavior",)


def test_model_object_text_blank_and_invalid_inputs_are_supported():
    model = SourceBrief.model_validate(
        _source_brief(summary="Webhook authentication must verify HMAC signature headers.")
    )
    obj = type("Obj", (), {"summary": "Webhook replay prevention needs timestamp nonce handling."})()

    assert extract_source_webhook_authentication_requirements(model).summary["requirement_count"] == 1
    assert build_source_webhook_authentication_requirements(obj).summary["requirement_count"] == 1
    assert build_source_webhook_authentication_requirements("Webhook audit log should record authentication events.").summary["requirement_count"] == 1
    assert build_source_webhook_authentication_requirements("").records == ()
    assert build_source_webhook_authentication_requirements(object()).records == ()


def test_serializers_aliases_and_negated_scope_are_stable():
    result = build_source_webhook_authentication_requirements(
        _source_brief(summary="No webhook authentication or replay changes are required.")
    )
    populated = build_source_webhook_authentication_requirements(
        _source_brief(source_id="webhook-auth-2", summary="Webhook authentication must reject invalid signature failures with 403.")
    )

    assert result.records == ()
    assert json.loads(json.dumps(source_webhook_authentication_requirements_to_dict(populated), sort_keys=True))["source_id"] == "webhook-auth-2"
    assert source_webhook_authentication_requirements_to_dicts(populated) == populated.to_dict()["records"]
    assert "# Source Webhook Authentication Requirements Report: webhook-auth-2" in source_webhook_authentication_requirements_to_markdown(populated)


def _source_brief(source_id="webhook-auth", summary="Webhook authentication requirements", source_payload=None):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": "Webhook authentication",
        "summary": summary,
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation_brief(scope):
    return {
        "id": "impl-webhook-auth",
        "source_brief_id": "webhook-auth",
        "title": "Webhook authentication implementation",
        "problem_statement": "Plan webhook authentication.",
        "mvp_goal": "Authenticate webhook events.",
        "scope": scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run tests.",
        "definition_of_done": ["Requirements detected."],
    }
