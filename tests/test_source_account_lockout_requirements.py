import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_account_lockout_requirements import (
    SourceAccountLockoutRequirement,
    SourceAccountLockoutRequirementsReport,
    build_source_account_lockout_requirements,
    derive_source_account_lockout_requirements,
    extract_source_account_lockout_requirements,
    generate_source_account_lockout_requirements,
    source_account_lockout_requirements_to_dict,
    source_account_lockout_requirements_to_dicts,
    source_account_lockout_requirements_to_markdown,
    summarize_source_account_lockout_requirements,
)


def test_extracts_lockout_requirements_from_markdown_in_stable_order():
    result = build_source_account_lockout_requirements(
        _source_brief(
            source_payload={
                "body": """
# Account lockout requirements

- Customers must be locked out after 5 failed login attempts for 15 minutes.
- CAPTCHA should challenge users after 3 failed attempts before another password retry.
- Locked users must use a self-service unlock email link.
- Support agents can perform admin unlock with an audit log entry.
- Send security notification email when an account is locked.
- Authentication events must be exported as audit evidence for lockout changes.
"""
            }
        )
    )

    assert isinstance(result, SourceAccountLockoutRequirementsReport)
    assert all(isinstance(record, SourceAccountLockoutRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "failed_attempt_threshold",
        "temporary_lockout",
        "captcha_or_step_up",
        "unlock_flow",
        "admin_unlock",
        "notification",
        "audit_evidence",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["failed_attempt_threshold"].value == "after 5 failed login, after 3 failed attempts"
    assert by_type["failed_attempt_threshold"].subject == "Customers"
    assert by_type["temporary_lockout"].value == "15 minutes"
    assert by_type["captcha_or_step_up"].value == "CAPTCHA"
    assert by_type["unlock_flow"].value == "self-service unlock"
    assert by_type["admin_unlock"].subject == "Support agents"
    assert by_type["audit_evidence"].confidence == "high"
    assert result.summary["requirement_count"] == 7
    assert result.summary["requirement_type_counts"]["notification"] == 1
    assert result.summary["status"] == "ready_for_account_lockout_planning"


def test_structured_payload_and_implementation_brief_are_supported():
    source = _source_brief(
        source_payload={
            "account_lockout": [
                {
                    "threshold": "After 8 failed sign-ins",
                    "duration": "30 minutes",
                    "subject": "privileged users",
                    "notification": "email and webhook alerts",
                    "evidence": "security event export",
                },
                {
                    "admin_unlock": "Support agents can unlock locked users after identity verification.",
                },
            ],
        }
    )
    model_result = build_source_account_lockout_requirements(SourceBrief.model_validate(source))
    implementation_result = build_source_account_lockout_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Login throttling must show CAPTCHA after 4 failed attempts.",
                    "Self-service unlock flow must send an email link to affected users.",
                ],
                definition_of_done=[
                    "Lockout audit evidence must include authentication event retention.",
                ],
            )
        )
    )

    assert [record.requirement_type for record in model_result.records] == [
        "failed_attempt_threshold",
        "temporary_lockout",
        "admin_unlock",
        "notification",
        "audit_evidence",
    ]
    threshold = next(record for record in model_result.records if record.requirement_type == "failed_attempt_threshold")
    assert threshold.source_field == "source_payload.account_lockout[0]"
    assert threshold.subject == "privileged users"
    assert threshold.confidence == "high"
    assert [record.requirement_type for record in implementation_result.records] == [
        "failed_attempt_threshold",
        "captcha_or_step_up",
        "unlock_flow",
        "notification",
        "audit_evidence",
    ]
    by_type = {record.requirement_type: record for record in implementation_result.records}
    assert by_type["captcha_or_step_up"].value == "CAPTCHA"
    assert by_type["unlock_flow"].subject == "affected users"


def test_negated_out_of_scope_malformed_and_duplicate_inputs_are_stable():
    empty = build_source_account_lockout_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={
                "body": "No account lockout, failed login throttling, CAPTCHA, or unlock changes are required."
            },
        )
    )
    repeat = build_source_account_lockout_requirements(
        _source_brief(
            summary="Authentication copy update.",
            source_payload={
                "body": "No account lockout, failed login throttling, CAPTCHA, or unlock changes are required."
            },
        )
    )
    object_empty = build_source_account_lockout_requirements(
        SimpleNamespace(id="object-empty", summary="No lockout or failed login changes are needed.")
    )
    malformed = build_source_account_lockout_requirements({"source_payload": {"notes": object()}})
    duplicate = build_source_account_lockout_requirements(
        {
            "id": "dupes",
            "source_payload": {
                "requirements": [
                    "Accounts must lock out after 6 failed attempts for 20 minutes.",
                    "Accounts must lock out after 6 failed attempts for 20 minutes.",
                ],
            },
        }
    )

    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "failed_attempt_threshold": 0,
            "temporary_lockout": 0,
            "captcha_or_step_up": 0,
            "unlock_flow": 0,
            "admin_unlock": 0,
            "notification": 0,
            "audit_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_account_lockout_language",
    }
    assert "No source account lockout requirements were inferred." in empty.to_markdown()
    assert object_empty.records == ()
    assert malformed.records == ()
    assert [record.requirement_type for record in duplicate.records] == [
        "failed_attempt_threshold",
        "temporary_lockout",
    ]
    assert duplicate.records[0].evidence == (
        "source_payload.requirements[0]: Accounts must lock out after 6 failed attempts for 20 minutes.",
    )


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="lockout-model",
        source_payload={
            "requirements": [
                "Account lockout must notify customers by email for account | risk events.",
                "Admin unlock must allow support agents to override lockout with audit evidence.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_account_lockout_requirements(source)
    model_result = generate_source_account_lockout_requirements(model)
    derived = derive_source_account_lockout_requirements(model)
    extracted = extract_source_account_lockout_requirements(model)
    text_result = build_source_account_lockout_requirements("After 5 failed logins, lock the account for 10 minutes.")
    object_result = build_source_account_lockout_requirements(
        SimpleNamespace(id="object-lockout", metadata={"lockout": "CAPTCHA must challenge risky failed login retries."})
    )
    payload = source_account_lockout_requirements_to_dict(model_result)
    markdown = source_account_lockout_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_account_lockout_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_account_lockout_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_account_lockout_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_account_lockout_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "value",
        "subject",
        "evidence",
        "source_field",
        "confidence",
        "missing_detail_guidance",
    ]
    assert [record.requirement_type for record in model_result.records] == [
        "admin_unlock",
        "notification",
        "audit_evidence",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Requirement Type | Value | Subject | Source Field | Confidence | Missing Detail Guidance | Evidence |" in markdown
    assert "account \\| risk events" in markdown
    assert "Confirm which admin roles can override lockout" in markdown
    assert text_result.records[0].requirement_type == "failed_attempt_threshold"
    assert object_result.records[0].requirement_type == "captcha_or_step_up"


def _source_brief(
    *,
    source_id="sb-lockout",
    title="Account lockout requirements",
    domain="authentication",
    summary="General account lockout requirements.",
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
        "id": "impl-lockout",
        "source_brief_id": "source-lockout",
        "title": "Account lockout rollout",
        "domain": "authentication",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need account lockout requirements before task generation.",
        "problem_statement": "Lockout requirements need to be extracted early.",
        "mvp_goal": "Plan account lockout work from source briefs.",
        "product_surface": "authentication",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review authentication lockout flows.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
