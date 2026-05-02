import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_password_policy_requirements import (
    SourcePasswordPolicyRequirement,
    SourcePasswordPolicyRequirementsReport,
    build_source_password_policy_requirements,
    derive_source_password_policy_requirements,
    extract_source_password_policy_requirements,
    generate_source_password_policy_requirements,
    source_password_policy_requirements_to_dict,
    source_password_policy_requirements_to_dicts,
    source_password_policy_requirements_to_markdown,
    summarize_source_password_policy_requirements,
)


def test_extracts_explicit_source_payload_password_policy_fields():
    result = build_source_password_policy_requirements(
        _source_brief(
            summary="Authentication must enforce password policy before launch.",
            source_payload={
                "security": {
                    "password_policy": {
                        "minimum_length": "Passwords must be at least 12 characters.",
                        "complexity": "Require uppercase, lowercase, number, and special character complexity.",
                        "reuse": "Prevent reuse of the last 5 passwords.",
                        "breach": "Reject breached passwords and common dictionary passwords.",
                    },
                    "account_recovery": [
                        "Password reset links must expire within 15 minutes.",
                        "Lockout is required after 5 failed login attempts.",
                    ],
                }
            },
        )
    )

    by_type = {record.policy_type: record for record in result.records}

    assert isinstance(result, SourcePasswordPolicyRequirementsReport)
    assert all(isinstance(record, SourcePasswordPolicyRequirement) for record in result.records)
    assert {
        "minimum_length",
        "complexity",
        "reuse_history",
        "breach_check",
        "reset_flow",
        "lockout",
    } <= set(by_type)
    assert by_type["minimum_length"].value == "at least 12 characters"
    assert by_type["reuse_history"].value == "last 5 passwords"
    assert by_type["lockout"].value == "after 5 failed login attempts"
    assert by_type["breach_check"].confidence == "high"
    assert by_type["reset_flow"].source_field == "source_payload.security.account_recovery[0]"
    assert by_type["minimum_length"].unresolved_questions[0].startswith("Confirm the exact")
    assert by_type["reset_flow"].suggested_plan_impacts[0].startswith("Design reset token")
    assert result.summary["policy_type_counts"]["breach_check"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_infers_summary_text_and_implementation_brief_password_rules():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            problem_statement=(
                "Enterprise admins need account security controls for credential policy."
            ),
            mvp_goal=(
                "Admins can configure an organization password policy and require MFA "
                "reauthentication before password changes."
            ),
            scope=[
                "Password rotation must force privileged users to change passwords every 90 days.",
                "Reset password flow requires step-up MFA before changing credentials.",
            ],
            acceptance_criteria=[
                "Admin-enforced tenant password policy is applied during signup and reset."
            ],
        )
    )
    summary_result = build_source_password_policy_requirements(
        "Accounts should require minimum password length of 14 characters and complexity with symbols."
    )
    implementation_result = generate_source_password_policy_requirements(implementation)

    assert {
        "minimum_length",
        "complexity",
    } <= {record.policy_type for record in summary_result.records}
    by_type = {record.policy_type: record for record in implementation_result.records}
    assert {"rotation", "mfa_adjacent", "admin_enforced_policy"} <= set(by_type)
    assert by_type["rotation"].value == "every 90 days"
    assert by_type["admin_enforced_policy"].confidence == "high"
    assert implementation_result.brief_id == "implementation-password-policy"
    assert implementation_result.title == "Password policy implementation"


def test_supports_object_inputs_deduplicates_evidence_and_does_not_mutate_mapping():
    source = _source_brief(
        summary="Passwords must be at least 12 characters.",
        source_payload={
            "requirements": [
                "Passwords must be at least 12 characters.",
                "Passwords must be at least 12 characters.",
            ],
            "security": {"password_policy": "Password history prevents reuse of previous 6 passwords."},
        },
    )
    original = copy.deepcopy(source)
    object_result = derive_source_password_policy_requirements(
        SimpleNamespace(
            id="object-policy",
            title="Object password rules",
            summary="Admin enforced password policy lets admins configure password complexity.",
            acceptance_criteria=[
                "Password reset must require reauthentication before credential change."
            ],
        )
    )
    mapping_result = build_source_password_policy_requirements(source)

    minimum = next(record for record in mapping_result.records if record.policy_type == "minimum_length")

    assert source == original
    assert len(minimum.evidence) == 1
    assert "reuse_history" in {record.policy_type for record in mapping_result.records}
    assert {"admin_enforced_policy", "complexity", "mfa_adjacent", "reset_flow"} <= {
        record.policy_type for record in object_result.records
    }


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="password-source",
        title="Password source",
        summary="Password reset must require MFA reauthentication.",
        source_payload={
            "security": {
                "password_policy": [
                    "Passwords must be at least 16 characters.",
                    "Password rotation must happen every 180 days.",
                    "Reject compromised passwords.",
                ]
            }
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_password_policy_requirements(model)
    extracted = extract_source_password_policy_requirements(model)
    summarized = summarize_source_password_policy_requirements(model)
    payload = source_password_policy_requirements_to_dict(result)
    markdown = source_password_policy_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert summarized.to_dict() == result.to_dict()
    assert source_password_policy_requirements_to_dicts(result) == payload["requirements"]
    assert source_password_policy_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "policy_type",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "unresolved_questions",
        "suggested_plan_impacts",
    ]
    assert [record["policy_type"] for record in payload["requirements"]] == [
        "minimum_length",
        "rotation",
        "breach_check",
        "reset_flow",
        "mfa_adjacent",
    ]
    assert markdown.startswith("# Source Password Policy Requirements Report: password-source")
    assert "| Policy Type | Value | Confidence | Source Field | Evidence |" in markdown
    assert "minimum_length" in markdown


def test_unrelated_malformed_and_blank_inputs_return_empty_reports():
    unrelated = build_source_password_policy_requirements(
        _source_brief(
            title="Profile copy",
            summary="Update the profile settings labels and navigation.",
            source_payload={"requirements": ["Keep existing email copy unchanged."]},
        )
    )
    malformed = build_source_password_policy_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_password_policy_requirements("")

    assert unrelated.records == ()
    assert unrelated.to_dicts() == []
    assert unrelated.summary == {
        "requirement_count": 0,
        "policy_types": [],
        "policy_type_counts": {
            "minimum_length": 0,
            "complexity": 0,
            "rotation": 0,
            "reuse_history": 0,
            "breach_check": 0,
            "reset_flow": 0,
            "lockout": 0,
            "mfa_adjacent": 0,
            "admin_enforced_policy": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_password_policy_language",
    }
    assert malformed.records == ()
    assert blank_text.records == ()
    assert "No source password policy requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_password_policy_requirements(unrelated) == unrelated.summary


def _source_brief(
    *,
    source_id="source-password-policy",
    title="Password policy requirements",
    domain="identity",
    summary="General password policy requirements.",
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


def _implementation_brief(
    *,
    brief_id="implementation-password-policy",
    title="Password policy implementation",
    problem_statement="Implement password policy.",
    mvp_goal="Ship password policy.",
    scope=None,
    acceptance_criteria=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-password-policy",
        "title": title,
        "domain": "identity",
        "target_user": "admin",
        "buyer": "security",
        "workflow_context": "Authentication settings",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "auth",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run identity policy tests.",
        "definition_of_done": [] if acceptance_criteria is None else acceptance_criteria,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
