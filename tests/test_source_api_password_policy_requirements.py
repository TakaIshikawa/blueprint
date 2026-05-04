import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_password_policy_requirements import (
    SourceAPIPasswordPolicyRequirement,
    SourceAPIPasswordPolicyRequirementsReport,
    build_source_api_password_policy_requirements,
    derive_source_api_password_policy_requirements,
    extract_source_api_password_policy_requirements,
    generate_source_api_password_policy_requirements,
    source_api_password_policy_requirements_to_dict,
    source_api_password_policy_requirements_to_dicts,
    source_api_password_policy_requirements_to_markdown,
    summarize_source_api_password_policy_requirements,
)


def test_nested_source_payload_extracts_password_policy_categories_in_order():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            source_payload={
                "password_policy": {
                    "complexity": "Password complexity rules must require uppercase, lowercase, digits, and special characters.",
                    "length": "Password length requirements must enforce minimum 12 characters.",
                    "history": "Password history tracking must prevent reuse of last 5 passwords.",
                    "expiration": "Password expiration policies must enforce rotation every 90 days.",
                    "reset": "Password reset workflows must send secure token via email with 1-hour expiration.",
                    "breach": "Breach password detection must check against HaveIBeenPwned database.",
                    "strength": "Password strength meter must provide real-time feedback on password quality.",
                    "hashing": "Password hashing algorithms must use bcrypt with work factor 12.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPIPasswordPolicyRequirementsReport)
    assert all(isinstance(record, SourceAPIPasswordPolicyRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "password_complexity_rules",
        "password_length_requirements",
        "password_history_tracking",
        "password_expiration_policies",
        "password_reset_workflows",
        "breach_password_detection",
        "password_strength_meter",
        "password_hashing_algorithms",
    ]
    assert by_category["password_complexity_rules"].value in {"complexity", "mixed case", "alphanumeric", "character type"}
    assert by_category["password_length_requirements"].value in {"length", "minimum length", "12 character"}
    assert by_category["password_hashing_algorithms"].value in {"bcrypt", "hash", "salted hash"}
    assert by_category["password_complexity_rules"].source_field == "source_payload.password_policy.complexity"
    assert by_category["password_complexity_rules"].suggested_owners == ("security", "backend", "api_platform")
    assert by_category["password_complexity_rules"].planning_notes[0].startswith("Define complexity rules")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must enforce password complexity with uppercase, lowercase, and digits.",
            "Password length must be minimum 12 characters.",
        ],
        definition_of_done=[
            "Password history tracking prevents reuse of last 5 passwords.",
            "Breach password detection checks against known breach databases.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Password reset workflows must send secure token via email.",
            "Password strength meter must provide real-time feedback.",
        ],
        api={"security": "Password hashing algorithms must use bcrypt or argon2."},
        source_payload={"metadata": {"password": "Password expiration policies must enforce 90-day rotation."}},
    )

    source_result = build_source_api_password_policy_requirements(source)
    implementation_result = generate_source_api_password_policy_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "password_reset_workflows" in source_categories
    assert "password_strength_meter" in source_categories
    # At least one of these two fields should be the source for one of the records
    source_fields = {r.source_field for r in source_result.records}
    assert any(field.startswith("requirements") or field.startswith("api.") for field in source_fields)
    assert {
        "password_complexity_rules",
        "password_length_requirements",
        "password_history_tracking",
        "breach_password_detection",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-password-policy"
    assert implementation_result.title == "Password policy implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_password_policy():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            summary="API needs password policy support for credential security.",
            source_payload={
                "requirements": [
                    "API must have password complexity rules for user accounts.",
                    "Passwords should be of sufficient length for security.",
                    "Password history should track recent passwords.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "password_complexity_rules" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_policy_enforcement",
        "missing_hashing_algorithm",
    ]
    assert "Specify password policy enforcement mechanisms (validation points, rejection handling, compliance checks)." in result.summary["gap_messages"]
    assert "Define password hashing algorithm (bcrypt, argon2, scrypt, PBKDF2) and salting strategy." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_policy_enforcement"] >= 1
    assert result.summary["status"] == "needs_password_policy_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="password-policy-model",
        title="Password policy source",
        summary="Password policy source.",
        source_payload={
            "password_policy": {
                "complexity": "Password complexity rules must enforce mixed case and special characters.",
                "same_complexity": "Password complexity rules must enforce mixed case and special characters.",
                "hashing": "Password hashing algorithms must use bcrypt with salting.",
            },
            "acceptance_criteria": [
                "Password complexity rules must enforce mixed case and special characters.",
                "Password history tracking must prevent reuse of last 5 passwords.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"requirements", "api"}
        }
    )

    result = build_source_api_password_policy_requirements(source)
    extracted = extract_source_api_password_policy_requirements(model)
    derived = derive_source_api_password_policy_requirements(model)
    payload = source_api_password_policy_requirements_to_dict(result)
    markdown = source_api_password_policy_requirements_to_markdown(result)
    complexity = next(record for record in result.records if record.category == "password_complexity_rules")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_password_policy_requirements(result) == result.summary
    assert source_api_password_policy_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_password_policy_requirements_to_dicts(result.records) == payload["records"]
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
        "planning_notes",
        "gap_messages",
    ]
    # Evidence should be deduplicated and sorted
    assert len(complexity.evidence) == 1
    assert "Password complexity rules must enforce mixed case and special characters" in complexity.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API Password Policy Requirements Report: password-policy-model")
    assert "password" in markdown.casefold() or "complexity" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-password-policy"
        summary = "No password policy or password requirements are needed for this release."

    object_result = build_source_api_password_policy_requirements(
        SimpleNamespace(
            id="object-password-policy",
            summary="API must enforce password complexity rules with bcrypt hashing.",
            password_policy={"complexity": "Password complexity rules must require uppercase and digits."},
        )
    )
    negated = build_source_api_password_policy_requirements(BriefLike())
    no_scope = build_source_api_password_policy_requirements(
        _source_brief(summary="Password policy is out of scope and no password requirements are planned.")
    )
    unrelated = build_source_api_password_policy_requirements(
        _source_brief(
            title="Password field UI",
            summary="Password input field and password form should be reviewed.",
            source_payload={"requirements": ["Update password textbox styling and password database schema."]},
        )
    )
    malformed = build_source_api_password_policy_requirements({"source_payload": {"password_policy": {"notes": object()}}})
    blank = build_source_api_password_policy_requirements("")
    invalid = build_source_api_password_policy_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "password_complexity_rules": 0,
            "password_length_requirements": 0,
            "password_history_tracking": 0,
            "password_expiration_policies": 0,
            "password_reset_workflows": 0,
            "breach_password_detection": 0,
            "password_strength_meter": 0,
            "password_hashing_algorithms": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_policy_enforcement": 0,
            "missing_hashing_algorithm": 0,
        },
        "gap_messages": [],
        "status": "no_password_policy_language",
    }
    assert "password_complexity_rules" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API password policy requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_password_policy_requirements(unrelated) == expected_summary


def test_password_reset_workflows_and_breach_detection():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            summary="API must support password reset workflows with breach detection.",
            requirements=[
                "Password reset must send secure token via email with 1-hour expiration.",
                "Password recovery must support account recovery through email verification.",
                "Breach password detection must check against HaveIBeenPwned database.",
                "Compromised password check must reject known leaked passwords.",
            ],
            source_payload={
                "password_policy": {
                    "reset": "Password reset workflows must include token generation and validation.",
                    "strength": "Password strength meter must provide visual feedback on password quality.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "password_reset_workflows" in by_category
    assert "breach_password_detection" in by_category
    assert "password_strength_meter" in by_category
    assert result.summary["requirement_count"] >= 3
    assert result.summary["status"] in {"ready_for_planning", "needs_password_policy_details"}


def test_password_history_and_expiration_policies():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            requirements=[
                "Password history tracking must prevent reuse of last 10 passwords.",
                "Password reuse prevention must block historical password matches.",
                "Password expiration policies must enforce rotation every 90 days.",
                "Password age must trigger forced change notifications 7 days before expiry.",
                "Password rotation must be configurable per tenant.",
            ],
            source_payload={
                "password_policy": {
                    "history": "Password change history must be stored securely with bcrypt hashing.",
                    "expiration": "Password lifetime must default to 90 days with configurable extension.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "password_history_tracking" in by_category
    assert "password_expiration_policies" in by_category
    assert result.summary["requirement_count"] >= 2
    assert result.summary["status"] in {"ready_for_planning", "needs_password_policy_details"}


def test_complexity_and_length_requirements():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            requirements=[
                "Password complexity rules must require uppercase, lowercase, digits, and special characters.",
                "Character requirements must validate mixed case alphanumeric passwords.",
                "Password length requirements must enforce minimum 12 characters.",
                "Maximum password length must be 128 characters.",
                "Minimum length validation must reject passwords shorter than 12 characters.",
            ],
            source_payload={
                "password_policy": {
                    "complexity": "Complexity check must validate character types and reject weak passwords.",
                    "length": "Length policy must enforce min 12 and max 128 character count.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "password_complexity_rules" in by_category
    assert "password_length_requirements" in by_category
    assert by_category["password_complexity_rules"].confidence in {"high", "medium"}
    assert by_category["password_length_requirements"].confidence in {"high", "medium"}
    assert result.summary["requirement_count"] >= 2


def test_hashing_algorithms_and_strength_meter():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            requirements=[
                "Password hashing algorithms must use bcrypt with work factor 12.",
                "Cryptographic hash must use salted hash for secure storage.",
                "Password storage must use argon2id for new passwords.",
                "Password strength meter must calculate entropy and provide feedback.",
                "Strength indicator must show weak, medium, strong, very strong ratings.",
            ],
            source_payload={
                "password_policy": {
                    "hashing": "Hash algorithm must support bcrypt, argon2, and scrypt with configurable work factors.",
                    "strength": "Strength score must provide real-time feedback on password quality.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "password_hashing_algorithms" in by_category
    assert "password_strength_meter" in by_category
    assert by_category["password_hashing_algorithms"].value in {"bcrypt", "argon2", "argon2id", "scrypt", "hash", "salted hash"}
    assert result.summary["requirement_count"] >= 2
    assert result.summary["status"] in {"ready_for_planning", "needs_password_policy_details"}


def test_passkeys_support_edge_case():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            summary="API must support password policy with optional passkeys support.",
            requirements=[
                "Password complexity rules must enforce strong passwords.",
                "Passkeys support should be available as an alternative to passwords.",
                "Password hashing must use bcrypt for traditional password authentication.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "password_complexity_rules" in categories
    assert "password_hashing_algorithms" in categories
    # Passkeys are mentioned but should not create a separate category
    assert result.summary["requirement_count"] >= 2


def test_adaptive_policies_edge_case():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            summary="API must support adaptive password policies based on risk assessment.",
            requirements=[
                "Password policy must adapt based on user risk profile.",
                "Adaptive policies must enforce stronger rules for high-risk accounts.",
                "Password complexity rules must vary by account sensitivity.",
                "Password length requirements must increase for privileged accounts.",
            ],
        )
    )

    categories = [record.category for record in result.records]
    assert "password_complexity_rules" in categories
    assert "password_length_requirements" in categories
    # Adaptive policies should be captured as part of complexity and length requirements
    assert result.summary["requirement_count"] >= 2


def test_no_policy_edge_case():
    result = build_source_api_password_policy_requirements(
        _source_brief(
            summary="API user authentication without specific password policy requirements.",
            requirements=[
                "User authentication must support login with username and password.",
                "Session management must track user sessions.",
            ],
        )
    )

    # No password policy requirements should be extracted
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_password_policy_language"


def _source_brief(
    *,
    source_id="source-password-policy",
    title="Password policy source brief",
    domain="api",
    summary="Password policy source brief.",
    requirements=None,
    api=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "api": {} if api is None else api,
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
    brief_id="implementation-password-policy",
    title="Password policy implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-password-policy",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need password policy planning.",
        "problem_statement": "Password policy requirements need to be extracted early.",
        "mvp_goal": "Plan password complexity, length requirements, history tracking, expiration policies, reset workflows, breach detection, strength meter, and hashing algorithms.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run password policy extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
