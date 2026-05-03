import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_authentication_readiness import (
    TaskApiAuthenticationReadinessFinding,
    TaskApiAuthenticationReadinessPlan,
    analyze_task_api_authentication_readiness,
    build_task_api_authentication_readiness_plan,
    extract_task_api_authentication_readiness,
    generate_task_api_authentication_readiness,
    recommend_task_api_authentication_readiness,
    summarize_task_api_authentication_readiness,
    task_api_authentication_readiness_plan_to_dict,
    task_api_authentication_readiness_plan_to_dicts,
)


def test_ready_api_authentication_task_has_no_recommended_checks():
    result = analyze_task_api_authentication_readiness(
        _plan(
            [
                _task(
                    "task-api-auth",
                    title="Add API authentication for developer endpoints",
                    description=(
                        "Implement API key and bearer token authentication for public API endpoints. "
                        "Create API keys with bcrypt hashing and secure storage in vault. "
                        "Support bearer tokens with JWT format, exp claims, and token expiry validation. "
                        "Implement OAuth client credentials flow with client ID and client secret verification. "
                        "Provide credential rotation and manual revocation workflows. "
                        "Return 401 Unauthorized with WWW-Authenticate header for invalid or expired credentials. "
                        "Add comprehensive tests for auth success, 401 failures, token expiry, rotation, and revocation."
                    ),
                    files_or_modules=["src/api/authentication/api_key_validator.py", "src/api/authentication/bearer_token.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiAuthenticationReadinessPlan)
    assert result.plan_id == "plan-api-auth"
    assert result.impacted_task_ids == ("task-api-auth",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiAuthenticationReadinessFinding)
    assert finding.detected_signals == (
        "api_key",
        "bearer_token",
        "oauth",
        "token_expiry",
        "credential_hashing",
        "rotation_revocation",
        "auth_failure",
    )
    assert finding.present_safeguards == (
        "api_key_or_bearer_token_mechanism",
        "oauth_client_credentials_flow",
        "token_expiry_validation",
        "credential_hashing_or_encryption",
        "rotation_or_revocation_workflow",
        "401_unauthorized_response",
        "test_coverage",
    )
    assert finding.missing_safeguards == ()
    assert finding.recommended_checks == ()
    assert finding.readiness == "ready"
    assert "files_or_modules: src/api/authentication/api_key_validator.py" in finding.evidence
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "ready": 1}


def test_partial_authentication_task_reports_specific_recommended_checks():
    result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial-auth",
                    title="Add bearer token validation",
                    description=(
                        "Implement bearer token authentication for API endpoints. "
                        "Verify bearer tokens and return 401 Unauthorized for invalid tokens. "
                        "Add tests for token validation."
                    ),
                    files_or_modules=["src/api/auth/bearer_token_validator.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-partial-auth"
    assert finding.detected_signals == ("bearer_token", "auth_failure")
    assert "api_key_or_bearer_token_mechanism" in finding.present_safeguards
    assert "401_unauthorized_response" in finding.present_safeguards
    assert "test_coverage" in finding.present_safeguards
    assert finding.missing_safeguards == (
        "oauth_client_credentials_flow",
        "token_expiry_validation",
        "credential_hashing_or_encryption",
        "rotation_or_revocation_workflow",
    )
    assert finding.readiness == "weak"
    assert len(finding.recommended_checks) == 4
    assert any("expiry" in check for check in finding.recommended_checks)
    assert any("hash" in check or "encrypt" in check for check in finding.recommended_checks)
    assert result.summary["missing_safeguard_counts"]["credential_hashing_or_encryption"] == 1
    assert result.summary["present_safeguard_counts"]["401_unauthorized_response"] == 1


def test_path_hints_contribute_to_detection():
    result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire authentication guards",
                    description="Add middleware for token validation and credential hashing with tests.",
                    files_or_modules=[
                        "src/api/authentication/api_key.py",
                        "src/auth/bearer_token.py",
                        "src/security/bcrypt_hasher.py",
                        "src/auth/token_rotation.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"api_key", "bearer_token", "credential_hashing", "rotation_revocation"} <= set(finding.detected_signals)
    assert "files_or_modules: src/api/authentication/api_key.py" in finding.evidence
    assert "files_or_modules: src/security/bcrypt_hasher.py" in finding.evidence
    assert "test_coverage" in finding.present_safeguards


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update API docs",
                    description="Improve API endpoint documentation.",
                    files_or_modules=["src/api/docs.py"],
                ),
                _task(
                    "task-no-auth",
                    title="Public API endpoint",
                    description="No API authentication, API keys, bearer tokens, or OAuth changes are required.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-auth")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "impacted_task_count": 0,
        "not_applicable_task_ids": ["task-docs", "task-no-auth"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"weak": 0, "partial": 0, "ready": 0},
        "signal_counts": {
            "api_key": 0,
            "bearer_token": 0,
            "oauth": 0,
            "token_expiry": 0,
            "credential_hashing": 0,
            "rotation_revocation": 0,
            "auth_failure": 0,
        },
        "present_safeguard_counts": {
            "api_key_or_bearer_token_mechanism": 0,
            "oauth_client_credentials_flow": 0,
            "token_expiry_validation": 0,
            "credential_hashing_or_encryption": 0,
            "rotation_or_revocation_workflow": 0,
            "401_unauthorized_response": 0,
            "test_coverage": 0,
        },
        "missing_safeguard_counts": {
            "api_key_or_bearer_token_mechanism": 0,
            "oauth_client_credentials_flow": 0,
            "token_expiry_validation": 0,
            "credential_hashing_or_encryption": 0,
            "rotation_or_revocation_workflow": 0,
            "401_unauthorized_response": 0,
            "test_coverage": 0,
        },
    }


def test_model_object_aliases_serialization_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="API key rotation",
                description="Add API key rotation and revocation with tests.",
            ),
            _task(
                "task-a",
                title="Bearer token authentication",
                description=(
                    "Implement bearer token validation with JWT, bcrypt credential hashing, "
                    "token expiry checks, 401 Unauthorized responses, rotation support, and integration tests."
                ),
                metadata={"authentication": "Bearer tokens with OAuth client credentials and expiry validation."},
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_api_authentication_readiness(model)
    payload = task_api_authentication_readiness_plan_to_dict(result)
    task_result = build_task_api_authentication_readiness_plan(ExecutionTask.model_validate(plan["tasks"][1]))
    object_result = build_task_api_authentication_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="API key hashing",
            description="Hash API keys with bcrypt and return 401 for invalid keys with tests.",
            files_or_modules=["src/api/auth/api_key_hasher.py"],
        )
    )

    assert plan == original
    assert result.impacted_task_ids == ("task-z", "task-a")
    assert result.records == result.findings
    assert task_result.findings[0].task_id == "task-a"
    assert object_result.findings[0].task_id == "task-object"
    assert extract_task_api_authentication_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_api_authentication_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_api_authentication_readiness(plan).to_dict() == result.to_dict()
    assert task_api_authentication_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_api_authentication_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "impacted_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "evidence",
        "recommended_checks",
    ]


def test_validation_command_contributes_to_test_coverage():
    result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-validated",
                    title="Add API key auth",
                    description="Implement API key authentication with bcrypt hashing and 401 errors.",
                    validation_commands=["pytest tests/api/test_api_key_authentication.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "test_coverage" in finding.present_safeguards
    assert "validation_commands: pytest tests/api/test_api_key_authentication.py" in finding.evidence


def test_weak_partial_and_ready_readiness_levels():
    weak_result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Add token support",
                    description="Add bearer token parsing.",
                )
            ]
        )
    )
    weak_finding = weak_result.findings[0]
    assert weak_finding.readiness == "weak"
    assert len(weak_finding.missing_safeguards) >= 5

    partial_result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="API key validation",
                    description=(
                        "Validate API keys from X-API-Key header with bcrypt hash comparison. "
                        "Return 401 Unauthorized for invalid keys. Add auth tests."
                    ),
                )
            ]
        )
    )
    partial_finding = partial_result.findings[0]
    assert partial_finding.readiness == "partial"
    assert 1 <= len(partial_finding.missing_safeguards) < 5

    ready_result = build_task_api_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Complete API auth",
                    description=(
                        "Implement API key and bearer token validation with JWT and OAuth client credentials. "
                        "Hash credentials with argon2, validate token expiry, support rotation and revocation. "
                        "Return 401 Unauthorized with WWW-Authenticate header. "
                        "Add comprehensive auth tests for success, failure, expiry, rotation, and revocation."
                    ),
                )
            ]
        )
    )
    ready_finding = ready_result.findings[0]
    assert ready_finding.readiness == "ready"
    assert ready_finding.missing_safeguards == ()


def test_task_list_input():
    result = build_task_api_authentication_readiness_plan(
        [
            _task(
                "task-1",
                title="API key auth",
                description="Add API key authentication with bcrypt and 401 responses.",
            ),
            _task(
                "task-2",
                title="Unrelated refactor",
                description="Refactor helper utilities.",
            ),
        ]
    )

    assert result.plan_id is None
    assert len(result.findings) == 1
    assert result.impacted_task_ids == ("task-1",)
    assert result.not_applicable_task_ids == ("task-2",)


def _plan(tasks, plan_id="plan-api-auth"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-api-auth",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_commands=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    if metadata is not None:
        task["metadata"] = metadata
    return task
