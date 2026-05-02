import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.task_account_recovery_readiness import (
    TaskAccountRecoveryReadinessPlan,
    TaskAccountRecoveryReadinessRecord,
    analyze_task_account_recovery_readiness,
    build_task_account_recovery_readiness_plan,
    extract_task_account_recovery_readiness,
    generate_task_account_recovery_readiness,
    recommend_task_account_recovery_readiness,
    summarize_task_account_recovery_readiness,
    task_account_recovery_readiness_plan_to_dict,
    task_account_recovery_readiness_plan_to_dicts,
    task_account_recovery_readiness_plan_to_markdown,
)


def test_password_reset_without_controls_recommends_token_and_abuse_readiness():
    result = build_task_account_recovery_readiness_plan(
        _plan(
            [
                _task(
                    "task-reset",
                    title="Add password reset with recovery email",
                    description="Users can request a reset token from the forgot password page.",
                    files_or_modules=["src/auth/password_reset/reset_tokens.py"],
                    acceptance_criteria=["Recovery email sends a reset link."],
                )
            ]
        )
    )

    assert isinstance(result, TaskAccountRecoveryReadinessPlan)
    assert result.account_recovery_task_ids == ("task-reset",)
    record = result.records[0]
    assert isinstance(record, TaskAccountRecoveryReadinessRecord)
    assert record.recovery_scenarios == ("self_service_reset",)
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "token_expiry",
        "one_time_use",
        "enumeration_resistance",
        "rate_limiting",
        "abuse_prevention",
        "telemetry",
        "test_coverage",
    )
    assert record.risk_level == "high"
    assert any("enumeration" in check for check in record.recommended_checks)
    assert any("abuse prevention" in check.lower() for check in record.recommended_checks)
    assert "files_or_modules: src/auth/password_reset/reset_tokens.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["account_recovery_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["enumeration_resistance"] == 1
    assert result.summary["scenario_counts"]["self_service_reset"] == 1


def test_mfa_lockout_support_compromise_and_audit_scenarios_are_distinguished():
    result = analyze_task_account_recovery_readiness(
        _plan(
            [
                _task(
                    "task-recovery",
                    title="Plan MFA recovery, support unlock, and compromised account handling",
                    description=(
                        "Recover MFA when users lose authenticator access, unlock account lockout cases, "
                        "and secure compromised account reports with support-assisted recovery."
                    ),
                    files_or_modules=[
                        "src/auth/mfa/recovery_codes.py",
                        "src/support/helpdesk_unlock.py",
                        "src/security/recovery_audit_events.py",
                    ],
                    tags=["account-lockout", "account-takeover"],
                    metadata={
                        "recovery": {
                            "support_verification": "Support verification requires identity verification before unlock.",
                            "telemetry": "Audit log records security event, actor, IP address, and device fingerprint.",
                            "rate_limiting": "Rate limiting throttles recovery and unlock attempts.",
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.recovery_scenarios == (
        "mfa_recovery",
        "account_lockout",
        "support_assisted_recovery",
        "compromised_account",
        "recovery_audit_logging",
    )
    assert record.present_safeguards == (
        "rate_limiting",
        "support_verification",
        "telemetry",
    )
    assert record.missing_safeguards == (
        "token_expiry",
        "one_time_use",
        "enumeration_resistance",
        "abuse_prevention",
        "test_coverage",
    )
    assert record.risk_level == "high"
    assert any("metadata.recovery.support_verification" in item for item in record.evidence)
    assert any("tags[0]" in item for item in record.evidence)


def test_fully_covered_recovery_workflow_is_low_risk():
    result = build_task_account_recovery_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship account recovery and password reset",
                    description="Self-service reset and account unlock flows are ready.",
                    acceptance_criteria=[
                        "Reset token expiration uses a 30 minute TTL.",
                        "One-time use tokens are consumed and invalidated after successful password reset.",
                        "Enumeration resistance returns the same response and does not reveal whether an account exists.",
                        "Rate limiting throttles requests per account, IP, and device.",
                        "Abuse prevention includes bot protection and suspicious activity risk scoring.",
                        "Support verification requires step-up identity verification before manual account recovery.",
                        "Telemetry audit log records recovery attempts, token issuance, actor, IP address, and security event.",
                        "Recovery tests cover reset, lockout, support recovery, abuse tests, and token replay.",
                    ],
                    validation_commands={
                        "test": ["poetry run pytest tests/auth/test_account_recovery.py"]
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.recovery_scenarios == (
        "self_service_reset",
        "account_lockout",
        "support_assisted_recovery",
        "recovery_audit_logging",
    )
    assert record.present_safeguards == (
        "token_expiry",
        "one_time_use",
        "enumeration_resistance",
        "rate_limiting",
        "abuse_prevention",
        "support_verification",
        "telemetry",
        "test_coverage",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0
    assert any("validation_commands:" in item for item in record.evidence)


def test_unrelated_tasks_are_not_applicable_with_stable_empty_summary():
    result = build_task_account_recovery_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust settings labels and loading states.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.recommendations == ()
    assert result.account_recovery_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "account_recovery_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "token_expiry": 0,
            "one_time_use": 0,
            "enumeration_resistance": 0,
            "rate_limiting": 0,
            "abuse_prevention": 0,
            "support_verification": 0,
            "telemetry": 0,
            "test_coverage": 0,
        },
        "scenario_counts": {},
    }
    assert "No account recovery readiness records were inferred." in result.to_markdown()
    assert "Not-applicable tasks: task-copy" in result.to_markdown()


def test_serialization_markdown_aliases_sorting_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Password reset | ready",
                description=(
                    "Password reset includes token expiration, one-time use, enumeration resistance, rate limiting, "
                    "abuse prevention, telemetry, and test coverage."
                ),
            ),
            _task(
                "task-a",
                title="Add support assisted unlock",
                description="Support assisted recovery lets agents unlock locked accounts.",
            ),
            _task("task-copy", title="Copy update", description="Change helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_account_recovery_readiness(plan)
    payload = task_account_recovery_readiness_plan_to_dict(result)
    markdown = task_account_recovery_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_account_recovery_readiness_plan_to_dicts(result) == payload["records"]
    assert task_account_recovery_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_account_recovery_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_account_recovery_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_account_recovery_readiness(plan).to_dict() == result.to_dict()
    assert result.account_recovery_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "account_recovery_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "recovery_scenarios",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_checks",
    ]
    assert markdown.startswith("# Task Account Recovery Readiness: plan-recovery")
    assert "Password reset \\| ready" in markdown
    assert "| Task | Title | Risk | Scenarios | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


def test_execution_plan_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Recover MFA backup codes",
        description="MFA recovery lets users replace lost authenticator backup codes.",
        acceptance_criteria=["Recovery tests cover backup code replacement."],
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add support unlock audit",
                    description="Support assisted recovery includes support verification and audit log telemetry.",
                    metadata={"test_coverage": "Recovery tests cover support unlock."},
                )
            ],
            plan_id="plan-model",
        )
    )

    object_result = build_task_account_recovery_readiness_plan([object_task])
    model_result = build_task_account_recovery_readiness_plan(plan_model)
    invalid = build_task_account_recovery_readiness_plan(17)

    assert object_result.records[0].task_id == "task-object"
    assert object_result.records[0].recovery_scenarios == ("mfa_recovery",)
    assert "test_coverage" in object_result.records[0].present_safeguards
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"
    assert model_result.records[0].recovery_scenarios == ("support_assisted_recovery", "recovery_audit_logging")
    assert invalid.records == ()


def _plan(tasks, *, plan_id="plan-recovery"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-recovery",
        "target_engine": "codex",
        "target_repo": "blueprint",
        "project_type": "python",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "metadata": {} if metadata is None else metadata,
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
