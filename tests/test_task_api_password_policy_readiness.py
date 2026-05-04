import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_password_policy_readiness import (
    TaskApiPasswordPolicyReadinessFinding,
    TaskApiPasswordPolicyReadinessPlan,
    plan_task_api_password_policy_readiness,
    task_api_password_policy_readiness_to_dict,
)


def test_strong_password_policy_task_has_all_safeguards():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-password-policy",
                    title="Implement comprehensive password policy for user accounts",
                    prompt=(
                        "Implement password complexity rules requiring uppercase, lowercase, digits, and special characters. "
                        "Enforce minimum length of 12 characters with validation checks. "
                        "Track password history to prevent reuse of last 10 passwords with secure storage. "
                        "Automate password expiration every 90 days with notification scheduler and cron jobs. "
                        "Implement secure password reset workflow with time-limited tokens and email verification. "
                        "Integrate HaveIBeenPwned API for breach password detection and pwned password checks. "
                        "Build password strength meter with entropy calculation and real-time visual feedback. "
                        "Use bcrypt hashing with work factor 12 and proper salting configuration. "
                        "Add comprehensive tests for all password policy rules and edge cases."
                    ),
                    outputs=["src/auth/password_policy.py", "src/auth/password_hash.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiPasswordPolicyReadinessPlan)
    assert result.plan_id == "plan-password-policy"
    assert result.password_policy_task_ids == ("task-password-policy",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiPasswordPolicyReadinessFinding)
    assert finding.detected_signals == (
        "password_complexity_rules",
        "password_length_requirements",
        "password_history_tracking",
        "password_expiration_policies",
        "password_reset_workflows",
        "breach_password_detection",
        "password_strength_meter",
        "password_hashing_algorithms",
    )
    assert finding.present_safeguards == (
        "complexity_rule_validation",
        "length_requirement_enforcement",
        "history_tracking_implementation",
        "expiration_automation_logic",
        "reset_workflow_security",
        "breach_database_integration",
        "strength_meter_accuracy",
        "hashing_algorithm_strength",
    )
    assert finding.missing_safeguards == ()
    assert finding.actionable_remediations == ()
    assert finding.readiness == "strong"
    assert any("Implement password complexity rules" in ev for ev in finding.evidence)
    assert result.summary["password_policy_task_count"] == 1
    assert result.summary["overall_readiness"] == "strong"


def test_partial_password_policy_task_reports_missing_safeguards():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-basic-policy",
                    title="Add password complexity validation",
                    prompt=(
                        "Implement password complexity rules requiring mixed case and digits. "
                        "Enforce minimum password length of 8 characters. "
                        "Add tests for password validation."
                    ),
                    outputs=["src/auth/password_validator.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-basic-policy"
    assert finding.detected_signals == ("password_complexity_rules", "password_length_requirements")
    assert "complexity_rule_validation" in finding.present_safeguards
    assert "length_requirement_enforcement" in finding.present_safeguards
    assert "history_tracking_implementation" in finding.missing_safeguards
    assert "expiration_automation_logic" in finding.missing_safeguards
    assert "breach_database_integration" in finding.missing_safeguards
    assert "hashing_algorithm_strength" in finding.missing_safeguards
    assert finding.readiness == "weak"
    assert len(finding.actionable_remediations) == 3
    assert any("history" in remediation.lower() for remediation in finding.actionable_remediations)


def test_path_hints_contribute_to_password_policy_detection():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire password policy validators",
                    prompt="Add middleware for password validation checks with bcrypt work factor configuration and salting.",
                    outputs=[
                        "src/auth/password_complexity.py",
                        "src/auth/password_length.py",
                        "src/security/bcrypt_hasher.py",
                        "src/auth/password_history.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"password_complexity_rules", "password_length_requirements", "password_history_tracking", "password_hashing_algorithms"} <= set(finding.detected_signals)
    assert "complexity_rule_validation" in finding.present_safeguards
    assert "hashing_algorithm_strength" in finding.present_safeguards


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update API docs",
                    prompt="Improve API endpoint documentation.",
                    outputs=["src/api/docs.py"],
                ),
                _task(
                    "task-no-policy",
                    title="Public API endpoint",
                    prompt="This endpoint has no password policy requirements and no password rules are involved.",
                    outputs=["src/api/public_endpoint.py"],
                ),
            ]
        )
    )

    assert result.password_policy_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-policy")
    assert result.findings == ()
    assert result.summary["password_policy_task_count"] == 0
    assert result.summary["not_applicable_task_count"] == 2


def test_reset_workflow_and_breach_detection_signals():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-reset-breach",
                    title="Implement password reset and breach detection",
                    prompt=(
                        "Implement password reset workflow with secure token generation and email verification. "
                        "Integrate HaveIBeenPwned API to check for compromised passwords and prevent pwned password usage."
                    ),
                    outputs=["src/auth/password_reset.py", "src/auth/breach_check.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "password_reset_workflows" in finding.detected_signals
    assert "breach_password_detection" in finding.detected_signals
    assert "reset_workflow_security" in finding.present_safeguards
    assert "breach_database_integration" in finding.present_safeguards


def test_strength_meter_and_hashing_signals():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-strength-hash",
                    title="Add password strength meter and secure hashing",
                    prompt=(
                        "Implement password strength meter with entropy calculation and visual feedback. "
                        "Use argon2 hashing algorithm with proper work factor and salt configuration for password storage."
                    ),
                    outputs=["src/auth/strength_meter.py", "src/auth/argon2_hasher.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "password_strength_meter" in finding.detected_signals
    assert "password_hashing_algorithms" in finding.detected_signals
    assert "strength_meter_accuracy" in finding.present_safeguards
    assert "hashing_algorithm_strength" in finding.present_safeguards


def test_serialization_and_compatibility_views():
    plan = _plan(
        [
            _task(
                "task-policy-001",
                title="Implement password policy",
                prompt="Add password complexity validation and bcrypt hashing.",
                outputs=["src/auth/policy.py"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = plan_task_api_password_policy_readiness(plan)
    serialized = task_api_password_policy_readiness_to_dict(result)

    assert plan == original
    assert result.records == result.findings
    assert serialized["password_policy_task_ids"] == ["task-policy-001"]
    assert "findings" in serialized
    assert "summary" in serialized
    assert list(serialized) == ["plan_id", "findings", "password_policy_task_ids", "not_applicable_task_ids", "summary", "records"]
    assert json.loads(json.dumps(serialized)) == serialized
    finding = result.findings[0]
    assert finding.actionable_gaps == finding.actionable_remediations


def test_multiple_tasks_with_different_readiness_levels():
    result = plan_task_api_password_policy_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Comprehensive password policy",
                    prompt=(
                        "Implement password complexity validation, length enforcement, history tracking, "
                        "expiration automation, reset workflow security, breach database integration, "
                        "strength meter accuracy, and bcrypt hashing with proper salting."
                    ),
                    outputs=["src/auth/full_policy.py"],
                ),
                _task(
                    "task-partial",
                    title="Basic password validation",
                    prompt="Add password complexity validation and minimum length check.",
                    outputs=["src/auth/basic_validator.py"],
                ),
                _task(
                    "task-weak",
                    title="Password field",
                    prompt="Add password length requirement.",
                    outputs=["src/auth/length.py"],
                ),
            ]
        )
    )

    assert len(result.findings) == 3
    assert result.password_policy_task_ids == ("task-strong", "task-partial", "task-weak")
    assert result.summary["readiness_distribution"]["strong"] == 1
    assert result.summary["readiness_distribution"]["weak"] == 2
    assert result.summary["overall_readiness"] in {"weak", "partial"}


def test_object_and_dict_inputs_are_handled():
    plan_dict = {
        "id": "plan-dict",
        "tasks": [
            {
                "task_id": "task-001",
                "title": "Password policy",
                "prompt": "Implement password complexity validation and bcrypt hashing.",
            }
        ],
    }
    plan_obj = SimpleNamespace(
        id="plan-obj",
        tasks=[
            SimpleNamespace(
                task_id="task-002",
                title="Password validation",
                prompt="Add password length requirements and strength meter.",
            )
        ],
    )

    result_dict = plan_task_api_password_policy_readiness(plan_dict)
    result_obj = plan_task_api_password_policy_readiness(plan_obj)

    assert result_dict.plan_id == "plan-dict"
    assert result_dict.password_policy_task_ids == ("task-001",)
    assert result_obj.plan_id == "plan-obj"
    assert result_obj.password_policy_task_ids == ("task-002",)


def test_empty_plan_produces_empty_findings():
    result = plan_task_api_password_policy_readiness(_plan([]))

    assert result.password_policy_task_ids == ()
    assert result.not_applicable_task_ids == ()
    assert result.findings == ()
    assert result.summary["password_policy_task_count"] == 0
    assert result.summary["overall_readiness"] == "weak"


def _plan(tasks):
    return {
        "id": "plan-password-policy",
        "tasks": tasks,
    }


def _task(task_id, *, title="", prompt="", outputs=None, scope=None):
    return {
        "task_id": task_id,
        "title": title,
        "prompt": prompt,
        "outputs": outputs or [],
        "scope": scope or [],
    }
