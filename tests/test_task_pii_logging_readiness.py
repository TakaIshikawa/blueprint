import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_pii_logging_readiness import (
    TaskPIILoggingReadinessFinding,
    TaskPIILoggingReadinessPlan,
    analyze_task_pii_logging_readiness,
    build_task_pii_logging_readiness_plan,
    extract_task_pii_logging_readiness,
    generate_task_pii_logging_readiness,
    recommend_task_pii_logging_readiness,
    summarize_task_pii_logging_readiness,
    task_pii_logging_readiness_plan_to_dict,
    task_pii_logging_readiness_plan_to_dicts,
    task_pii_logging_readiness_plan_to_markdown,
)


def test_complete_pii_logging_task_has_strong_readiness_and_no_remediations():
    result = analyze_task_pii_logging_readiness(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Add PII-safe request logging",
                    description=(
                        "Log request and response payload metadata that may include PII and email fields. "
                        "Mask and redact sensitive values before logging. "
                        "Use 30 days log retention with purge. "
                        "Apply sampling at 5 percent for payload logs. "
                        "Restrict log access with RBAC and least privilege. "
                        "Alert on PII leak detection. "
                        "Add redaction tests and masking test coverage."
                    ),
                    files_or_modules=["src/logging/pii_redaction_middleware.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskPIILoggingReadinessPlan)
    assert result.plan_id == "plan-pii-logging"
    assert result.pii_logging_task_ids == ("task-complete",)
    finding = result.findings[0]
    assert isinstance(finding, TaskPIILoggingReadinessFinding)
    assert finding.detected_signals == ("pii_logging", "sensitive_field_logging", "request_response_logging")
    assert finding.present_safeguards == (
        "masking",
        "log_retention",
        "sampling",
        "access_controls",
        "alerting",
        "test_coverage",
    )
    assert finding.missing_safeguards == ()
    assert finding.readiness == "strong"
    assert finding.actionable_remediations == ()
    assert any("description:" in ev for ev in finding.evidence)


def test_detects_pii_logging_tasks_and_stable_missing_safeguards():
    result = build_task_pii_logging_readiness_plan(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Log customer PII",
                    description="Add audit log entries containing customer email and phone values.",
                    files_or_modules=["src/audit/customer_activity_log.py"],
                ),
                _task(
                    "task-partial",
                    title="Analytics event logging",
                    description=(
                        "Send product analytics event logging for user data with redaction, "
                        "restricted log access, and monitoring alerts."
                    ),
                    files_or_modules=["src/analytics/events.py"],
                ),
                _task(
                    "task-request",
                    title="Trace request payloads",
                    description="Trace request body and response payload logging with masking tests.",
                    files_or_modules=["src/http/request_logger.py"],
                ),
            ]
        )
    )

    assert result.pii_logging_task_ids == ("task-weak", "task-partial", "task-request")
    by_id = {finding.task_id: finding for finding in result.findings}
    assert by_id["task-weak"].detected_signals == (
        "pii_logging",
        "sensitive_field_logging",
        "audit_event_logging",
    )
    assert by_id["task-weak"].readiness == "weak"
    assert by_id["task-weak"].missing_safeguards == (
        "masking",
        "log_retention",
        "sampling",
        "access_controls",
        "alerting",
        "test_coverage",
    )
    assert by_id["task-partial"].present_safeguards == ("masking", "access_controls", "alerting")
    assert by_id["task-partial"].readiness == "partial"
    assert by_id["task-request"].present_safeguards == ("masking", "test_coverage")
    assert result.summary["missing_safeguard_count"] == 13
    assert result.summary["readiness_counts"] == {"weak": 1, "partial": 2, "strong": 0}


def test_empty_no_impact_and_generic_logging_tasks_are_not_applicable():
    empty = build_task_pii_logging_readiness_plan(_plan([]))
    no_impact = extract_task_pii_logging_readiness(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update docs",
                    description="Clarify admin dashboard copy.",
                ),
                _task(
                    "task-no-pii",
                    title="Refactor logging setup",
                    description="No PII, personal data, payload, or sensitive data logging changes are in scope.",
                ),
                _task(
                    "task-generic",
                    title="Add retry logs",
                    description="Log retry count and deterministic job status only.",
                ),
            ]
        )
    )

    assert empty.findings == ()
    assert empty.records == ()
    assert empty.pii_logging_task_ids == ()
    assert empty.not_applicable_task_ids == ()
    assert empty.to_dicts() == []
    assert empty.summary == _empty_summary(0, [])
    assert no_impact.findings == ()
    assert no_impact.not_applicable_task_ids == ("task-docs", "task-no-pii", "task-generic")
    assert no_impact.summary == _empty_summary(3, ["task-docs", "task-no-pii", "task-generic"])
    assert "No PII logging readiness findings were inferred." in no_impact.to_markdown()


def test_model_object_aliases_and_serialization_are_stable_without_mutation():
    plan = _plan(
        [
            _task(
                "task-model",
                title="PII telemetry logging",
                description=(
                    "Log personal data in telemetry events with redaction, retention period, "
                    "sampling, access control, alerting, and tests."
                ),
                metadata={"pii_logging": "customer profile telemetry"},
            ),
            _task(
                "task-weak",
                title="Payload logger",
                description="Log request payload body for support debugging.",
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    build_result = build_task_pii_logging_readiness_plan(plan)
    summarize_result = summarize_task_pii_logging_readiness(model)
    generate_result = generate_task_pii_logging_readiness(model)
    recommend_result = recommend_task_pii_logging_readiness(model)
    dict_payload = task_pii_logging_readiness_plan_to_dict(generate_result)
    dicts_payload = task_pii_logging_readiness_plan_to_dicts(generate_result)
    markdown = task_pii_logging_readiness_plan_to_markdown(generate_result)
    task_result = build_task_pii_logging_readiness_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    object_result = build_task_pii_logging_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Audit event logging",
            description="Audit log contains user email with masking.",
            files_or_modules=["src/audit/user_email_log.py"],
        )
    )

    assert plan == original
    assert build_result.to_dict() == summarize_result.to_dict() == generate_result.to_dict()
    assert recommend_result.to_dict() == generate_result.to_dict()
    assert dict_payload["plan_id"] == "plan-pii-logging"
    assert len(dicts_payload) == 2
    assert "# Task PII Logging Readiness: plan-pii-logging" in markdown
    assert "| Task | Title | Readiness |" in markdown
    assert task_result.findings[0].task_id == "task-model"
    assert object_result.findings[0].task_id == "task-object"
    assert json.loads(json.dumps(dict_payload, sort_keys=True))["plan_id"] == "plan-pii-logging"


def test_compatibility_aliases_return_plan_instances():
    plan = _plan(
        [
            _task(
                "task-alias",
                title="Sensitive data logs",
                description="Log customer PII with masking and tests.",
            )
        ]
    )

    results = [
        build_task_pii_logging_readiness_plan(plan),
        analyze_task_pii_logging_readiness(plan),
        summarize_task_pii_logging_readiness(plan),
        extract_task_pii_logging_readiness(plan),
        generate_task_pii_logging_readiness(plan),
        recommend_task_pii_logging_readiness(plan),
    ]

    assert all(isinstance(result, TaskPIILoggingReadinessPlan) for result in results)
    assert all(result.plan_id == "plan-pii-logging" for result in results)
    assert all(len(result.findings) == 1 for result in results)


def _empty_summary(total_task_count: int, not_applicable_task_ids: list[str]) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "pii_logging_task_count": 0,
        "not_applicable_task_ids": not_applicable_task_ids,
        "pii_logging_task_ids": [],
        "missing_safeguard_count": 0,
        "readiness_counts": {"weak": 0, "partial": 0, "strong": 0},
        "signal_counts": {
            "pii_logging": 0,
            "sensitive_field_logging": 0,
            "request_response_logging": 0,
            "analytics_event_logging": 0,
            "audit_event_logging": 0,
        },
        "present_safeguard_counts": {
            "masking": 0,
            "log_retention": 0,
            "sampling": 0,
            "access_controls": 0,
            "alerting": 0,
            "test_coverage": 0,
        },
        "missing_safeguard_counts": {
            "masking": 0,
            "log_retention": 0,
            "sampling": 0,
            "access_controls": 0,
            "alerting": 0,
            "test_coverage": 0,
        },
    }


def _plan(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "plan-pii-logging",
        "implementation_brief_id": "brief-pii-logging",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id: str,
    *,
    title: str = "",
    description: str = "",
    files_or_modules: list[str] | None = None,
    acceptance_criteria: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "description": description or title,
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
