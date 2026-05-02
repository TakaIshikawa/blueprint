import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_audit_log_readiness import (
    TaskAuditLogReadinessPlan,
    TaskAuditLogReadinessRecord,
    analyze_task_audit_log_readiness,
    build_task_audit_log_readiness_plan,
    extract_task_audit_log_readiness,
    generate_task_audit_log_readiness,
    recommend_task_audit_log_readiness,
    summarize_task_audit_log_readiness,
    task_audit_log_readiness_plan_to_dict,
    task_audit_log_readiness_plan_to_markdown,
    task_audit_log_readiness_to_dicts,
)


def test_audit_tasks_produce_ranked_readiness_records_from_text_metadata_validation_and_paths():
    result = build_task_audit_log_readiness_plan(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add admin audit log",
                    description="Capture admin events in the audit trail for settings changes.",
                    acceptance_criteria=[
                        "Append-only immutable event capture records every create, update, and delete action.",
                        "Actor, resource, action, timestamp, outcome, IP address, and correlation ID are required.",
                        "Tamper-resistant signed logs use integrity validation.",
                        "Retention policy covers archival, purge, and legal hold.",
                        "Query validation and export permissions cover CSV export and redaction.",
                    ],
                ),
                _task(
                    "task-missing",
                    title="Build activity history feed",
                    description="Show user activity history for account changes.",
                    expected_file_paths=["src/audit/activity_history.py"],
                ),
                _task(
                    "task-partial",
                    title="Security trail search",
                    description="Add search for security trail events.",
                    metadata={"compliance_logging": {"surface": "regulatory logs", "retention": "90 day retention policy"}},
                    validation_plan="Exercise filtered audit-log exports with export validation.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskAuditLogReadinessPlan)
    assert result.plan_id == "plan-audit-log-readiness"
    assert result.audit_task_ids == ("task-missing", "task-partial", "task-strong")
    assert result.summary["readiness_counts"] == {"strong": 1, "partial": 1, "missing": 1}
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-missing"].readiness == "missing"
    assert {"audit_log", "activity_history"} <= set(by_id["task-missing"].matched_audit_signals)
    assert by_id["task-missing"].missing_safeguards == (
        "immutable_event_capture",
        "actor_resource_action_fields",
        "tamper_resistance",
        "retention_policy",
        "query_export_validation",
    )
    assert by_id["task-partial"].readiness == "partial"
    assert {"audit_log", "security_trail", "compliance_logging"} <= set(
        by_id["task-partial"].matched_audit_signals
    )
    assert by_id["task-strong"].readiness == "strong"
    assert by_id["task-strong"].missing_safeguards == ()
    assert any("files_or_modules" in item for item in by_id["task-missing"].evidence)
    assert any("metadata.compliance_logging" in item for item in by_id["task-partial"].evidence)
    assert any("validation_plan" in item for item in by_id["task-partial"].evidence)


def test_recommended_checks_are_deterministic_for_missing_audit_safeguards():
    result = build_task_audit_log_readiness_plan(
        _plan(
            [
                _task(
                    "task-audit",
                    title="Add compliance logging for admin actions",
                    description="Compliance logs record admin events.",
                    acceptance_criteria=[
                        "Retention policy keeps records for seven years.",
                        "Query validation covers filtered exports.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert isinstance(record, TaskAuditLogReadinessRecord)
    assert record.readiness == "partial"
    assert record.missing_safeguards == (
        "immutable_event_capture",
        "actor_resource_action_fields",
        "tamper_resistance",
    )
    assert record.recommended_checks == (
        "Verify audit events are captured append-only for all relevant create, update, delete, auth, and admin actions.",
        "Require actor, resource, action, timestamp, outcome, IP or request context, and correlation identifiers.",
        "Validate tamper resistance with restricted writes, integrity checks, signatures, or immutable storage.",
    )
    assert result.summary["missing_safeguard_counts"]["retention_policy"] == 0
    assert result.summary["missing_safeguard_counts"]["tamper_resistance"] == 1


def test_non_audit_and_malformed_inputs_return_empty_recommendation_sets():
    no_match = build_task_audit_log_readiness_plan(
        _plan([_task("task-docs", title="Update settings docs", description="Document settings only.")])
    )
    malformed = build_task_audit_log_readiness_plan({"tasks": "not a list"})

    assert no_match.records == ()
    assert no_match.recommendations == ()
    assert no_match.audit_task_ids == ()
    assert no_match.not_applicable_task_ids == ("task-docs",)
    assert no_match.summary["task_count"] == 1
    assert no_match.summary["audit_task_count"] == 0
    assert no_match.summary["readiness_counts"] == {"strong": 0, "partial": 0, "missing": 0}
    assert malformed.records == ()
    assert malformed.summary["task_count"] == 0
    assert generate_task_audit_log_readiness({"tasks": "not a list"}) == ()
    assert recommend_task_audit_log_readiness("not a plan") == ()
    assert generate_task_audit_log_readiness(None) == ()


def test_serializers_include_markdown_rows_aliases_and_no_mutation():
    source = _plan(
        [
            _task(
                "task-row | pipe",
                title="Audit log | export",
                description="Security logs include append-only immutable log entries.",
                metadata={
                    "safeguards": {
                        "actor_resource_action_fields": "actor resource action fields and request id",
                        "tamper_resistance": "tamper-evident hash chain",
                        "retention_policy": "retention policy with archive and purge",
                        "query_export_validation": "export validation with redacted exports",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = build_task_audit_log_readiness_plan(model)
    alias_result = summarize_task_audit_log_readiness(source)
    generated = generate_task_audit_log_readiness(model)
    payload = task_audit_log_readiness_plan_to_dict(result)
    markdown = task_audit_log_readiness_plan_to_markdown(result)
    empty = build_task_audit_log_readiness_plan({"id": "empty-plan", "tasks": []})

    assert source == original
    assert alias_result.to_dict() == build_task_audit_log_readiness_plan(source).to_dict()
    assert analyze_task_audit_log_readiness(source).to_dict() == alias_result.to_dict()
    assert extract_task_audit_log_readiness(source).to_dict() == alias_result.to_dict()
    assert generated == result.records
    assert result.recommendations == result.records
    assert result.to_dicts() == payload["records"]
    assert task_audit_log_readiness_to_dicts(result) == payload["records"]
    assert task_audit_log_readiness_to_dicts(generated) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "audit_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_audit_signals",
        "readiness",
        "missing_safeguards",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Audit Log Readiness: plan-audit-log-readiness")
    assert "Audit log \\| export" in markdown
    assert "Summary: 1 audit-log tasks across 1 total tasks" in markdown
    assert empty.to_markdown().endswith("No audit-log readiness recommendations were inferred.")


def _plan(tasks):
    return {
        "id": "plan-audit-log-readiness",
        "implementation_brief_id": "brief-audit-log-readiness",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    expected_file_paths=None,
    acceptance_criteria=None,
    validation_plan=None,
    risks=None,
    dependencies=None,
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if expected_file_paths is not None:
        task["expected_file_paths"] = expected_file_paths
    if validation_plan is not None:
        task["validation_plan"] = validation_plan
    if risks is not None:
        task["risks"] = risks
    if dependencies is not None:
        task["dependencies"] = dependencies
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
