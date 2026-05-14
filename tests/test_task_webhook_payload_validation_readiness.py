import copy

from blueprint.domain.models import ExecutionPlan
from blueprint.task_webhook_payload_validation_readiness import (
    build_task_webhook_payload_validation_readiness_plan,
    task_webhook_payload_validation_readiness_plan_to_dict,
    task_webhook_payload_validation_readiness_plan_to_dicts,
    task_webhook_payload_validation_readiness_plan_to_markdown,
)


def test_complete_webhook_payload_validation_task_is_ready():
    result = build_task_webhook_payload_validation_readiness_plan(_plan([_task("ready", title="Webhook payload validation", description="Validate webhook payloads.", acceptance_criteria=[
        "Schema source of truth is JSON Schema in the schema registry.",
        "Required field checks validate payload shape and mandatory fields.",
        "Version handling supports versioned payloads and event versions.",
        "Failure response rejects invalid payloads with 422 error response.",
        "Logging and auditability record validation failures.",
        "Replay fixture coverage uses golden payload fixtures.",
        "Validation tests include contract tests and pytest webhook tests.",
    ])]))
    assert result.records[0].readiness == "ready"
    assert result.records[0].missing_criteria == ()


def test_partial_webhook_validation_task_reports_ordered_gaps():
    record = build_task_webhook_payload_validation_readiness_plan([_task("partial", title="Validate webhook payload shape", description="Reject invalid payloads.")]).records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("required_field_checks", "failure_response")
    assert record.missing_criteria == ("schema_source_of_truth", "version_handling", "logging_auditability", "replay_fixture_coverage", "validation_tests")
    assert record.recommended_follow_up_actions[0].startswith("Identify the schema source")


def test_webhook_path_hints_nested_metadata_no_mutation_and_conversion():
    source = _plan([_task("paths", title="Event contract", description="Validate event type.", files_or_modules=["webhooks/schema/order_event.json", "fixtures/webhook_payloads/v2.json"], metadata={"audit": "Validation log captures failures."}), _task("noop", title="Docs", description="No webhook payload validation changes are required.")], plan_id="plan-webhooks")
    original = copy.deepcopy(source)
    result = build_task_webhook_payload_validation_readiness_plan(ExecutionPlan.model_validate(source))
    payload = task_webhook_payload_validation_readiness_plan_to_dict(result)
    assert source == original
    assert result.impacted_task_ids == ("paths",)
    assert result.ignored_task_ids == ("noop",)
    assert result.records[0].detected_signals == ("webhook_payload_validation", "schema_contract", "versioned_payload")
    assert any("metadata.audit" in item for item in result.records[0].evidence)
    assert task_webhook_payload_validation_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_webhook_payload_validation_readiness_plan_to_markdown(result).startswith("# Task Webhook Payload Validation Readiness: plan-webhooks")


def _plan(tasks, *, plan_id="plan-webhooks"):
    return {"id": plan_id, "implementation_brief_id": "brief-webhooks", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, files_or_modules=None, metadata=None):
    task = {"id": task_id, "title": title or task_id, "description": description or "", "acceptance_criteria": acceptance_criteria or []}
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
