import copy

from blueprint.domain.models import ExecutionPlan
from blueprint.task_queue_dead_letter_readiness import (
    build_task_queue_dead_letter_readiness_plan,
    task_queue_dead_letter_readiness_plan_to_dict,
    task_queue_dead_letter_readiness_plan_to_dicts,
    task_queue_dead_letter_readiness_plan_to_markdown,
)


def test_complete_queue_dead_letter_task_is_ready():
    result = build_task_queue_dead_letter_readiness_plan(_plan([_task("ready", title="Add DLQ handling", description="Implement dead letter queue handling.", acceptance_criteria=[
        "Routing conditions send messages to the DLQ after retries and retry exhaustion.",
        "Retention policy defines the failed message retention window and purge policy.",
        "Inspection tooling includes a queue dashboard and message inspection.",
        "Replay or discard workflow supports redrive and manual discard.",
        "Alerting ownership names the owner, on-call escalation, and runbook.",
        "Idempotency safeguards provide deduplication and safe replay behavior.",
        "Validation coverage includes integration tests and DLQ tests.",
    ])]))
    assert result.records[0].readiness == "ready"
    assert result.records[0].missing_criteria == ()


def test_partial_queue_dead_letter_task_reports_ordered_gaps():
    record = build_task_queue_dead_letter_readiness_plan([_task("partial", title="Dead letter queue for failed messages", description="Route failed jobs to DLQ after retries.")]).records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("routing_conditions",)
    assert record.missing_criteria == ("retention_policy", "inspection_tooling", "replay_or_discard_workflow", "alerting_ownership", "idempotency_safeguards", "validation_coverage")
    assert record.recommended_follow_up_actions[0].startswith("Specify retention policy")


def test_queue_dead_letter_path_hints_nested_metadata_no_mutation_and_conversion():
    source = _plan([_task("paths", title="Queue failure routing", description="Quarantine poison messages.", files_or_modules=["workers/dlq/replay_failed_messages.py", "queues/dead_letter/poison_handler.py"], metadata={"owner": "On-call runbook documents alerting."}), _task("noop", title="Queue docs", description="No DLQ changes are planned.")], plan_id="plan-dlq")
    original = copy.deepcopy(source)
    result = build_task_queue_dead_letter_readiness_plan(ExecutionPlan.model_validate(source))
    payload = task_queue_dead_letter_readiness_plan_to_dict(result)
    assert source == original
    assert result.impacted_task_ids == ("paths",)
    assert result.ignored_task_ids == ("noop",)
    assert result.records[0].detected_signals == ("dead_letter_queue", "poison_message_handling", "failure_routing")
    assert any("metadata.owner" in item for item in result.records[0].evidence)
    assert task_queue_dead_letter_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_queue_dead_letter_readiness_plan_to_markdown(result).startswith("# Task Queue Dead Letter Readiness: plan-dlq")


def _plan(tasks, *, plan_id="plan-dlq"):
    return {"id": plan_id, "implementation_brief_id": "brief-dlq", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, files_or_modules=None, metadata=None):
    task = {"id": task_id, "title": title or task_id, "description": description or "", "acceptance_criteria": acceptance_criteria or []}
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
