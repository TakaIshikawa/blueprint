import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_incident_response import (
    TaskIncidentResponsePlan,
    TaskIncidentResponseRecord,
    build_task_incident_response_plan,
    derive_task_incident_response_plan,
    generate_task_incident_response_records,
    task_incident_response_plan_to_dict,
    task_incident_response_plan_to_markdown,
)


def test_customer_impact_task_gets_critical_record_with_checklist_and_evidence():
    result = build_task_incident_response_plan(
        _plan(
            [
                _task(
                    "task-checkout",
                    title="Deploy production checkout rollout",
                    description=(
                        "Customer checkout can see degraded service and paging alerts "
                        "during the live rollout."
                    ),
                    risk_level="high",
                    test_command="poetry run pytest tests/test_checkout.py",
                    metadata={"validation_commands": ["curl -f https://example.test/health"]},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.task_id == "task-checkout"
    assert record.severity == "critical"
    assert record.detected_signals == (
        "customer-impact",
        "degraded-service",
        "alerting",
        "paging",
        "production",
    )
    assert "Prepare customer-impact status text and support handoff notes." in (
        record.responder_checklist
    )
    assert "Page the owning service team when alert acknowledgement is missed." in (
        record.escalation_notes
    )
    assert record.validation_evidence_requirements == (
        "Command output: poetry run pytest tests/test_checkout.py",
        "Command output: curl -f https://example.test/health",
        "Before and after service health, latency, and error-rate snapshot.",
        "Alert route, dashboard, and paging acknowledgement evidence.",
    )


def test_data_risk_migration_and_rollback_are_critical_with_recovery_guidance():
    result = build_task_incident_response_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Run customer data migration with rollback",
                    description="Backfill database rows and verify restore path.",
                    files_or_modules=["migrations/20260502_customer.sql"],
                    risk_level="medium",
                    validation_command="poetry run pytest tests/test_migrations.py",
                )
            ]
        )
    )

    assert result.records[0].to_dict() == {
        "task_id": "task-migration",
        "severity": "critical",
        "detected_signals": ["customer-impact", "data-risk", "migration", "rollback"],
        "responder_checklist": [
            "Name the incident commander, primary responder, and backup before execution.",
            "Capture baseline health and active alerts for affected production surfaces.",
            "Prepare customer-impact status text and support handoff notes.",
            "Verify backup, restore, or migration recovery path before starting.",
            "Set the rollback trigger, decision owner, and recovery time expectation.",
            "Record validation evidence and post-change incident timeline notes.",
        ],
        "escalation_notes": [
            "Escalate as critical severity if production health regresses during task-migration.",
            "Notify support and product owners when customer-visible behavior changes.",
            "Escalate before rerunning destructive data, migration, or recovery steps.",
        ],
        "validation_evidence_requirements": [
            "Command output: poetry run pytest tests/test_migrations.py",
            "Before and after service health, latency, and error-rate snapshot.",
            "Backup, restore, migration dry-run, or row-count reconciliation evidence.",
            "Rollback trigger verification and recovery validation evidence.",
        ],
    }


def test_external_service_queue_and_retry_task_gets_medium_operational_handoff():
    result = build_task_incident_response_plan(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Update external partner webhook worker",
                    description="Add retry backoff for queued webhook jobs and DLQ handling.",
                    files_or_modules=["src/integrations/partner_webhook.py"],
                    risk_level="medium",
                    test_command="poetry run pytest tests/test_webhook.py",
                )
            ]
        )
    )

    record = result.records[0]
    assert record.severity == "medium"
    assert record.detected_signals == ("external-service", "retry", "queue")
    assert "Confirm external service owner, status page, and contract rollback contact." in (
        record.responder_checklist
    )
    assert (
        "Retry budget, queue depth, and dead-letter queue inspection evidence."
        in record.validation_evidence_requirements
    )


def test_low_risk_documentation_task_gets_minimal_low_severity_guidance():
    result = build_task_incident_response_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update runbook documentation",
                    description="Clarify the on-call handoff runbook.",
                    files_or_modules=["docs/runbooks/on_call.md"],
                    risk_level="low",
                )
            ]
        )
    )

    assert result.records[0].to_dict() == {
        "task_id": "task-docs",
        "severity": "low",
        "detected_signals": [],
        "responder_checklist": [
            "Confirm task scope is documentation-only or low risk.",
            "Attach completion evidence or reviewer sign-off.",
        ],
        "escalation_notes": [
            "Escalate only if validation fails or scope expands beyond documentation."
        ],
        "validation_evidence_requirements": [
            "Reviewer sign-off or diff summary showing no production behavior change."
        ],
    }


def test_serialization_markdown_aliases_model_input_and_iterable_inputs_are_stable():
    plan = _plan(
        [
            _task(
                "task-alerts",
                title="Add production alerting",
                description="Add alert monitoring for production deploy health.",
                metadata={"test_command": "make smoke"},
            ),
            _task("task-docs", files_or_modules=["README.md"], risk_level="low"),
        ],
        plan_id="plan-ir",
    )
    original = copy.deepcopy(plan)

    result = build_task_incident_response_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_incident_response_plan(plan)
    payload = task_incident_response_plan_to_dict(result)
    generated = generate_task_incident_response_records(plan)
    iterable = build_task_incident_response_plan([_task("task-one"), _task("task-two")])
    empty = build_task_incident_response_plan({"id": "plan-empty", "tasks": []})

    assert plan == original
    assert isinstance(result, TaskIncidentResponsePlan)
    assert isinstance(TaskIncidentResponseRecord, type)
    assert payload == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert alias_result.to_dict() == result.to_dict()
    assert [record.to_dict() for record in generated] == payload["records"]
    assert iterable.plan_id is None
    assert [record.task_id for record in iterable.records] == ["task-one", "task-two"]
    assert list(payload) == ["plan_id", "records"]
    assert list(payload["records"][0]) == [
        "task_id",
        "severity",
        "detected_signals",
        "responder_checklist",
        "escalation_notes",
        "validation_evidence_requirements",
    ]
    assert task_incident_response_plan_to_markdown(result) == "\n".join(
        [
            "# Task Incident Response Plan: plan-ir",
            "",
            "## Task Records",
            "",
            "| Task | Severity | Signals | Checklist | Escalation | Validation Evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| task-alerts | medium | alerting, production | "
                "Name the incident commander, primary responder, and backup before execution.; "
                "Capture baseline health and active alerts for affected production surfaces.; "
                "Confirm paging route, alert thresholds, and acknowledgement owner.; "
                "Record validation evidence and post-change incident timeline notes. | "
                "Escalate as medium severity if production health regresses during task-alerts. | "
                "Command output: make smoke; Alert route, dashboard, and paging acknowledgement evidence. |"
            ),
            (
                "| task-docs | low | none | "
                "Confirm task scope is documentation-only or low risk.; "
                "Attach completion evidence or reviewer sign-off. | "
                "Escalate only if validation fails or scope expands beyond documentation. | "
                "Reviewer sign-off or diff summary showing no production behavior change. |"
            ),
        ]
    )
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Incident Response Plan: plan-empty",
            "",
            "No incident response records were derived.",
        ]
    )


def _plan(tasks, *, plan_id="plan-incident"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-incident",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Foundation"}, {"name": "Launch"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone="Foundation",
    files_or_modules=None,
    risk_level="medium",
    test_command=None,
    validation_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
    if validation_command is not None:
        task["validation_command"] = validation_command
    return task
