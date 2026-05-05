import copy
import json
from dataclasses import dataclass

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_operational_runbook import (
    TaskOperationalRunbookPlan,
    build_task_operational_runbook_plan,
    recommend_task_operational_runbooks,
    task_operational_runbook_plan_to_dict,
    task_operational_runbook_plan_to_markdown,
)


def test_migration_deploy_and_feature_flag_task_gets_actionable_sections():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Deploy guarded account schema migration",
                    description=(
                        "Run database migration during production rollout behind a feature flag "
                        "with a tested rollback plan."
                    ),
                    files_or_modules=[
                        "db/migrations/202605010001_add_accounts.py",
                        "src/feature_flags/accounts.py",
                    ],
                    acceptance_criteria=["Rollback steps are rehearsed before deploy."],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.task_id == "task-migration"
    assert runbook.requirement_status == "runbook_required"
    assert runbook.runbook_required is True
    assert runbook.operational_signals == (
        "deploy",
        "migration",
        "feature_flag",
        "rollback",
    )
    assert "Verify backups, migration lock behavior, expected runtime, and schema compatibility." in (
        runbook.sections.pre_checks
    )
    assert "Increase flag exposure gradually and pause between increments for health checks." in (
        runbook.sections.execution_steps
    )
    assert "Monitor database load, lock waits, error rates, and application compatibility." in (
        runbook.sections.monitoring
    )
    assert "List the exact rollback steps, expected duration, and validation checks." in (
        runbook.sections.rollback
    )
    assert "Have the database owner available until migration post-checks pass." in (
        runbook.sections.escalation
    )
    assert "Verify schema version, application reads and writes, and affected data counts." in (
        runbook.sections.post_checks
    )
    assert "files_or_modules: db/migrations/202605010001_add_accounts.py" in runbook.evidence
    assert "files_or_modules: src/feature_flags/accounts.py" in runbook.evidence
    assert (
        "description: Run database migration during production rollout behind a feature flag with a tested rollback plan."
        in runbook.evidence
    )
    assert "acceptance_criteria[0]: Rollback steps are rehearsed before deploy." in runbook.evidence
    assert runbook.evidence == tuple(dict.fromkeys(runbook.evidence))
    assert result.runbook_task_ids == ("task-migration",)
    assert result.no_runbook_task_ids == ()
    assert result.summary["signal_counts"]["migration"] == 1


def test_backfill_queue_and_cron_job_sections_are_included():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Backfill queued invoice jobs from nightly cron",
                    description=(
                        "Run a resumable backfill through workers and update the scheduled job "
                        "that enqueues invoice repair batches."
                    ),
                    files_or_modules=[
                        "src/backfills/invoice_backfill.py",
                        "src/queues/invoice_worker.py",
                        "src/cron/invoice_repair.py",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.operational_signals == ("backfill", "queue", "cron_job")
    assert "Estimate affected records, batching, idempotency, and load limits before starting." in (
        runbook.sections.pre_checks
    )
    assert "Run the backfill in bounded batches with checkpoints and resumable state." in (
        runbook.sections.execution_steps
    )
    assert "Watch queue depth, processing latency, retries, dead letters, and worker errors." in (
        runbook.sections.monitoring
    )
    assert "Verify the scheduler registered the expected next run time." in (
        runbook.sections.post_checks
    )


def test_alert_incident_and_on_call_tasks_include_escalation_guidance():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-alerts",
                    title="Tune incident alerts for on-call responders",
                    description=(
                        "Update paging thresholds after a SEV-2 incident and document the "
                        "on-call escalation policy."
                    ),
                    files_or_modules=["ops/alerts/payments.yml"],
                    metadata={"runbook": {"channel": "incident commander handoff"}},
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.operational_signals == ("incident_response", "alert", "on_call")
    assert "Confirm alert thresholds, routing, mute windows, and dashboard links before rollout." in (
        runbook.sections.pre_checks
    )
    assert "Keep incident command and stakeholder channels updated during execution." in (
        runbook.sections.escalation
    )
    assert "Trigger or simulate the alert path and verify the expected notification target." in (
        runbook.sections.post_checks
    )
    assert "metadata.runbook.channel: incident commander handoff" in runbook.evidence


def test_external_service_and_rollback_runbook_is_traceable():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-vendor",
                    title="Switch external API webhook endpoint",
                    description=(
                        "Change the vendor integration endpoint with timeout monitoring and "
                        "rollback to the previous credentials if errors spike."
                    ),
                    files_or_modules=["src/integrations/vendors/webhook_client.py"],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.operational_signals == ("alert", "external_service", "rollback")
    assert "Confirm vendor status, credentials, rate limits, timeouts, and fallback behavior." in (
        runbook.sections.pre_checks
    )
    assert "Monitor vendor error rates, timeout rates, retry volume, and integration-specific alerts." in (
        runbook.sections.monitoring
    )
    assert "Document how to disable the integration or restore the previous endpoint or credentials." in (
        runbook.sections.rollback
    )
    assert "files_or_modules: src/integrations/vendors/webhook_client.py" in runbook.evidence
    assert "rollback" in runbook.rationale


def test_low_risk_task_returns_explicit_no_runbook_needed_result():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard empty state",
                    description="Clarify static copy and spacing in an admin panel.",
                    files_or_modules=["src/ui/empty_state.py"],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.requirement_status == "no_runbook_needed"
    assert runbook.runbook_required is False
    assert runbook.operational_signals == ()
    assert runbook.sections.to_dict() == {
        "pre_checks": [],
        "execution_steps": [],
        "monitoring": [],
        "rollback": [],
        "escalation": [],
        "post_checks": [],
    }
    assert runbook.rationale == "No production operations runbook needed."
    assert runbook.evidence == ()
    assert result.runbook_task_ids == ()
    assert result.no_runbook_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "runbook_task_count": 0,
        "no_runbook_task_count": 1,
        "signal_counts": {
            "deploy": 0,
            "migration": 0,
            "incident_response": 0,
            "alert": 0,
            "on_call": 0,
            "feature_flag": 0,
            "backfill": 0,
            "queue": 0,
            "cron_job": 0,
            "external_service": 0,
            "rollback": 0,
        },
    }


def test_mixed_task_shapes_and_alias_are_supported():
    dict_task = _task(
        "task-deploy",
        title="Production deploy for billing service",
        files_or_modules=["deploy/billing.yml"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Update queue worker retry policy",
            files_or_modules=["src/workers/retry_policy.py"],
        )
    )
    object_task = TaskLike(
        id="task-vendor",
        title="Refresh partner integration",
        description="Change third-party webhook retry timeouts.",
        files_or_modules=["src/integrations/partner_webhook.py"],
        acceptance_criteria=["Vendor fallback is verified."],
    )

    result = recommend_task_operational_runbooks([dict_task, model_task, object_task])

    assert isinstance(result, TaskOperationalRunbookPlan)
    assert result.plan_id is None
    assert result.runbook_task_ids == ("task-deploy", "task-single", "task-vendor")
    assert [runbook.task_id for runbook in result.runbooks] == [
        "task-deploy",
        "task-single",
        "task-vendor",
    ]
    assert _runbook(result, "task-deploy").operational_signals == ("deploy",)
    assert _runbook(result, "task-single").operational_signals == ("queue",)
    assert _runbook(result, "task-vendor").operational_signals == ("external_service",)


def test_execution_plan_input_serializes_without_mutation():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Deploy monitoring alert update",
                description="Change production alert thresholds for checkout.",
                files_or_modules=["ops/monitoring/checkout_alerts.yml"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_operational_runbook_plan(ExecutionPlan.model_validate(plan))
    payload = task_operational_runbook_plan_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["runbooks"]
    assert list(payload) == [
        "plan_id",
        "runbooks",
        "runbook_task_ids",
        "no_runbook_task_ids",
        "summary",
    ]
    assert list(payload["runbooks"][0]) == [
        "task_id",
        "title",
        "requirement_status",
        "runbook_required",
        "operational_signals",
        "sections",
        "rationale",
        "evidence",
    ]
    assert list(payload["runbooks"][0]["sections"]) == [
        "pre_checks",
        "execution_steps",
        "monitoring",
        "rollback",
        "escalation",
        "post_checks",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_markdown_and_invalid_empty_inputs_are_deterministic():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Deploy release",
                    description="Roll out production release with rollback plan.",
                    files_or_modules=["deploy/release.yml"],
                )
            ]
        )
    )
    empty = build_task_operational_runbook_plan({"id": "plan-empty", "tasks": []})
    invalid = build_task_operational_runbook_plan({"id": "plan-invalid", "tasks": "nope"})
    none_source = build_task_operational_runbook_plan(None)

    markdown = task_operational_runbook_plan_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Operational Runbook Plan: plan-runbook")
    assert "| `task-deploy` | runbook_required | deploy, rollback |" in markdown
    assert "Confirm the production change owner" in markdown
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Operational Runbook Plan: plan-empty",
            "",
            "No tasks were available for operational runbook planning.",
        ]
    )
    assert invalid.summary["task_count"] == 0
    assert invalid.runbooks == ()
    assert none_source.summary["task_count"] == 0
    assert none_source.runbooks == ()


@dataclass(frozen=True)
class TaskLike:
    id: str
    title: str
    description: str
    files_or_modules: list[str]
    acceptance_criteria: list[str]


def _runbook(result, task_id):
    return next(runbook for runbook in result.runbooks if runbook.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-runbook",
        "implementation_brief_id": "brief-runbook",
        "milestones": [],
        "tasks": tasks,
    }


def test_malformed_task_inputs_with_missing_fields():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                {"title": "Task without ID"},
                {"id": "task-minimal"},
            ]
        )
    )

    assert result.summary["task_count"] == 2
    assert len(result.runbooks) == 2


def test_boundary_conditions_empty_and_whitespace_fields():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-empty",
                    title="",
                    description="",
                    files_or_modules=[""],
                    acceptance_criteria=[""],
                ),
                _task(
                    "task-whitespace",
                    title="   ",
                    description="   \n\t  ",
                    files_or_modules=["  ", "\t\n"],
                ),
            ]
        )
    )

    assert result.summary["task_count"] == 2
    for runbook in result.runbooks:
        assert runbook.requirement_status == "no_runbook_needed"


def test_complex_multi_signal_deployment_scenario():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-complex",
                    title="Multi-phase production deployment",
                    description=(
                        "Deploy database migration, feature flag rollout, queue backfill, "
                        "cron job update, external service integration, alert configuration, "
                        "with on-call support and comprehensive rollback plan."
                    ),
                    files_or_modules=[
                        "db/migrations/202605_complex.sql",
                        "src/feature_flags/rollout.py",
                        "src/backfills/data_backfill.py",
                        "src/queues/processor.py",
                        "src/cron/scheduled_tasks.py",
                        "src/integrations/external_api.py",
                        "ops/alerts/monitoring.yml",
                    ],
                    acceptance_criteria=[
                        "Rollback procedure rehearsed.",
                        "On-call engineer assigned.",
                        "Incident response plan documented.",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert len(runbook.operational_signals) >= 5
    assert "migration" in runbook.operational_signals
    assert "deploy" in runbook.operational_signals
    assert "feature_flag" in runbook.operational_signals
    assert "backfill" in runbook.operational_signals
    assert "queue" in runbook.operational_signals
    assert "cron_job" in runbook.operational_signals
    assert "external_service" in runbook.operational_signals
    assert len(runbook.sections.pre_checks) > 0
    assert len(runbook.sections.execution_steps) > 0
    assert len(runbook.sections.monitoring) > 0
    assert len(runbook.sections.rollback) > 0
    assert len(runbook.sections.escalation) > 0
    assert len(runbook.sections.post_checks) > 0


def test_incident_response_with_multiple_alert_channels():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-incident",
                    title="Update incident response alert routing",
                    description=(
                        "Configure multi-channel alert routing for SEV-1 incidents with "
                        "on-call escalation, paging integration, and incident commander handoff."
                    ),
                    files_or_modules=["ops/alerts/incident_routing.yml"],
                    metadata={
                        "runbook": {
                            "channel": "incident-response",
                            "escalation_policy": "immediate",
                        }
                    },
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "incident_response" in runbook.operational_signals
    assert "alert" in runbook.operational_signals
    assert "on_call" in runbook.operational_signals
    assert any("escalation" in step.lower() for step in runbook.sections.escalation)


def test_gradual_rollout_with_canary_deployment():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-canary",
                    title="Canary deployment with gradual feature flag rollout",
                    description=(
                        "Deploy to 1% canary hosts, monitor metrics, then gradually increase "
                        "feature flag exposure with rollback capability at each stage."
                    ),
                    files_or_modules=[
                        "deploy/canary.yml",
                        "src/feature_flags/gradual_rollout.py",
                    ],
                    acceptance_criteria=[
                        "Canary deployment monitored for 1 hour before expansion.",
                        "Rollback tested in pre-production.",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "deploy" in runbook.operational_signals
    assert "feature_flag" in runbook.operational_signals
    assert "rollback" in runbook.operational_signals


def test_database_backfill_with_idempotency_checks():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Idempotent historical data backfill",
                    description=(
                        "Run resumable backfill of 10M records with batching, checkpointing, "
                        "and idempotency guarantees for safe retry."
                    ),
                    files_or_modules=["src/backfills/historical_data.py"],
                    acceptance_criteria=[
                        "Backfill progress checkpointed every 10k records.",
                        "Idempotency verified with duplicate run test.",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "backfill" in runbook.operational_signals
    assert any("batch" in step.lower() for step in runbook.sections.execution_steps)
    assert any("idempot" in step.lower() for step in runbook.sections.pre_checks)


def test_queue_worker_with_dead_letter_handling():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-queue-dlq",
                    title="Update queue worker with dead letter handling",
                    description=(
                        "Add retry logic and dead letter queue routing for failed messages "
                        "with monitoring on queue depth and processing latency."
                    ),
                    files_or_modules=["src/queues/worker_retry.py"],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "queue" in runbook.operational_signals
    assert any("queue depth" in mon.lower() for mon in runbook.sections.monitoring)


def test_cron_job_schedule_change_with_overlap_prevention():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-cron",
                    title="Update nightly cron schedule with overlap prevention",
                    description=(
                        "Change cron schedule from hourly to every 30 minutes with locking "
                        "to prevent overlapping runs."
                    ),
                    files_or_modules=["src/cron/nightly_jobs.py"],
                    acceptance_criteria=["Scheduler registered new run time."],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "cron_job" in runbook.operational_signals
    assert any("scheduler" in check.lower() for check in runbook.sections.post_checks)


def test_external_service_integration_with_timeout_handling():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-external",
                    title="Integrate external payment service with timeout handling",
                    description=(
                        "Add payment gateway integration with circuit breaker, timeout "
                        "configuration, and fallback to cached credentials on errors."
                    ),
                    files_or_modules=["src/integrations/payment_gateway.py"],
                    acceptance_criteria=["Timeout and circuit breaker tested."],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "external_service" in runbook.operational_signals
    assert any("timeout" in check.lower() for check in runbook.sections.pre_checks)


def test_rollback_procedure_with_data_consistency_validation():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-rollback",
                    title="Deploy with comprehensive rollback validation",
                    description=(
                        "Production deployment with tested rollback procedure including "
                        "data consistency checks and service health validation."
                    ),
                    files_or_modules=["deploy/production.yml"],
                    acceptance_criteria=[
                        "Rollback steps documented with expected duration.",
                        "Data consistency validated post-rollback.",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert "deploy" in runbook.operational_signals
    assert "rollback" in runbook.operational_signals
    assert len(runbook.sections.rollback) > 0


def test_metadata_driven_runbook_requirements():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Task with metadata runbook signals",
                    description="Standard feature deployment.",
                    files_or_modules=["src/feature.py"],
                    metadata={
                        "runbook": {
                            "notes": "Deploy with caution and monitor carefully",
                        }
                    },
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.task_id == "task-metadata"
    evidence_with_metadata = [e for e in runbook.evidence if "metadata" in e]
    assert len(evidence_with_metadata) > 0


def test_sorting_runbook_required_before_no_runbook():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update UI copy",
                    files_or_modules=["src/ui/text.py"],
                ),
                _task(
                    "task-deploy",
                    title="Production deployment",
                    files_or_modules=["deploy/prod.yml"],
                ),
                _task(
                    "task-docs",
                    title="Update documentation",
                    files_or_modules=["docs/readme.md"],
                ),
            ]
        )
    )

    runbook_required = [r.task_id for r in result.runbooks if r.runbook_required]
    no_runbook = [r.task_id for r in result.runbooks if not r.runbook_required]

    assert "task-deploy" in runbook_required
    assert "task-copy" in no_runbook
    assert "task-docs" in no_runbook


def test_special_characters_in_runbook_fields():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-special",
                    title="Deploy with <special> & \"quoted\" characters",
                    description=(
                        "Production deploy with unicode: \u00e9\u00f1\u00fc and symbols: #$%^&*"
                    ),
                    files_or_modules=["deploy/prod-\u00e9.yml"],
                )
            ]
        )
    )

    runbook = result.runbooks[0]
    markdown = task_operational_runbook_plan_to_markdown(result)

    assert runbook.task_id == "task-special"
    assert "task-special" in markdown


def test_very_long_descriptions_and_file_lists():
    long_description = " ".join(
        [
            "Deploy migration backfill queue cron external service alert rollback "
            "on-call incident response feature flag monitoring escalation"
        ]
        * 15
    )
    many_files = [f"deploy/service_{i}.yml" for i in range(50)]

    result = build_task_operational_runbook_plan(
        _plan([_task("task-long", description=long_description, files_or_modules=many_files)])
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    assert len(runbook.operational_signals) > 0


def test_serialization_preserves_all_runbook_fields():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-serialize",
                    title="Production migration",
                    description="Database migration with rollback.",
                    files_or_modules=["db/migrations/001.sql"],
                )
            ]
        )
    )

    payload = task_operational_runbook_plan_to_dict(result)
    json_payload = json.loads(json.dumps(payload))

    assert payload == json_payload
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["runbooks"]
    assert all(
        "sections" in runbook and isinstance(runbook["sections"], dict)
        for runbook in payload["runbooks"]
    )


def test_edge_case_file_paths_for_signal_detection():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Deploy with various path formats",
                    files_or_modules=[
                        "/absolute/deploy/service.yml",
                        "./relative/migration.sql",
                        "../parent/backfill.py",
                        "deploy.yml",
                        "path\\with\\backslashes\\deploy.yml",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert len(runbook.operational_signals) > 0


def test_acceptance_criteria_with_operational_keywords():
    result = build_task_operational_runbook_plan(
        _plan(
            [
                _task(
                    "task-ac",
                    title="Feature deployment",
                    description="Deploy new feature to production.",
                    files_or_modules=["src/feature.py"],
                    acceptance_criteria=[
                        "Pre-deployment checks completed.",
                        "Rollback procedure documented and tested.",
                        "Monitoring alerts configured for error rates.",
                        "On-call engineer assigned and briefed.",
                        "Post-deployment verification steps executed.",
                    ],
                )
            ]
        )
    )

    runbook = result.runbooks[0]

    assert runbook.runbook_required is True
    ac_evidence = [e for e in runbook.evidence if "acceptance_criteria" in e]
    assert len(ac_evidence) > 0


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
