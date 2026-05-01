import copy
import json

from blueprint.domain.models import ExecutionTask
from blueprint.task_observability_gaps import (
    TaskObservabilityGapPlan,
    TaskObservabilityGapRow,
    analyze_task_observability_gaps,
    summarize_task_observability_gaps,
    task_observability_gaps_to_dict,
)


def test_operational_task_without_observability_terms_surfaces_gaps():
    result = analyze_task_observability_gaps(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile API endpoint",
                    description="Create a backend route that returns profile responses.",
                    files_or_modules=["src/blueprint/api/profile.py"],
                    acceptance_criteria=["Request test covers the new response."],
                )
            ]
        )
    )

    assert isinstance(result, TaskObservabilityGapPlan)
    assert result.plan_id == "plan-observability-gaps"
    assert result.summary == {
        "task_count": 1,
        "operational_task_count": 1,
        "gap_count": 1,
        "covered_count": 0,
        "severity_counts": {"none": 0, "low": 0, "medium": 1, "high": 0},
    }
    row = result.rows[0]
    assert isinstance(row, TaskObservabilityGapRow)
    assert row.task_id == "task-api"
    assert row.gap_severity == "medium"
    assert row.operational_signals == ("service", "api")
    assert row.expected_telemetry == ("logs", "metrics", "traces", "slos")
    assert row.covered_telemetry == ()
    assert row.missing_coverage == ("logs", "metrics", "traces", "slos")
    assert row.evidence == (
        "description: Create a backend route that returns profile responses.",
        "files_or_modules: src/blueprint/api/profile.py",
        "title: Add profile API endpoint",
    )
    assert "Exercise the changed request or dependency boundary and confirm trace propagation." in (
        row.recommended_validation_steps
    )


def test_explicit_observability_coverage_clears_expected_gaps():
    result = analyze_task_observability_gaps(
        _task(
            "task-worker",
            title="Run invoice export worker",
            description="Add a background worker that exports invoice records to a vendor API.",
            acceptance_criteria=[
                (
                    "Structured logs, metrics, traces, dashboard, alerts, and runbook "
                    "coverage are validated for export failures with audit events and an SLO."
                )
            ],
        )
    )

    row = result.rows[0]

    assert result.plan_id is None
    assert row.gap_severity == "none"
    assert row.operational_signals == (
        "api",
        "worker",
        "integration",
        "payment",
        "import_export",
    )
    assert row.expected_telemetry == (
        "logs",
        "metrics",
        "traces",
        "dashboards",
        "alerts",
        "audit_events",
        "slos",
        "runbooks",
    )
    assert row.covered_telemetry == row.expected_telemetry
    assert row.missing_coverage == ()
    assert row.recommended_validation_steps == (
        "Verify the referenced telemetry in the same validation run as the task change.",
    )


def test_metadata_and_file_hints_detect_runtime_work():
    result = analyze_task_observability_gaps(
        [
            _task(
                "task-import",
                title="Build partner record adapter",
                description="Transform partner records into normalized accounts.",
                files_or_modules=[
                    "src/blueprint/integrations/salesforce_export.py",
                    "src/blueprint/pipelines/account_backfill.py",
                ],
                metadata={"schedule": "nightly batch data pipeline"},
            ),
            _task(
                "task-copy",
                title="Tighten admin copy",
                description="Adjust labels in the settings page.",
                files_or_modules=["docs/settings-copy.md"],
            ),
        ]
    )

    assert [row.task_id for row in result.rows] == ["task-import"]
    row = result.rows[0]
    assert row.operational_signals == ("integration", "import_export", "data_pipeline")
    assert row.gap_severity == "high"
    assert "audit_events" in row.missing_coverage
    assert row.evidence == (
        "files_or_modules: src/blueprint/integrations/salesforce_export.py",
        "files_or_modules: src/blueprint/pipelines/account_backfill.py",
        "description: Transform partner records into normalized accounts.",
        "metadata.schedule: nightly batch data pipeline",
    )
    assert result.summary["task_count"] == 2
    assert result.summary["operational_task_count"] == 1


def test_high_risk_integration_receives_stronger_expectations():
    result = analyze_task_observability_gaps(
        _task(
            "task-risk",
            title="Replace payment queue integration",
            description="Process external payment callbacks from the queue.",
            risk_level="high",
            acceptance_criteria=[
                "Structured logs, metrics, and traces cover the callback path."
            ],
        )
    )

    row = result.rows[0]

    assert row.risk_level == "high"
    assert row.gap_severity == "high"
    assert row.expected_telemetry == (
        "logs",
        "metrics",
        "traces",
        "dashboards",
        "alerts",
        "audit_events",
        "slos",
        "runbooks",
    )
    assert row.covered_telemetry == ("logs", "metrics", "traces")
    assert row.missing_coverage == (
        "dashboards",
        "alerts",
        "audit_events",
        "slos",
        "runbooks",
    )
    assert row.recommended_validation_steps[-1] == (
        "Run a failure-mode validation for provider errors, retries, and operator handoff."
    )


def test_model_inputs_iterables_and_stable_serialization_are_supported_without_mutation():
    task_dict = _task(
        "task-dict",
        title="Create billing service",
        description="Add service code for subscription invoice responses.",
        acceptance_criteria=["Metrics and alerts are already documented."],
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Schedule account cleanup cron",
            description="Cron job removes expired account exports.",
            files_or_modules=["src/blueprint/cron/account_cleanup.py"],
            acceptance_criteria=["Dashboard and runbook are ready."],
        )
    )

    first = summarize_task_observability_gaps([task_dict, task_model])
    second = analyze_task_observability_gaps([task_dict, task_model])
    payload = task_observability_gaps_to_dict(first)

    assert task_dict == original
    assert payload == task_observability_gaps_to_dict(second)
    assert first.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "gap_severity",
        "operational_signals",
        "expected_telemetry",
        "covered_telemetry",
        "missing_coverage",
        "evidence",
        "recommended_validation_steps",
    ]
    assert first.summary == {
        "task_count": 2,
        "operational_task_count": 2,
        "gap_count": 2,
        "covered_count": 0,
        "severity_counts": {"none": 0, "low": 0, "medium": 2, "high": 0},
    }


def _plan(tasks):
    return {
        "id": "plan-observability-gaps",
        "implementation_brief_id": "brief-observability-gaps",
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
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
