import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_residency_readiness import (
    DataResidencyReadinessTask,
    TaskDataResidencyReadinessPlan,
    TaskDataResidencyReadinessRecord,
    analyze_task_data_residency_readiness,
    build_task_data_residency_readiness_plan,
    derive_task_data_residency_readiness,
    extract_task_data_residency_readiness,
    generate_task_data_residency_readiness,
    recommend_task_data_residency_readiness,
    summarize_task_data_residency_readiness,
    task_data_residency_readiness_plan_to_dict,
    task_data_residency_readiness_plan_to_dicts,
    task_data_residency_readiness_plan_to_markdown,
    task_data_residency_readiness_to_dicts,
)


def test_strict_residency_requirement_generates_execution_ready_tasks_with_evidence():
    result = build_task_data_residency_readiness_plan(
        _plan(
            [
                _task(
                    "task-eu-only",
                    title="Store EU tenant profile data",
                    description=(
                        "EU customer data must remain in the EU only. Implement data residency for "
                        "profile storage before launch."
                    ),
                    files_or_modules=["infra/data_residency/eu_only_profiles.tf"],
                    metadata={"compliance": "Audit evidence must show EU storage and processing."},
                )
            ]
        )
    )

    assert isinstance(result, TaskDataResidencyReadinessPlan)
    assert len(result.records) == 1
    record = result.records[0]
    assert isinstance(record, TaskDataResidencyReadinessRecord)
    assert record.task_id == "task-eu-only"
    assert record.detected_signals == (
        "data_residency",
        "strict_region",
        "storage_boundary",
        "compliance_evidence",
    )
    assert "EU" in record.regions
    assert _categories(record) == (
        "region_selection",
        "storage_boundaries",
        "replication_policy",
        "backup_location",
        "observability",
        "compliance_evidence",
        "rollout_validation",
    )
    assert all(isinstance(task, DataResidencyReadinessTask) for task in record.generated_tasks)
    assert all(len(task.acceptance_criteria) >= 3 for task in record.generated_tasks)
    assert any("description: EU customer data must remain" in item for item in record.evidence)
    assert any("Rationale: description: EU customer data must remain" in task.description for task in record.generated_tasks)
    assert result.data_residency_task_ids == ("task-eu-only",)
    assert result.impacted_task_ids == result.data_residency_task_ids
    assert result.summary["generated_task_category_counts"]["region_selection"] == 1


def test_multi_region_constraints_preserve_region_context_and_serialization_is_stable():
    plan = _plan(
        [
            _task(
                "task-multi-region",
                title="Regional tenant sharding | EU and US",
                description=(
                    "Multi-region customers need tenant isolation by region. EU and US tenants cannot "
                    "share resident data stores."
                ),
                acceptance_criteria=[
                    "Allowed regions include EU and US tenant shards.",
                    "Rollout validation covers regional tests for both shards.",
                ],
                metadata={
                    "allowed_regions": ["EU", "US"],
                    "observability": "Alert on region drift and cross-region transfer.",
                },
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = analyze_task_data_residency_readiness(plan)
    payload = task_data_residency_readiness_plan_to_dict(result)
    markdown = task_data_residency_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_data_residency_readiness_plan_to_dicts(result) == payload["records"]
    assert task_data_residency_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_data_residency_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_data_residency_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_data_residency_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_data_residency_readiness(plan).to_dict() == result.to_dict()
    assert summarize_task_data_residency_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_data_residency_readiness(plan).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert {"EU", "US"} <= set(result.records[0].regions)
    assert "multi_region_constraint" in result.records[0].detected_signals
    assert "observability" in result.records[0].detected_signals
    assert "Regional tenant sharding \\| EU and US" in markdown
    assert "| Task | Title | Readiness | Signals | Regions | Generated Tasks | Evidence |" in markdown
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "data_residency_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "regions",
        "generated_tasks",
        "readiness",
        "evidence",
    ]


def test_backup_and_replication_concerns_generate_specific_acceptance_criteria():
    result = build_task_data_residency_readiness_plan(
        _plan(
            [
                _task(
                    "task-backup-replication",
                    title="Constrain backup and read replica regions",
                    description=(
                        "Backup location and replication policy must keep snapshots and read replicas "
                        "inside Canada for resident data."
                    ),
                    validation_commands={
                        "residency": ["poetry run pytest tests/infra/test_backup_region_residency.py"]
                    },
                )
            ]
        )
    )

    record = result.records[0]
    by_category = {task.category: task for task in record.generated_tasks}
    assert {"replication_policy", "backup_location"} <= set(record.detected_signals)
    assert "Canada" in record.regions
    assert "replication_policy" in by_category
    assert "backup_location" in by_category
    assert any("Replication destinations are limited" in item for item in by_category["replication_policy"].acceptance_criteria)
    assert any("Backup and snapshot locations are configured" in item for item in by_category["backup_location"].acceptance_criteria)
    assert any("validation_commands: poetry run pytest tests/infra/test_backup_region_residency.py" in item for item in record.evidence)
    assert record.readiness == "partial"


def test_no_residency_needs_empty_invalid_and_object_model_inputs_are_supported():
    no_match = build_task_data_residency_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update onboarding copy", description="Static text only."),
                _task(
                    "task-explicit-none",
                    title="Profile cache tuning",
                    description="No data residency requirements are in scope for this cache TTL change.",
                ),
            ]
        )
    )
    empty = build_task_data_residency_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_data_residency_readiness_plan(13)
    object_task = SimpleNamespace(
        id="task-object",
        title="Japan object storage boundary",
        description="Resident data must be stored in Japan object storage buckets.",
        files_or_modules=["infra/storage_boundary/japan_bucket_region.tf"],
        acceptance_criteria=["Done."],
        status="pending",
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="EU backup residency",
            description="Backup region must stay in the EU for customer exports.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([model_task.model_dump(mode="python")], plan_id="plan-model")
    )

    object_result = build_task_data_residency_readiness_plan([object_task])
    model_result = build_task_data_residency_readiness_plan(plan_model)

    assert no_match.records == ()
    assert no_match.no_impact_task_ids == ("task-copy", "task-explicit-none")
    assert "No task data residency readiness records were inferred." in no_match.to_markdown()
    assert "No-impact tasks: task-copy, task-explicit-none" in no_match.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert object_result.records[0].task_id == "task-object"
    assert "Japan" in object_result.records[0].regions
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"


def _categories(record):
    return tuple(task.category for task in record.generated_tasks)


def _plan(tasks, plan_id="plan-data-residency"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-data-residency",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-data-residency",
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
