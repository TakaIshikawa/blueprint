import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_data_lineage_readiness_matrix import (
    PlanDataLineageReadinessMatrix,
    PlanDataLineageReadinessRow,
    analyze_plan_data_lineage_readiness_matrix,
    build_plan_data_lineage_readiness_matrix,
    derive_plan_data_lineage_readiness_matrix,
    extract_plan_data_lineage_readiness_matrix,
    generate_plan_data_lineage_readiness_matrix,
    plan_data_lineage_readiness_matrix_to_dict,
    plan_data_lineage_readiness_matrix_to_dicts,
    plan_data_lineage_readiness_matrix_to_markdown,
    summarize_plan_data_lineage_readiness_matrix,
)


def test_matrix_groups_tasks_by_source_transformation_and_consumer():
    result = build_plan_data_lineage_readiness_matrix(
        _plan(
            [
                _task(
                    "task-events-dashboard",
                    title="Build event stream analytics dashboard lineage",
                    description=(
                        "Aggregate Segment event stream data into dashboard metrics for product analytics."
                    ),
                    files_or_modules=["src/analytics/event_stream_dashboard.py"],
                    acceptance_criteria=[
                        "Source mapping from Segment event stream to Looker dashboard is documented.",
                        "Audit evidence includes reconciliation row counts and data quality validation.",
                    ],
                    metadata={"owner": "Analytics Platform"},
                ),
                _task(
                    "task-events-dashboard-tests",
                    title="Validate event stream dashboard data quality",
                    description=(
                        "Aggregate event stream metrics for the analytics dashboard and compare row counts."
                    ),
                    depends_on=["task-events-dashboard"],
                    acceptance_criteria=[
                        "Validation query stores audit evidence for the consumer mapping."
                    ],
                ),
                _task(
                    "task-docs",
                    title="Refresh copy",
                    description="Update onboarding labels.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanDataLineageReadinessMatrix)
    assert all(isinstance(row, PlanDataLineageReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-lineage"
    assert result.lineage_task_ids == (
        "task-events-dashboard",
        "task-events-dashboard-tests",
    )
    assert result.no_lineage_task_ids == ("task-docs",)
    assert len(result.rows) == 1

    row = result.rows[0]
    assert row.source == "event_stream"
    assert row.transformation == "aggregation"
    assert row.consumer == "analytics_dashboard"
    assert row.task_ids == ("task-events-dashboard", "task-events-dashboard-tests")
    assert row.readiness == "ready"
    assert row.severity == "low"
    assert row.gaps == ()
    assert any("acceptance_criteria[0]: Source mapping" in item for item in row.evidence)
    assert any("row counts" in item for item in row.retention_audit_evidence)


def test_severity_scoring_and_missing_source_detection():
    result = build_plan_data_lineage_readiness_matrix(
        _plan(
            [
                _task(
                    "task-missing-source",
                    title="Backfill reporting dashboard with unresolved source",
                    description="Backfill monthly report metrics for finance operations.",
                    acceptance_criteria=[
                        "Gap: upstream source is TBD before coding starts.",
                        "Audit evidence will include reconciliation checks.",
                    ],
                ),
                _task(
                    "task-partial",
                    title="Migrate billing records into finance reports",
                    description="Migrate Stripe billing records for finance reporting.",
                    acceptance_criteria=[
                        "Source and consumer are documented, but retention evidence is not defined."
                    ],
                ),
            ]
        )
    )

    missing = _row(result, "task-missing-source")
    assert missing.source == "missing_source"
    assert missing.transformation == "backfill"
    assert missing.consumer == "reporting"
    assert missing.readiness == "blocked"
    assert missing.severity == "high"
    assert "task-missing-source" in result.missing_source_task_ids
    assert "Missing upstream data source." in missing.gaps

    partial = _row(result, "task-partial")
    assert partial.source == "billing_system"
    assert partial.transformation == "migration"
    assert partial.consumer == "reporting"
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert any("retention evidence is not defined" in gap for gap in partial.gaps)

    assert result.summary["severity_counts"] == {"high": 1, "medium": 1, "low": 0}
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}
    assert result.summary["missing_source_task_count"] == 1


def test_dependency_acceptance_metadata_model_input_and_stable_ordering():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-warehouse",
                    title="Publish user database sync to warehouse",
                    description="Sync account records from Postgres into Snowflake warehouse.",
                    depends_on=["task-schema"],
                    acceptance_criteria=[
                        "Consumer mapping names Snowflake warehouse tables.",
                        "Audit trail captures source mapping, owner review, and validation query.",
                    ],
                    metadata={"retention": "Retain lineage evidence for 90 days."},
                ),
                _task(
                    "task-audit",
                    title="Audit log reporting rollup",
                    description="Aggregate audit log records for compliance reporting.",
                    acceptance_criteria=[
                        "Audit evidence includes checksums and reviewer signoff."
                    ],
                ),
                _task(
                    "task-source-missing",
                    title="Transform dashboard metric feed",
                    description="Transform the dashboard metric feed; source unclear.",
                    acceptance_criteria=["Consumer is the analytics dashboard."],
                ),
            ]
        )
    )

    result = build_plan_data_lineage_readiness_matrix(plan)

    assert [row.severity for row in result.rows] == ["high", "low", "low"]
    assert [row.task_ids[0] for row in result.rows] == [
        "task-source-missing",
        "task-audit",
        "task-warehouse",
    ]
    warehouse = _row(result, "task-warehouse")
    assert warehouse.source == "customer_profile"
    assert warehouse.transformation == "sync"
    assert warehouse.consumer == "warehouse"
    assert warehouse.readiness == "ready"
    assert any("depends_on[0]: task-schema" in item for item in warehouse.evidence)
    assert any("metadata.retention" in item for item in warehouse.retention_audit_evidence)


def test_serialization_aliases_markdown_empty_invalid_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-report | 1",
                title="Billing | reporting migration",
                description="Migrate Stripe billing records into monthly report reporting.",
                acceptance_criteria=[
                    "Source mapping, consumer mapping, and audit evidence are documented.",
                    "Retention evidence includes reconciliation row counts.",
                ],
            ),
            _task("task-copy", title="Copy update", description="Update help text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_data_lineage_readiness_matrix(plan)
    payload = plan_data_lineage_readiness_matrix_to_dict(result)
    markdown = plan_data_lineage_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_data_lineage_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_data_lineage_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_data_lineage_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_data_lineage_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_data_lineage_readiness_matrix(result) == result.summary
    assert plan_data_lineage_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_data_lineage_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "lineage_task_ids",
        "missing_source_task_ids",
        "no_lineage_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "source",
        "transformation",
        "consumer",
        "task_ids",
        "titles",
        "retention_audit_evidence",
        "gaps",
        "readiness",
        "severity",
        "evidence",
    ]
    assert markdown.startswith("# Plan Data Lineage Readiness Matrix: plan-lineage")
    assert "Billing \\| reporting migration" in markdown
    assert "task-report \\| 1" in markdown
    assert "No lineage signals: task-copy" in markdown

    empty = build_plan_data_lineage_readiness_matrix({"id": "empty-lineage", "tasks": []})
    invalid = build_plan_data_lineage_readiness_matrix(23)
    object_result = build_plan_data_lineage_readiness_matrix(
        SimpleNamespace(
            id="task-object",
            title="Customer export pipeline",
            description="Pipeline exports user database records to a customer export.",
            acceptance_criteria=["Audit evidence includes source mapping and retention review."],
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-lineage",
        "rows": [],
        "records": [],
        "lineage_task_ids": [],
        "missing_source_task_ids": [],
        "no_lineage_task_ids": [],
        "summary": {
            "task_count": 0,
            "row_count": 0,
            "lineage_task_count": 0,
            "missing_source_task_count": 0,
            "no_lineage_task_count": 0,
            "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
            "severity_counts": {"high": 0, "medium": 0, "low": 0},
            "source_counts": {},
            "consumer_counts": {},
        },
    }
    assert "No data lineage readiness rows were inferred." in empty.to_markdown()
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert object_result.rows[0].source == "customer_profile"
    assert object_result.rows[0].consumer == "customer_export"


def _row(result, task_id):
    return next(row for row in result.rows if task_id in row.task_ids)


def _plan(tasks, *, plan_id="plan-lineage"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-lineage",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
