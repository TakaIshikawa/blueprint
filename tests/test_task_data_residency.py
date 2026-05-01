import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_residency import (
    TaskDataResidencyPlan,
    TaskDataResidencyRecord,
    analyze_task_data_residency,
    build_task_data_residency_plan,
    summarize_task_data_residency,
    task_data_residency_plan_to_dict,
    task_data_residency_plan_to_markdown,
)


def test_eu_us_region_routing_requires_residency_review():
    result = build_task_data_residency_plan(
        _plan(
            [
                _task(
                    "task-region-routing",
                    title="Route EU customer data to US search cluster",
                    description="Add region routing so EU profiles can fail over to the us-east-1 data store.",
                    files_or_modules=[
                        "src/blueprint/routing/eu_us_region_router.py",
                        "infra/regions/us-east-1/search.tf",
                    ],
                    acceptance_criteria=[
                        "Cross-border transfer impact assessment is attached before rollout."
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDataResidencyPlan)
    assert result.plan_id == "plan-data-residency"
    assert result.review_task_ids == ("task-region-routing",)
    assert result.summary == {
        "task_count": 1,
        "review_task_count": 1,
        "status_counts": {
            "residency_review_required": 1,
            "residency_review_recommended": 0,
            "residency_review_not_needed": 0,
        },
        "surface_counts": {
            "primary data store": 1,
            "region routing": 1,
            "replicas": 0,
            "backups": 0,
            "analytics exports": 0,
            "logs": 0,
            "cdn caches": 0,
            "third-party processors": 0,
        },
        "data_category_counts": {
            "personal data": 1,
            "authentication data": 0,
            "payment data": 0,
            "analytics data": 0,
            "logs": 0,
            "backup data": 0,
        },
    }
    record = result.records[0]
    assert isinstance(record, TaskDataResidencyRecord)
    assert record.residency_review_status == "residency_review_required"
    assert record.region_signals == ("eu", "us", "us-east-1")
    assert record.affected_surfaces == ("primary data store", "region routing")
    assert record.data_categories == ("personal data",)
    assert record.review_reasons == (
        "Task references geographic regions or residency jurisdictions.",
        "Task references cross-border movement, routing, replication, or processor changes.",
        "Task touches storage, routing, backup, analytics, cache, log, or processor surfaces.",
        "Task references data categories with residency or localization implications.",
    )
    assert record.recommended_actions[:2] == (
        "Confirm source and destination regions, residency commitments, and allowed transfer mechanism before implementation.",
        "Document affected storage, processing, cache, log, backup, and analytics surfaces with owners.",
    )
    assert record.evidence[:3] == (
        "files_or_modules: src/blueprint/routing/eu_us_region_router.py",
        "files_or_modules: infra/regions/us-east-1/search.tf",
        "title: Route EU customer data to US search cluster",
    )


def test_backups_logs_and_analytics_exports_are_flagged_with_actions():
    result = analyze_task_data_residency(
        [
            _task(
                "task-backups",
                title="Add EU backup replica",
                description="Replicate billing backup snapshots to eu-west-1 for disaster recovery.",
                files_or_modules=["infra/backups/billing_replica.tf"],
            ),
            _task(
                "task-logs",
                title="Ship auth logs to analytics warehouse",
                description="Export authentication logs and IP address events to the US analytics warehouse.",
                files_or_modules=["pipelines/analytics/log_export.py"],
            ),
        ]
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-backups"].residency_review_status == "residency_review_required"
    assert by_id["task-backups"].affected_surfaces == (
        "replicas",
        "backups",
    )
    assert by_id["task-backups"].data_categories == ("payment data", "backup data")
    assert (
        "Verify replica, backup, restore, retention, and disaster-recovery locations comply with residency requirements."
        in by_id["task-backups"].recommended_actions
    )
    assert by_id["task-logs"].residency_review_status == "residency_review_required"
    assert by_id["task-logs"].affected_surfaces == ("analytics exports", "logs")
    assert by_id["task-logs"].data_categories == (
        "personal data",
        "authentication data",
        "analytics data",
        "logs",
    )
    assert (
        "Review log, telemetry, warehouse, and analytics export destinations plus retention and access controls."
        in by_id["task-logs"].recommended_actions
    )


def test_third_party_processor_and_cdn_cache_signals_are_recommended_without_region():
    result = build_task_data_residency_plan(
        [
            _task(
                "task-processor",
                title="Send profile webhooks to subprocessor",
                description="Add a vendor webhook processor for customer profile updates.",
                files_or_modules=["src/blueprint/integrations/vendor_webhook.py"],
            ),
            _task(
                "task-cache",
                title="Cache profile avatars at CDN edge",
                description="Store customer avatar cache entries in the CDN.",
                files_or_modules=["infra/cdn/avatar_cache.tf"],
            ),
        ]
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-cache"].residency_review_status == "residency_review_recommended"
    assert by_id["task-cache"].affected_surfaces == ("cdn caches",)
    assert (
        "Confirm CDN or edge cache locations, purge behavior, and cache-key data do not violate localization commitments."
        in by_id["task-cache"].recommended_actions
    )
    assert by_id["task-processor"].residency_review_status == "residency_review_required"
    assert by_id["task-processor"].affected_surfaces == ("third-party processors",)
    assert (
        "Validate processor or subprocessor region coverage, data processing terms, and transfer impact assessment needs."
        in by_id["task-processor"].recommended_actions
    )


def test_benign_ui_copy_and_doc_only_surface_mentions_do_not_need_review():
    result = build_task_data_residency_plan(
        [
            _task(
                "task-copy",
                title="Update settings page copy",
                description="Adjust labels and helper text for the profile page.",
                files_or_modules=["src/blueprint/ui/settings_copy.py"],
            ),
            _task(
                "task-docs",
                title="Document analytics dashboard labels",
                description="Update docs for reading log charts.",
                files_or_modules=["docs/analytics-dashboard.md"],
            ),
        ]
    )

    assert [record.residency_review_status for record in result.records] == [
        "residency_review_not_needed",
        "residency_review_not_needed",
    ]
    assert result.review_task_ids == ()
    assert all(record.recommended_actions == () for record in result.records)


def test_model_inputs_malformed_optional_fields_and_serialization_are_stable():
    task_dict = _task(
        "task-malformed",
        title="Refresh CA storage metadata | reports",
        description="Update regional storage metadata for customer reports.",
        files_or_modules={"main": "infra/storage/ca-central-1/reports.tf", "none": None},
        acceptance_criteria={"review": "Validate residency owner approval."},
        metadata={
            "region": "CA",
            "notes": [{"surface": "storage bucket"}, None, 7],
            "bad": 7,
        },
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Export EU analytics data",
            description="Add analytics export for user events in Europe.",
            files_or_modules=["pipelines/exports/eu_events.py"],
        )
    )
    raw_result = build_task_data_residency_plan(_plan([task_dict]))
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")]))

    first = summarize_task_data_residency(_plan([task_dict, task_model.model_dump(mode="python")]))
    second = build_task_data_residency_plan(plan_model)
    payload = task_data_residency_plan_to_dict(first)
    markdown = task_data_residency_plan_to_markdown(first)

    assert task_dict == original
    assert raw_result.records[0].task_id == "task-malformed"
    assert raw_result.records[0].region_signals == ("ca", "ca-central-1")
    assert task_data_residency_plan_to_dict(second)["records"][0]["task_id"] == "task-model"
    assert first.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "review_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "residency_review_status",
        "region_signals",
        "affected_surfaces",
        "data_categories",
        "review_reasons",
        "recommended_actions",
        "evidence",
    ]
    assert markdown.startswith("# Task Data Residency Plan: plan-data-residency")
    assert "reports \\| reports" not in markdown
    assert "| `task-malformed` | residency_review_required | ca, ca-central-1 |" in markdown
    assert "| `task-model` | residency_review_required | eu |" in markdown


def _plan(tasks):
    return {
        "id": "plan-data-residency",
        "implementation_brief_id": "brief-data-residency",
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
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
