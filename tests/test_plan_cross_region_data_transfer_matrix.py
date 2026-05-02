import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_cross_region_data_transfer_matrix import (
    PlanCrossRegionDataTransferMatrix,
    PlanCrossRegionDataTransferRow,
    build_plan_cross_region_data_transfer_matrix,
    plan_cross_region_data_transfer_matrix_to_dict,
    plan_cross_region_data_transfer_matrix_to_markdown,
)


def test_cross_region_transfer_signals_create_deterministic_rows_and_controls():
    result = build_plan_cross_region_data_transfer_matrix(
        _plan(
            [
                _task(
                    "task-replicate",
                    title="Replicate customer data from US to EU",
                    description=(
                        "Replicate production customer data from the US primary region to the EU "
                        "read replica with encrypted object storage failover."
                    ),
                    acceptance_criteria=[
                        "DPA review, residency exception, encryption, and owner approval are recorded."
                    ],
                    owner_type="data_platform",
                ),
                _task(
                    "task-analytics",
                    title="Export APAC logs for global analytics",
                    description=(
                        "Export APAC customer support logs into a global analytics warehouse with "
                        "retention bounds."
                    ),
                    metadata={"data_owner": "analytics-platform"},
                ),
                _task(
                    "task-vendor",
                    title="Enable UK support vendor access",
                    description=(
                        "Third-party support processor in Canada needs follow-the-sun support access "
                        "to UK customer account data under SCCs."
                    ),
                    risk_level="high",
                ),
                _task(
                    "task-backup",
                    title="Back up Australia tenant snapshots",
                    description="Copy Australia customer data backups into Canada archive buckets.",
                ),
                _task(
                    "task-docs",
                    title="Refresh runbook",
                    description="Update internal support triage documentation.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanCrossRegionDataTransferMatrix)
    assert result.plan_id == "plan-transfer"
    assert result.transfer_task_ids == (
        "task-replicate",
        "task-analytics",
        "task-vendor",
        "task-backup",
    )
    assert result.no_signal_task_ids == ("task-docs",)
    assert [(row.task_id, row.transfer_type) for row in result.rows] == [
        ("task-replicate", "replication"),
        ("task-replicate", "object_storage"),
        ("task-replicate", "cross_border_transfer"),
        ("task-analytics", "export"),
        ("task-analytics", "logs"),
        ("task-analytics", "analytics"),
        ("task-analytics", "cross_border_transfer"),
        ("task-vendor", "vendor_processing"),
        ("task-vendor", "support_access"),
        ("task-vendor", "cross_border_transfer"),
        ("task-backup", "backup"),
        ("task-backup", "object_storage"),
        ("task-backup", "cross_border_transfer"),
    ]

    replication = _row(result, "task-replicate", "replication")
    assert isinstance(replication, PlanCrossRegionDataTransferRow)
    assert replication.source_regions == ("US",)
    assert replication.destination_regions == ("EU",)
    assert replication.required_controls == (
        "DPA review",
        "encryption",
        "residency exception",
        "owner approval",
    )
    assert replication.priority == "high"
    assert replication.owner_hint == "data_platform"
    assert "title: Replicate customer data from US to EU" in replication.compliance_evidence

    analytics = _row(result, "task-analytics", "analytics")
    assert analytics.source_regions == ("APAC",)
    assert analytics.destination_regions == ("global",)
    assert analytics.required_controls == (
        "DPA review",
        "encryption",
        "residency exception",
        "retention bounds",
        "owner approval",
    )
    assert analytics.priority == "medium"
    assert analytics.owner_hint == "analytics-platform"

    vendor = _row(result, "task-vendor", "vendor_processing")
    assert vendor.source_regions == ("UK",)
    assert vendor.destination_regions == ("Canada", "global")
    assert vendor.required_controls == (
        "DPA review",
        "encryption",
        "residency exception",
        "owner approval",
    )
    assert vendor.priority == "high"
    assert vendor.owner_hint == "privacy_owner"

    backup = _row(result, "task-backup", "backup")
    assert backup.source_regions == ("Australia",)
    assert backup.destination_regions == ("Canada",)
    assert backup.required_controls == (
        "DPA review",
        "encryption",
        "residency exception",
        "retention bounds",
        "owner approval",
    )

    assert result.summary == {
        "task_count": 5,
        "transfer_task_count": 4,
        "no_signal_task_count": 1,
        "transfer_type_counts": {
            "replication": 1,
            "export": 1,
            "logs": 1,
            "backup": 1,
            "vendor_processing": 1,
            "support_access": 1,
            "object_storage": 2,
            "analytics": 1,
            "cross_border_transfer": 4,
        },
        "region_counts": {
            "US": 3,
            "EU": 3,
            "UK": 3,
            "APAC": 4,
            "Canada": 6,
            "Australia": 3,
            "global": 7,
        },
        "priority_counts": {"high": 10, "medium": 3, "low": 0},
    }


def test_execution_plan_input_serialization_ordering_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-export | pipe",
                title="Export EU records | Canada",
                description="Export EU customer data to Canada reporting bucket with retention bounds.",
                metadata={"owner": "reporting"},
            ),
            _task(
                "task-support",
                title="Support access for global cases",
                description="Customer support agents need global case access for US and UK tenants.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_cross_region_data_transfer_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_cross_region_data_transfer_matrix_to_dict(result)
    markdown = plan_cross_region_data_transfer_matrix_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "transfer_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "task_title",
        "source_regions",
        "destination_regions",
        "transfer_type",
        "required_controls",
        "compliance_evidence",
        "owner_hint",
        "priority",
    ]
    assert [(row.task_id, row.transfer_type, row.priority) for row in result.rows] == [
        ("task-export | pipe", "export", "medium"),
        ("task-export | pipe", "object_storage", "high"),
        ("task-export | pipe", "analytics", "medium"),
        ("task-export | pipe", "cross_border_transfer", "high"),
        ("task-support", "support_access", "high"),
        ("task-support", "cross_border_transfer", "high"),
    ]
    assert markdown.startswith("# Plan Cross-Region Data Transfer Matrix: plan-transfer")
    assert (
        "| Task | Title | Source Regions | Destination Regions | Transfer Type | Controls | "
        "Priority | Owner | Evidence |"
    ) in markdown
    assert "`task-export \\| pipe`" in markdown
    assert "Export EU records \\| Canada" in markdown
    assert plan_cross_region_data_transfer_matrix_to_markdown(result) == result.to_markdown()


def test_empty_invalid_and_no_signal_inputs_render_deterministic_empty_outputs():
    empty = build_plan_cross_region_data_transfer_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_cross_region_data_transfer_matrix(17)
    no_signal = build_plan_cross_region_data_transfer_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Optimize API pagination",
                    description="Tune backend query limits for account search.",
                    files_or_modules=["src/api/search.py"],
                )
            ]
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "transfer_task_ids": [],
        "no_signal_task_ids": [],
        "summary": {
            "task_count": 0,
            "transfer_task_count": 0,
            "no_signal_task_count": 0,
            "transfer_type_counts": {
                "replication": 0,
                "export": 0,
                "logs": 0,
                "backup": 0,
                "vendor_processing": 0,
                "support_access": 0,
                "object_storage": 0,
                "analytics": 0,
                "cross_border_transfer": 0,
            },
            "region_counts": {
                "US": 0,
                "EU": 0,
                "UK": 0,
                "APAC": 0,
                "Canada": 0,
                "Australia": 0,
                "global": 0,
            },
            "priority_counts": {"high": 0, "medium": 0, "low": 0},
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Cross-Region Data Transfer Matrix: empty-plan",
            "",
            "Summary: 0 of 0 tasks require transfer planning (high: 0, medium: 0, low: 0).",
            "",
            "No cross-region data transfer rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert no_signal.rows == ()
    assert no_signal.no_signal_task_ids == ("task-api",)
    assert "No transfer signals: task-api" in no_signal.to_markdown()


def _row(result, task_id, transfer_type):
    return next(
        row for row in result.rows if row.task_id == task_id and row.transfer_type == transfer_type
    )


def _plan(tasks, *, plan_id="plan-transfer"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-transfer",
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
    owner_type=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if owner_type is not None:
        task["owner_type"] = owner_type
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
