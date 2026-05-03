import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_data_archival_readiness_matrix import (
    PlanDataArchivalReadinessMatrix,
    PlanDataArchivalReadinessRow,
    build_plan_data_archival_readiness_matrix,
    generate_plan_data_archival_readiness_matrix,
    plan_data_archival_readiness_matrix_to_dict,
    plan_data_archival_readiness_matrix_to_dicts,
    plan_data_archival_readiness_matrix_to_markdown,
)


def test_complete_archival_plan_emits_ready_rows_with_owner_evidence_risk_and_next_action():
    result = build_plan_data_archival_readiness_matrix(
        _plan(
            [
                _task(
                    "task-archive-policy",
                    title="Define customer data archival trigger and storage tier",
                    description=(
                        "Archive inactive customer records after the 365 day retention window "
                        "using a Glacier cold storage tier and validated lifecycle policy."
                    ),
                    acceptance_criteria=[
                        "Retrieval SLA restores archived records within 4 hours for compliance requests.",
                        "Legal hold handling suspends deletion while litigation hold is active.",
                    ],
                    metadata={"owner": "data-platform"},
                ),
                _task(
                    "task-restore-delete",
                    title="Restore testing and deletion handoff ownership",
                    description=(
                        "Run a quarterly restore test and assign deletion handoff ownership to "
                        "the privacy operations owner after archival."
                    ),
                    acceptance_criteria=[
                        "Sample restore testing evidence and deletion owner sign-off are attached."
                    ],
                    metadata={"deletion_owner": "privacy-ops"},
                ),
            ]
        )
    )

    assert isinstance(result, PlanDataArchivalReadinessMatrix)
    assert result.plan_id == "plan-archival"
    assert result.archival_task_ids == ("task-archive-policy", "task-restore-delete")
    assert [row.area for row in result.rows] == [
        "archival_trigger_definition",
        "storage_tier_validation",
        "retrieval_sla",
        "legal_hold_handling",
        "restore_testing",
        "deletion_handoff_ownership",
    ]
    assert all(isinstance(row, PlanDataArchivalReadinessRow) for row in result.rows)
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.risk == "low" for row in result.rows)
    assert all(row.evidence for row in result.rows)
    assert all(row.owner for row in result.rows)
    assert all(row.next_action == "Ready for archival handoff." for row in result.rows)
    assert result.gap_areas == ()
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 6}
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 6}


def test_incomplete_archival_plan_marks_retrieval_legal_hold_and_restore_testing_gaps():
    result = build_plan_data_archival_readiness_matrix(
        _plan(
            [
                _task(
                    "task-archive-basic",
                    title="Archive account exports after retention window",
                    description=(
                        "Archive account exports after a 90 day retention window in the cold storage tier."
                    ),
                    acceptance_criteria=[
                        "Deletion handoff owner is privacy operations after archival."
                    ],
                    metadata={"owner": "data-platform"},
                )
            ]
        )
    )

    retrieval = _row(result, "retrieval_sla")
    legal_hold = _row(result, "legal_hold_handling")
    restore = _row(result, "restore_testing")

    assert retrieval.gaps == ("Missing retrieval SLA.",)
    assert legal_hold.gaps == ("Missing legal hold handling.",)
    assert restore.gaps == ("Missing restore testing.",)
    assert retrieval.risk == legal_hold.risk == restore.risk == "high"
    assert retrieval.readiness == legal_hold.readiness == restore.readiness == "partial"
    assert result.gap_areas == ("retrieval_sla", "legal_hold_handling", "restore_testing")
    assert result.summary["risk_counts"] == {"high": 3, "medium": 0, "low": 3}
    assert "Document the retrieval SLA" in retrieval.next_action


def test_serialization_model_input_markdown_empty_invalid_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-archive | records",
                title="Archive records | legal hold",
                description=(
                    "Archive records after TTL, validate Glacier storage tier, set retrieval SLA "
                    "within 2 hours, handle legal hold, run restore testing, and assign deletion owner."
                ),
                metadata={"owner": "records-team"},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_data_archival_readiness_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_data_archival_readiness_matrix_to_dict(result)
    markdown = plan_data_archival_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_data_archival_readiness_matrix(plan).to_dict() == result.to_dict()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_data_archival_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "archival_task_ids",
        "gap_areas",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "area",
        "owner",
        "evidence",
        "gaps",
        "readiness",
        "risk",
        "next_action",
        "task_ids",
    ]
    assert markdown.startswith("# Plan Data Archival Readiness Matrix: plan-archival")
    assert "task-archive \\| records" in markdown
    assert "Archive records \\| legal hold" in markdown

    empty = build_plan_data_archival_readiness_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_data_archival_readiness_matrix(23)

    assert empty.plan_id == "empty-plan"
    assert len(empty.rows) == 6
    assert empty.summary["ready_area_count"] == 0
    assert invalid.plan_id is None
    assert invalid.summary["task_count"] == 0


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks, *, plan_id="plan-archival"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-archival",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
