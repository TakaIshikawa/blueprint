import json

from blueprint.plan_permission_audit_readiness_matrix import (
    PlanPermissionAuditReadinessMatrix,
    PlanPermissionAuditReadinessRow,
    build_plan_permission_audit_readiness_matrix,
    generate_plan_permission_audit_readiness_matrix,
    plan_permission_audit_readiness_matrix_to_dict,
    plan_permission_audit_readiness_matrix_to_markdown,
)


def test_complete_permission_audit_plan_returns_expected_review_rows():
    result = build_plan_permission_audit_readiness_matrix(
        _plan(
            [
                _task(
                    "task-permissions",
                    title="Review role inventory and privileged permissions",
                    description=(
                        "Attach role inventory, review privileged admin permissions, and document "
                        "delegated access for service accounts."
                    ),
                    acceptance_criteria=[
                        "Migration diff evidence includes before and after RBAC permission diff.",
                        "Approval owners provide security approval and role owner sign-off.",
                        "Post-launch sampling plan spot checks access grants after launch.",
                    ],
                    metadata={"security_owner": "security-platform"},
                )
            ]
        )
    )

    assert isinstance(result, PlanPermissionAuditReadinessMatrix)
    assert result.permission_task_ids == ("task-permissions",)
    assert [row.area for row in result.rows] == [
        "role_inventory",
        "privileged_permission_review",
        "delegated_access",
        "migration_diff_evidence",
        "approval_owners",
        "post_launch_sampling",
    ]
    assert all(isinstance(row, PlanPermissionAuditReadinessRow) for row in result.rows)
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.priority == "low" for row in result.rows)
    assert all(row.owner == "security-platform" for row in result.rows)
    assert all(row.evidence for row in result.rows)
    assert result.high_priority_gap_areas == ()
    assert result.summary["priority_counts"] == {"high": 0, "medium": 0, "low": 6}


def test_ownerless_permission_audit_input_surfaces_high_priority_gaps():
    result = build_plan_permission_audit_readiness_matrix(
        _plan(
            [
                _task(
                    "task-ownerless",
                    title="Permission audit role inventory",
                    description=(
                        "Role inventory, privileged permission review, delegated access review, "
                        "migration diff evidence, approval owners, and post-launch sampling are documented."
                    ),
                )
            ]
        )
    )

    assert all(row.owner == "unassigned" for row in result.rows)
    assert all("Missing owner." in row.gaps for row in result.rows)
    assert all(row.priority == "high" for row in result.rows)
    assert result.high_priority_gap_areas == tuple(row.area for row in result.rows)


def test_evidence_poor_permission_audit_input_marks_missing_diff_evidence_high_priority():
    result = build_plan_permission_audit_readiness_matrix(
        _plan(
            [
                _task(
                    "task-rbac",
                    title="Review RBAC permission changes",
                    description=(
                        "Role inventory exists, privileged admin permissions are reviewed, delegated access "
                        "is documented, approval owners are assigned, and post-launch sampling is planned."
                    ),
                    metadata={"approval_owner": "security-platform"},
                )
            ]
        )
    )

    diff = _row(result, "migration_diff_evidence")

    assert diff.gaps == ("Missing permission migration diff evidence.",)
    assert diff.priority == "high"
    assert diff.owner == "security-platform"
    assert "migration_diff_evidence" in result.high_priority_gap_areas
    assert result.summary["priority_counts"] == {"high": 1, "medium": 0, "low": 5}


def test_serialization_aliases_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-permission | audit",
                title="Permission audit | launch",
                description=(
                    "Role inventory, privileged permissions, delegated access, migration diff evidence, "
                    "approval owners, and post-launch sampling are ready."
                ),
                metadata={"owner": "iam-team"},
            )
        ]
    )
    result = build_plan_permission_audit_readiness_matrix(plan)
    payload = plan_permission_audit_readiness_matrix_to_dict(result)
    markdown = plan_permission_audit_readiness_matrix_to_markdown(result)

    assert generate_plan_permission_audit_readiness_matrix(plan).to_dict() == result.to_dict()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "permission_task_ids",
        "high_priority_gap_areas",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "area",
        "owner",
        "evidence",
        "gaps",
        "readiness",
        "priority",
        "next_action",
        "task_ids",
    ]
    assert markdown.startswith("# Plan Permission Audit Readiness Matrix: plan-permission")
    assert "task-permission \\| audit" in markdown
    assert "privileged permissions" in markdown


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks, *, plan_id="plan-permission"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-permission",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, metadata=None):
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
