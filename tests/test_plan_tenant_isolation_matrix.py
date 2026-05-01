import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_tenant_isolation_matrix import (
    PlanTenantIsolationMatrix,
    PlanTenantIsolationMatrixRow,
    build_plan_tenant_isolation_matrix,
    plan_tenant_isolation_matrix_to_dict,
    plan_tenant_isolation_matrix_to_markdown,
    summarize_plan_tenant_isolation,
)


def test_tenant_workspace_and_role_tasks_are_grouped_by_concern_without_mutation():
    plan = _plan(
        [
            _task(
                "task-tenant-query",
                title="Scope account search by tenant",
                description=(
                    "Add tenant_id filter to customer account search and enforce authorization."
                ),
                acceptance_criteria=[
                    "Run negative cross-tenant tests with two tenant fixtures.",
                ],
            ),
            _task(
                "task-workspace-role",
                title="Add workspace membership role checks",
                description="Validate workspace membership before granting admin role permissions.",
                acceptance_criteria=["Run permission test matrix for member and non-member users."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_tenant_isolation_matrix(plan)
    payload = plan_tenant_isolation_matrix_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanTenantIsolationMatrix)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "concern_type",
        "task_ids",
        "risk_level",
        "required_safeguards",
        "missing_safeguards",
        "recommended_validation",
        "evidence",
    ]

    by_concern = {row.concern_type: row for row in result.rows}

    assert isinstance(by_concern["tenant"], PlanTenantIsolationMatrixRow)
    assert by_concern["tenant"].task_ids == ("task-tenant-query",)
    assert by_concern["tenant"].risk_level == "high"
    assert by_concern["tenant"].missing_safeguards == ()
    assert by_concern["workspace"].task_ids == ("task-workspace-role",)
    assert by_concern["workspace"].missing_safeguards == (
        "tenant_scoping: Scope every read, write, and background job to the active tenant boundary.",
        "authorization: Enforce server-side authorization before returning or mutating tenant-scoped data.",
    )
    assert by_concern["membership"].task_ids == ("task-workspace-role",)
    assert any(
        item.startswith("membership_validation:")
        for item in by_concern["membership"].required_safeguards
    )
    assert by_concern["role"].task_ids == ("task-workspace-role",)
    assert result.summary == {
        "concern_count": 6,
        "impacted_task_count": 2,
        "risk_counts": {"critical": 1, "high": 5, "medium": 0},
        "missing_safeguard_count": sum(len(row.missing_safeguards) for row in result.rows),
    }


def test_plan_metadata_cross_tenant_and_impersonation_are_critical():
    result = summarize_plan_tenant_isolation(
        _plan(
            [
                _task(
                    "task-support",
                    title="Implement support admin impersonation",
                    description=(
                        "Support access can impersonate a user with reason code and audit log."
                    ),
                    acceptance_criteria=[
                        "Test impersonation sessions expire and cannot elevate permissions."
                    ],
                )
            ],
            metadata={
                "risk": "Cross-tenant reporting may aggregate organization metrics.",
                "safeguard": "Use tenant scoped queries and cross-tenant test fixtures.",
            },
        )
    )

    by_concern = {row.concern_type: row for row in result.rows}

    assert [row.concern_type for row in result.rows][:2] == [
        "cross_tenant",
        "admin_impersonation",
    ]
    assert by_concern["cross_tenant"].risk_level == "critical"
    assert by_concern["cross_tenant"].task_ids == ()
    assert "plan metadata" in result.to_markdown()
    assert by_concern["admin_impersonation"].risk_level == "critical"
    assert by_concern["admin_impersonation"].task_ids == ("task-support",)
    assert any(
        item.startswith("impersonation_controls:")
        for item in by_concern["admin_impersonation"].required_safeguards
    )
    assert any(
        "captures a reason" in item
        for item in by_concern["admin_impersonation"].recommended_validation
    )
    assert result.summary["risk_counts"] == {"critical": 2, "high": 3, "medium": 0}


def test_invites_shared_resources_and_organization_signals_include_missing_safeguards():
    result = build_plan_tenant_isolation_matrix(
        [
            _task(
                "task-invites",
                title="Add organization invite workflow",
                description="Create invitation links for organization members.",
                acceptance_criteria=["Audit invite creation and revocation."],
            ),
            _task(
                "task-sharing",
                title="Add shared dashboard public link",
                description="Shared resource can be opened from a share link.",
            ),
        ]
    )

    by_concern = {row.concern_type: row for row in result.rows}

    assert by_concern["organization"].task_ids == ("task-invites",)
    assert by_concern["invite"].risk_level == "medium"
    assert by_concern["invite"].task_ids == ("task-invites",)
    assert any(
        item.startswith("invite_controls:") for item in by_concern["invite"].missing_safeguards
    )
    assert by_concern["shared_resource"].risk_level == "medium"
    assert by_concern["shared_resource"].task_ids == ("task-sharing",)
    assert any(
        "shared links, exports, and listings" in item
        for item in by_concern["shared_resource"].recommended_validation
    )


def test_model_empty_invalid_dict_and_markdown_outputs_are_stable():
    task = _task(
        "task-pipe",
        title="Tenant | workspace boundary",
        description="Use tenant scoped authorization for workspace records.",
    )
    plan_model = ExecutionPlan.model_validate(_plan([task]))

    model_result = build_plan_tenant_isolation_matrix(plan_model)
    empty_result = build_plan_tenant_isolation_matrix(_plan([]))
    invalid_result = build_plan_tenant_isolation_matrix("not a plan")
    plain_dict_result = build_plan_tenant_isolation_matrix(
        {"id": "dict-task", "title": "Role permissions"}
    )

    assert model_result.plan_id == "plan-tenant"
    assert model_result.rows[0].concern_type == "tenant"
    assert "Tenant \\| workspace boundary" in model_result.to_markdown()
    assert plain_dict_result.plan_id is None
    assert plain_dict_result.rows[0].concern_type == "role"
    assert invalid_result.rows == ()
    assert invalid_result.summary == {
        "concern_count": 0,
        "impacted_task_count": 0,
        "risk_counts": {"critical": 0, "high": 0, "medium": 0},
        "missing_safeguard_count": 0,
    }
    assert empty_result.to_dict() == {
        "plan_id": "plan-tenant",
        "rows": [],
        "summary": {
            "concern_count": 0,
            "impacted_task_count": 0,
            "risk_counts": {"critical": 0, "high": 0, "medium": 0},
            "missing_safeguard_count": 0,
        },
    }
    assert plan_tenant_isolation_matrix_to_markdown(empty_result) == (
        "# Plan Tenant Isolation Matrix: plan-tenant\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Concern count: 0\n"
        "- Impacted task count: 0\n"
        "- Critical risk count: 0\n"
        "- High risk count: 0\n"
        "- Medium risk count: 0\n"
        "- Missing safeguard count: 0\n"
        "\n"
        "No tenant isolation impact was detected."
    )


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-tenant",
        "implementation_brief_id": "brief-tenant",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
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
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
