import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_authorization_boundary import (
    TaskAuthorizationBoundaryPlan,
    generate_task_authorization_boundary_plans,
    task_authorization_boundary_plans_to_dicts,
)


def test_rbac_permissions_admin_and_policy_tasks_get_boundary_plans():
    records = generate_task_authorization_boundary_plans(
        _plan(
            [
                _task(
                    "task-rbac",
                    title="Add RBAC permissions for admin console",
                    description="Enforce role-based permissions and admin-only access policy.",
                    acceptance_criteria=["Role matrix covers admin and member deny cases."],
                ),
                _task(
                    "task-copy",
                    title="Update dashboard labels",
                    description="Copy-only UI text change.",
                ),
            ]
        )
    )

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TaskAuthorizationBoundaryPlan)
    assert record.task_id == "task-rbac"
    assert record.concerns == ("authorization", "rbac", "admin_access", "auth_policy")
    assert "security reviewer" in record.reviewer_roles
    assert "authorization domain owner" in record.reviewer_roles
    assert "trust and safety reviewer" in record.reviewer_roles
    assert any("role matrix" in check.lower() for check in record.boundary_checks)
    assert any("admin-only" in item for item in record.evidence)
    assert any("Permission semantics" in reason for reason in record.escalation_reasons)


def test_ownership_tenant_impersonation_and_permission_migration_have_specific_reviewers():
    records = generate_task_authorization_boundary_plans(
        _plan(
            [
                _task(
                    "task-boundary",
                    title="Migrate permissions for support impersonation",
                    description=(
                        "Add ownership checks, tenant isolation, and break-glass impersonation "
                        "for workspace support access."
                    ),
                    files_or_modules=[
                        "src/tenants/access.py",
                        "src/admin/impersonation.py",
                    ],
                ),
            ]
        )
    )

    record = records[0]
    assert record.concerns == (
        "authorization",
        "ownership",
        "tenant_isolation",
        "admin_access",
        "impersonation",
        "permission_migration",
    )
    assert "multi-tenant architecture reviewer" in record.reviewer_roles
    assert "data migration owner" in record.reviewer_roles
    assert any("server-trusted identifiers" in check for check in record.boundary_checks)
    assert any("Tenant or workspace boundary" in reason for reason in record.escalation_reasons)
    assert "files_or_modules: src/tenants/access.py" in record.evidence


def test_model_input_matches_mapping_input_without_mutation_and_serializes():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Change authorization policy",
                description="Update auth policy to deny by default for billing exports.",
                metadata={"review": "security reviewer required"},
            )
        ],
        plan_id="plan-auth-model",
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    mapping_records = generate_task_authorization_boundary_plans(plan)
    model_records = generate_task_authorization_boundary_plans(model)
    payload = task_authorization_boundary_plans_to_dicts(model_records)

    assert plan == original
    assert payload == task_authorization_boundary_plans_to_dicts(mapping_records)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "task_id",
        "title",
        "concerns",
        "boundary_checks",
        "reviewer_roles",
        "evidence",
        "escalation_reasons",
    ]


def test_deterministic_order_and_deduped_evidence():
    records = generate_task_authorization_boundary_plans(
        _plan(
            [
                _task(
                    "task-z",
                    title="Permissions endpoint",
                    description="Update permissions endpoint.",
                    files_or_modules=["src/permissions/service.py"],
                    acceptance_criteria=["Permissions endpoint rejects missing permission."],
                ),
                _task(
                    "task-a",
                    title="OAuth login",
                    description="Require authentication and valid session for login callback.",
                ),
            ]
        )
    )

    assert [record.task_id for record in records] == ["task-a", "task-z"]
    assert records[0].concerns == ("authentication",)
    assert records[1].concerns == ("authorization",)
    assert len(records[1].evidence) == len(set(records[1].evidence))


def test_empty_unaffected_or_malformed_plans_return_empty_list():
    assert generate_task_authorization_boundary_plans(_plan([])) == []
    assert generate_task_authorization_boundary_plans(
        _plan([_task("task-docs", title="Update README", description="Documentation only.")])
    ) == []
    assert generate_task_authorization_boundary_plans({"tasks": "not a list"}) == []
    assert generate_task_authorization_boundary_plans("not a plan") == []
    assert generate_task_authorization_boundary_plans(None) == []


def _plan(tasks, *, plan_id="plan-auth-boundary"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-auth-boundary",
        "milestones": [{"name": "Authorization"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": (
            ["Boundary behavior is validated."]
            if acceptance_criteria is None
            else acceptance_criteria
        ),
        "metadata": {} if metadata is None else metadata,
    }
