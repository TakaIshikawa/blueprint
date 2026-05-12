import json

from blueprint._simple_task_readiness import SimpleReadinessPlan
from blueprint.domain.models import ExecutionPlan
from blueprint.task_permission_migration_readiness import (
    build_task_permission_migration_readiness,
    build_task_permission_migration_readiness_plan,
    extract_task_permission_migration_readiness_records,
    recommend_task_permission_migration_readiness,
    summarize_task_permission_migration_readiness,
    task_permission_migration_readiness_to_dict,
    task_permission_migration_readiness_to_dicts,
    task_permission_migration_readiness_to_markdown,
)


def test_detects_permission_migration_and_all_required_safeguards():
    result = build_task_permission_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-rbac",
                    title="Migrate RBAC roles to workspace permissions",
                    description="Migrate permissions from legacy RBAC role model.",
                    acceptance_criteria=[
                        "Principal inventory covers affected users, groups, and service accounts.",
                        "Permission mapping documents old-to-new role and scope mapping.",
                        "Least privilege review checks privileged roles for over-grants.",
                        "Fallback access uses break glass and restore previous access steps.",
                        "Audit logs emit permission change events.",
                        "Rollout validation uses canary accounts and authorization tests.",
                    ],
                    files_or_modules=["src/authz/rbac_permission_migration.py"],
                )
            ]
        )
    )

    assert isinstance(result, SimpleReadinessPlan)
    record = result.records[0]
    assert record.detected_signals == ("rbac_migration", "acl_migration")
    assert record.present_criteria == (
        "principal_inventory",
        "permission_mapping",
        "least_privilege_review",
        "fallback_access",
        "audit_logging",
        "rollout_validation",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_reports_missing_permission_safeguards_from_metadata_dependencies_and_paths():
    source = _plan(
        [
            _task(
                "task-scope",
                title="Change OAuth scopes for vendor API",
                description="Narrow OAuth scopes and sync plan entitlements.",
                depends_on=["authorization policy rewrite"],
                metadata={"access": {"mapping": "scope mapping for legacy-to-new grants"}},
                files_or_modules=["src/policies/entitlement_scope_migration.py"],
            ),
            _task("task-copy", title="Admin copy", description="No role or permission migration changes are required."),
        ]
    )

    result = build_task_permission_migration_readiness_plan(ExecutionPlan.model_validate(source))

    assert result.impacted_task_ids == ("task-scope",)
    assert result.ignored_task_ids == ("task-copy",)
    record = result.records[0]
    assert record.detected_signals == ("scope_or_entitlement_change",)
    assert record.present_criteria == ("permission_mapping",)
    assert record.missing_criteria == (
        "principal_inventory",
        "least_privilege_review",
        "fallback_access",
        "audit_logging",
        "rollout_validation",
    )
    assert any("depends_on" in item for item in record.evidence)
    assert any("metadata.access.mapping" in item for item in record.evidence)
    assert any(record.recommended_follow_up_actions)


def test_aliases_serialization_markdown_sorting_and_invalid_inputs_are_stable():
    source = _plan(
        [
            _task("task-missing", title="Rewrite ACL policy", description="ACL permission model migration."),
            _task(
                "task-partial",
                title="RBAC role migration",
                description="RBAC role migration with audit logging.",
            ),
        ],
        plan_id="plan-permission-sort",
    )

    result = build_task_permission_migration_readiness(source)
    payload = task_permission_migration_readiness_to_dict(result)
    markdown = task_permission_migration_readiness_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-missing", "task-partial"]
    assert summarize_task_permission_migration_readiness(result) is result
    assert extract_task_permission_migration_readiness_records(source) == result.records
    assert recommend_task_permission_migration_readiness(source) == result.records
    assert result.to_dicts() == payload["records"]
    assert task_permission_migration_readiness_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Task Permission Migration Readiness: plan-permission-sort")
    assert build_task_permission_migration_readiness_plan(42).records == ()
    assert build_task_permission_migration_readiness_plan({"tasks": "bad"}).records == ()


def _plan(tasks, *, plan_id="plan-permission"):
    return {"id": plan_id, "implementation_brief_id": "brief-permission", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    depends_on=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if depends_on is not None:
        task["depends_on"] = depends_on
    if metadata is not None:
        task["metadata"] = metadata
    return task
