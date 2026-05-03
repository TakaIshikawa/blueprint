import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_permission_migration_readiness import (
    TaskPermissionMigrationReadinessPlan,
    TaskPermissionMigrationReadinessRecord,
    build_task_permission_migration_readiness,
    build_task_permission_migration_readiness_plan,
    derive_task_permission_migration_readiness_plan,
    extract_task_permission_migration_readiness_records,
    generate_task_permission_migration_readiness_plan,
    summarize_task_permission_migration_readiness,
    task_permission_migration_readiness_to_dict,
    task_permission_migration_readiness_to_dicts,
    task_permission_migration_readiness_to_markdown,
)


def test_missing_readiness_detects_permission_migration_vectors_from_task_prose_and_paths():
    result = build_task_permission_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-rbac",
                    title="Migrate legacy roles to workspace roles",
                    description=(
                        "Change the RBAC role model, rewrite access policies, and migrate permissions "
                        "for existing workspace admins."
                    ),
                    files_or_modules=[
                        "src/authz/roles/migration.py",
                        "src/policies/workspace_rules.py",
                    ],
                    acceptance_criteria=["Existing permissions are granted under the new roles."],
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Polish labels in the admin settings page.",
                ),
            ],
            risks=["Role migration can over-grant access if the mapping is incomplete."],
        )
    )

    assert isinstance(result, TaskPermissionMigrationReadinessPlan)
    assert all(isinstance(record, TaskPermissionMigrationReadinessRecord) for record in result.records)
    assert result.ignored_task_ids == ("task-copy",)
    record = result.records[0]
    assert record.task_id == "task-rbac"
    assert record.migration_vectors == (
        "role_model_change",
        "permission_backfill",
        "policy_rewrite",
    )
    assert record.present_safeguards == ()
    assert record.readiness_level == "missing"
    assert record.missing_safeguards == (
        "compatibility_mapping",
        "migration_backfill",
        "audit_events",
        "rollback_plan",
        "access_review",
        "test_fixtures",
        "customer_communication",
    )
    assert any("old-to-new" in action for action in record.recommended_followups)
    assert "files_or_modules: src/authz/roles/migration.py" in record.evidence
    assert any(evidence.startswith("risks[0]: Role migration") for evidence in record.evidence)
    assert result.summary["permission_task_count"] == 1
    assert result.summary["ignored_task_count"] == 1
    assert result.summary["vector_counts"]["role_model_change"] == 1
    assert result.summary["readiness_counts"]["missing"] == 1


def test_partial_readiness_reports_present_and_missing_safeguards_from_acceptance_and_risks():
    result = build_task_permission_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-scopes",
                    title="Change OAuth scopes for partner API",
                    description="Narrow OAuth scopes and sync plan entitlements to subscription access.",
                    acceptance_criteria=[
                        "Compatibility mapping covers old-to-new scopes.",
                        "Audit events record every permission change.",
                        "Authorization tests cover read scope and write scope fixtures.",
                    ],
                    risks=["Rollback plan is still missing for entitlement sync failures."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.migration_vectors == ("scope_change", "entitlement_sync")
    assert record.present_safeguards == (
        "compatibility_mapping",
        "audit_events",
        "test_fixtures",
    )
    assert record.missing_safeguards == (
        "migration_backfill",
        "rollback_plan",
        "access_review",
        "customer_communication",
    )
    assert record.readiness_level == "partial"
    assert any("rollback or fallback" in action for action in record.recommended_actions)
    assert result.summary["present_safeguard_counts"]["audit_events"] == 1
    assert result.summary["missing_safeguard_counts"]["rollback_plan"] == 1


def test_strong_readiness_requires_all_permission_safeguards_where_applicable():
    result = build_task_permission_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-groups",
                    title="Map SAML groups to scoped support roles",
                    description=(
                        "Group mapping changes SAML groups, introduces scoped access, and allows "
                        "break glass admin override."
                    ),
                    acceptance_criteria=[
                        "Compatibility matrix maps legacy-to-new groups, roles, and scopes.",
                        "Migration backfill reconciles existing access with an idempotent backfill job.",
                        "Audit logs emit role change events and admin override security events.",
                        "Rollback plan restores previous access semantics.",
                        "Access review validates least privilege for privileged roles.",
                        "Permission tests include access matrix fixtures and policy regression tests.",
                        "Customer communication includes admin notice, release notes, and support runbook.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.migration_vectors == ("role_model_change", "scope_change", "group_mapping", "admin_override")
    assert record.present_safeguards == (
        "compatibility_mapping",
        "migration_backfill",
        "audit_events",
        "rollback_plan",
        "access_review",
        "test_fixtures",
        "customer_communication",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "strong"
    assert record.recommended_followups == ()
    assert result.summary["status"] == "strong"
    assert result.summary["readiness_counts"]["strong"] == 1


def test_nested_metadata_evidence_and_plan_context_are_recursive_and_deterministic():
    result = build_task_permission_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Entitlement sync for add-on seats",
                    description="Sync entitlements from billing to product access.",
                    metadata={
                        "authorization": {
                            "policy_rewrite": "Policy rewrite moves add-on checks into the policy engine.",
                            "safeguards": {
                                "access_review": "Security review approves privileged add-on access.",
                                "customer_communication": "Customer notice explains admin-visible access changes.",
                            },
                        }
                    },
                )
            ],
            acceptance_criteria=["Backfill job reconciles existing entitlements."],
            metadata={"audit_events": {"required": "Audit events capture migrated grants."}},
        )
    )

    record = result.records[0]
    assert record.migration_vectors == ("entitlement_sync", "policy_rewrite")
    assert record.present_safeguards == (
        "migration_backfill",
        "audit_events",
        "access_review",
        "customer_communication",
    )
    assert record.readiness_level == "partial"
    assert any(
        "metadata.authorization.policy_rewrite: Policy rewrite moves add-on checks into the policy engine."
        in evidence
        for evidence in record.evidence
    )
    assert any(
        "metadata.authorization.safeguards.access_review: Security review approves privileged add-on access."
        in evidence
        for evidence in record.evidence
    )
    assert any("acceptance_criteria[0]: Backfill job reconciles existing entitlements." in evidence for evidence in record.evidence)
    assert any("metadata.audit_events.required: Audit events capture migrated grants." in evidence for evidence in record.evidence)


def test_model_inputs_aliases_markdown_and_serialization_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Policy rewrite | group mapping",
                    description="Rewrite policies and map groups from directory claims.",
                    acceptance_criteria=[
                        "Compatibility mapping maps old-to-new policies.",
                        "Rollback plan documents fallback policy.",
                    ],
                )
            ],
            plan_id="plan-permission-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Admin override migration",
            description="Add break glass admin override with audit logs.",
        )
    )

    result = build_task_permission_migration_readiness(plan)
    generated = generate_task_permission_migration_readiness_plan(plan)
    derived = derive_task_permission_migration_readiness_plan(result)
    extracted = extract_task_permission_migration_readiness_records(plan)
    summarized = summarize_task_permission_migration_readiness(task)
    payload = task_permission_migration_readiness_to_dict(result)
    markdown = task_permission_migration_readiness_to_markdown(result)

    assert result.plan_id == "plan-permission-model"
    assert generated.to_dict() == result.to_dict()
    assert derived is result
    assert extracted == result.records
    assert summarized["permission_task_count"] == 1
    assert result.readiness_records == result.records
    assert result.recommendations == result.records
    assert result.records[0].detected_vectors == result.records[0].migration_vectors
    assert result.records[0].recommended_actions == result.records[0].recommended_followups
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert task_permission_migration_readiness_to_dicts(result) == payload["records"]
    assert task_permission_migration_readiness_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "readiness_records",
        "permission_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "migration_vectors",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "evidence",
        "recommended_followups",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Permission Migration Readiness: plan-permission-model")
    assert "Policy rewrite \\| group mapping" in markdown


def test_empty_invalid_and_negated_inputs_return_stable_ignored_tasks_without_mutation():
    plan = _plan(
        [
            _task(
                "task-copy",
                title="Update admin copy",
                description="No role or permission migration changes are required for this copy update.",
            ),
            _task(
                "task-ui",
                title="Settings layout",
                description="Rearrange account settings layout.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_permission_migration_readiness_plan(plan)
    invalid = build_task_permission_migration_readiness_plan({"id": "bad", "tasks": "not a list"})

    assert plan == original
    assert result.records == ()
    assert result.readiness_records == ()
    assert result.permission_task_ids == ()
    assert result.ignored_task_ids == ("task-copy", "task-ui")
    assert result.summary == {
        "task_count": 2,
        "permission_task_count": 0,
        "permission_task_ids": [],
        "ignored_task_count": 2,
        "ignored_task_ids": ["task-copy", "task-ui"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"missing": 0, "partial": 0, "strong": 0},
        "vector_counts": {
            "role_model_change": 0,
            "permission_backfill": 0,
            "scope_change": 0,
            "entitlement_sync": 0,
            "policy_rewrite": 0,
            "group_mapping": 0,
            "admin_override": 0,
        },
        "present_safeguard_counts": {
            "compatibility_mapping": 0,
            "migration_backfill": 0,
            "audit_events": 0,
            "rollback_plan": 0,
            "access_review": 0,
            "test_fixtures": 0,
            "customer_communication": 0,
        },
        "missing_safeguard_counts": {
            "compatibility_mapping": 0,
            "migration_backfill": 0,
            "audit_events": 0,
            "rollback_plan": 0,
            "access_review": 0,
            "test_fixtures": 0,
            "customer_communication": 0,
        },
        "status": "no_permission_migration_signals",
    }
    assert "No permission migration readiness records were inferred." in result.to_markdown()
    assert "Ignored tasks: task-copy, task-ui" in result.to_markdown()
    assert invalid.records == ()
    assert invalid.ignored_task_ids == ()
    assert invalid.summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-permission", risks=None, acceptance_criteria=None, metadata=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-permission",
        "milestones": [],
        "tasks": tasks,
    }
    if risks is not None:
        plan["risks"] = risks
    if acceptance_criteria is not None:
        plan["acceptance_criteria"] = acceptance_criteria
    if metadata is not None:
        plan["metadata"] = metadata
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
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
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if risks is not None:
        task["risks"] = risks
    return task
