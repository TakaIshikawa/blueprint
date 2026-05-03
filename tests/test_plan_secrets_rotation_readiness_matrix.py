import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_secrets_rotation_readiness_matrix import (
    PlanSecretsRotationReadinessMatrix,
    PlanSecretsRotationReadinessRow,
    analyze_plan_secrets_rotation_readiness_matrix,
    build_plan_secrets_rotation_readiness_matrix,
    derive_plan_secrets_rotation_readiness_matrix,
    extract_plan_secrets_rotation_readiness_matrix,
    generate_plan_secrets_rotation_readiness_matrix,
    plan_secrets_rotation_readiness_matrix_to_dict,
    plan_secrets_rotation_readiness_matrix_to_dicts,
    plan_secrets_rotation_readiness_matrix_to_markdown,
    summarize_plan_secrets_rotation_readiness_matrix,
)


def test_secret_rotation_tasks_emit_readiness_rows_with_required_signals():
    result = build_plan_secrets_rotation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-rotate-api-key",
                    title="Rotate billing API key with overlap window",
                    description=(
                        "Inventory Stripe API keys in vault, rotate the token for billing workers, "
                        "and assess downstream service dependency impact."
                    ),
                    depends_on=["task-vault-sync"],
                    acceptance_criteria=[
                        "Security owner: Platform Security.",
                        "Use dual-read for old and new keys during the overlap grace period.",
                        "Rollout order is canary worker, batch jobs, then webhook consumers.",
                        "Verification includes smoke tests, health checks, monitors, and alerts.",
                        "Rollback restores the previous key and records audit evidence in the change ticket.",
                    ],
                    metadata={"owner": "Platform Security"},
                ),
                _task(
                    "task-copy",
                    title="Refresh settings copy",
                    description="Update labels on the settings page.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanSecretsRotationReadinessMatrix)
    assert all(isinstance(row, PlanSecretsRotationReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-secrets"
    assert result.rotation_task_ids == ("task-rotate-api-key",)
    assert result.no_rotation_task_ids == ("task-copy",)
    assert len(result.rows) == 1

    row = result.rows[0]
    assert row.secret_inventory == "present"
    assert row.rotation_owner == "present"
    assert row.dependency_impact == "present"
    assert row.overlap_window == "present"
    assert row.rollout_order == "present"
    assert row.verification == "present"
    assert row.rollback == "present"
    assert row.audit_evidence == "present"
    assert row.gaps == ()
    assert row.readiness == "ready"
    assert any("Rotate billing API key" in item for item in row.evidence)


def test_missing_owner_or_verification_is_flagged_as_blocking_gap():
    result = build_plan_secrets_rotation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-missing-owner",
                    title="Rotate customer credential",
                    description="Rotate the customer credential after inventorying vault secrets.",
                    acceptance_criteria=[
                        "Dependency impact covers API clients.",
                        "Verification uses smoke tests.",
                        "Rollback restores old credential and audit evidence is retained.",
                    ],
                ),
                _task(
                    "task-missing-verification",
                    title="Rotate webhook token",
                    description="Security owner rotates the webhook token with rollout order by service.",
                    acceptance_criteria=[
                        "Inventory all webhook tokens.",
                        "Dual-read overlap window keeps old and new tokens active.",
                        "Rollback restores the previous token and audit evidence is captured.",
                    ],
                ),
            ]
        )
    )

    owner_gap = _row(result, "task-missing-owner")
    assert owner_gap.readiness == "blocked"
    assert "Missing rotation owner." in owner_gap.gaps
    assert owner_gap.verification == "present"

    verification_gap = _row(result, "task-missing-verification")
    assert verification_gap.readiness == "blocked"
    assert "Missing verification." in verification_gap.gaps
    assert verification_gap.rotation_owner == "present"
    assert result.summary["readiness_counts"] == {"blocked": 2, "partial": 0, "ready": 0}


def test_partial_readiness_model_input_and_stable_ordering():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Rotate service secret",
                    description="Inventory service secrets and rotate key material in KMS.",
                    acceptance_criteria=[
                        "Owner: SRE.",
                        "Dependency impact, overlap window, rollout order, verification, rollback, and audit evidence are documented.",
                    ],
                ),
                _task(
                    "task-partial",
                    title="Rotate internal token",
                    description="Inventory internal token values and rotate them for one integration.",
                    acceptance_criteria=[
                        "Owner: Security.",
                        "Verification will validate new credentials.",
                    ],
                ),
            ]
        )
    )

    result = build_plan_secrets_rotation_readiness_matrix(plan)

    assert [row.task_id for row in result.rows] == ["task-partial", "task-ready"]
    assert _row(result, "task-partial").readiness == "partial"
    assert _row(result, "task-ready").readiness == "ready"


def test_serialization_aliases_markdown_empty_invalid_object_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-token | rotate",
                title="Token | rotation",
                description="Inventory tokens and rotate token credentials.",
                acceptance_criteria=[
                    "Owner: Security.",
                    "Dependency impact, dual-read overlap window, rollout order, verification, rollback, and audit evidence are ready.",
                ],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_secrets_rotation_readiness_matrix(plan)
    payload = plan_secrets_rotation_readiness_matrix_to_dict(result)
    markdown = plan_secrets_rotation_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_secrets_rotation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_secrets_rotation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_secrets_rotation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_secrets_rotation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_secrets_rotation_readiness_matrix(result) == result.summary
    assert plan_secrets_rotation_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_secrets_rotation_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "rotation_task_ids",
        "no_rotation_task_ids",
        "summary",
    ]
    assert "Token \\| rotation" in markdown
    assert "task-token \\| rotate" in markdown

    empty = build_plan_secrets_rotation_readiness_matrix({"id": "empty-secrets", "tasks": []})
    invalid = build_plan_secrets_rotation_readiness_matrix(23)
    object_result = build_plan_secrets_rotation_readiness_matrix(
        SimpleNamespace(
            id="object-task",
            title="Rotate object secret",
            description="Owner rotates secret after inventory.",
            acceptance_criteria=["Verification, rollback, dependency impact, overlap window, rollout order, and audit evidence are ready."],
        )
    )

    assert empty.to_dict()["rows"] == []
    assert "No secrets rotation readiness rows were inferred." in empty.to_markdown()
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert object_result.rows[0].task_id == "object-task"


def _row(result, task_id):
    return next(row for row in result.rows if row.task_id == task_id)


def _plan(tasks, *, plan_id="plan-secrets"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-secrets",
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
