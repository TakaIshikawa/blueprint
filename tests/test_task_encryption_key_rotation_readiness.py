import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_encryption_key_rotation_readiness import (
    TaskEncryptionKeyRotationReadinessPlan,
    TaskEncryptionKeyRotationReadinessRecommendation,
    analyze_task_encryption_key_rotation_readiness,
    build_task_encryption_key_rotation_readiness_plan,
    extract_task_encryption_key_rotation_readiness,
    generate_task_encryption_key_rotation_readiness,
    recommend_task_encryption_key_rotation_readiness,
    summarize_task_encryption_key_rotation_readiness,
    task_encryption_key_rotation_readiness_plan_to_dict,
    task_encryption_key_rotation_readiness_plan_to_dicts,
    task_encryption_key_rotation_readiness_plan_to_markdown,
)


def test_detects_key_rotation_surfaces_and_missing_safeguards():
    result = build_task_encryption_key_rotation_readiness_plan(
        _plan(
            [
                _task(
                    "task-keys",
                    title="Rotate KMS signing keys for encrypted customer data",
                    description=(
                        "Rotate KMS customer managed keys, signing keys, API keys, and JWT tokens "
                        "used for database encryption."
                    ),
                    files_or_modules=[
                        "infra/kms/customer_key_rotation.tf",
                        "src/security/signing_keys.py",
                        "src/db/database_encryption.py",
                    ],
                    acceptance_criteria=["Credential migration replaces old API keys after rollout."],
                )
            ]
        )
    )

    assert isinstance(result, TaskEncryptionKeyRotationReadinessPlan)
    assert result.rotation_task_ids == ("task-keys",)
    record = result.records[0]
    assert isinstance(record, TaskEncryptionKeyRotationReadinessRecommendation)
    assert {
        "encryption",
        "kms",
        "token",
        "signing_key",
        "api_key",
        "database_encryption",
        "credential_migration",
    } <= set(record.key_surfaces)
    assert record.required_safeguards == (
        "dual_read_window",
        "staged_key_rollout",
        "rollback_key_retention",
        "rotation_audit_log",
        "data_re_encryption_job",
        "key_ownership",
        "expiry_monitoring",
        "incident_fallback",
    )
    assert "dual_read_window" in record.missing_safeguards
    assert record.risk_level == "high"
    assert any("description:" in item and "signing keys" in item for item in record.evidence)
    assert "files_or_modules: infra/kms/customer_key_rotation.tf" in record.evidence
    assert result.summary["rotation_task_count"] == 1
    assert result.summary["surface_counts"]["kms"] == 1


def test_metadata_acceptance_criteria_and_paths_detect_safeguards_and_surfaces():
    result = analyze_task_encryption_key_rotation_readiness(
        _plan(
            [
                _task(
                    "task-certs",
                    title="Renew TLS certificate and secrets rotation",
                    description="Rotate certificates and vault secrets for public edge services.",
                    files_or_modules=["ops/certificates/tls_rotation.yml", "infra/secrets/api_keys.tf"],
                    metadata={
                        "rotation": {
                            "dual_read_window": "Old and new keys remain readable during the overlap window.",
                            "rollout": "Staged key rollout starts with canary tenants.",
                            "owner": "Rotation owner is the security platform on-call.",
                        }
                    },
                    acceptance_criteria=[
                        "Rollback key retention keeps previous keys for delayed readers.",
                        "Rotation audit log records key activation and deactivation events.",
                        "Expiry monitoring alerts before certificate expiry.",
                        "Incident fallback covers break glass renewal.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"secret", "certificate", "api_key"} <= set(record.key_surfaces)
    assert record.present_safeguards == (
        "dual_read_window",
        "staged_key_rollout",
        "rollback_key_retention",
        "rotation_audit_log",
        "key_ownership",
        "expiry_monitoring",
        "incident_fallback",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "medium"
    assert any("metadata.rotation.dual_read_window" in item for item in record.evidence)
    assert any("metadata.rotation.owner" in item for item in record.evidence)


def test_complete_database_rotation_remains_medium_due_sensitive_surface():
    result = build_task_encryption_key_rotation_readiness_plan(
        _plan(
            [
                _task(
                    "task-db",
                    title="Database encryption key migration",
                    description="Rekey database encryption and migrate credentials with KMS.",
                    acceptance_criteria=[
                        "Dual-read window supports old and new keys.",
                        "Staged key rollout starts with one tenant.",
                        "Rollback key retention keeps previous keys.",
                        "Rotation audit log records every key rotation event.",
                        "Data re-encryption job rewraps existing ciphertext.",
                        "Key ownership names the database platform owner.",
                        "Expiry monitoring tracks key age and renewal alerts.",
                        "Incident fallback documents break glass restore old key steps.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"kms", "database_encryption", "credential_migration"} <= set(record.key_surfaces)
    assert record.missing_safeguards == ()
    assert record.risk_level == "medium"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["missing_safeguard_count"] == 0


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Rotate API keys | partners",
                description="Rotate partner API keys and client secrets.",
            ),
            _task(
                "task-a",
                title="Token rollout",
                description="Rotate OAuth tokens with dual-read overlap window and staged rollout.",
            ),
            _task("task-review", title="Security review", description="Generic security review before launch."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_encryption_key_rotation_readiness(plan)
    payload = task_encryption_key_rotation_readiness_plan_to_dict(result)
    markdown = task_encryption_key_rotation_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["recommendations"]
    assert task_encryption_key_rotation_readiness_plan_to_dicts(result) == payload["recommendations"]
    assert task_encryption_key_rotation_readiness_plan_to_dicts(result.records) == payload["recommendations"]
    assert extract_task_encryption_key_rotation_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_encryption_key_rotation_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_encryption_key_rotation_readiness(plan).to_dict() == result.to_dict()
    assert result.records == result.recommendations
    assert result.rotation_task_ids == ("task-z", "task-a")
    assert result.ignored_task_ids == ("task-review",)
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "records",
        "rotation_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "key_surfaces",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_follow_up_actions",
    ]
    assert [record.risk_level for record in result.records] == ["high", "medium"]
    assert markdown.startswith("# Task Encryption Key Rotation Readiness: plan-key-rotation")
    assert "Rotate API keys \\| partners" in markdown
    assert "| Task | Title | Risk | Key Surfaces | Present Safeguards | Missing Safeguards | Evidence |" in markdown


def test_execution_plan_execution_task_iterable_and_no_op_behavior():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Certificate renewal",
            description="Rotate TLS certificates with expiry monitoring before renewal.",
        )
    )
    iterable_result = build_task_encryption_key_rotation_readiness_plan([model_task])
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-token",
                    title="Token secret rotation",
                    description="Rotate refresh tokens and secrets for OAuth clients.",
                ),
                _task("task-review", title="Security review", description="Review threat model and permissions."),
            ]
        )
    )

    result = build_task_encryption_key_rotation_readiness_plan(plan)
    noop = build_task_encryption_key_rotation_readiness_plan(
        _plan([_task("task-review", title="Security review", description="Generic security review.")])
    )

    assert iterable_result.plan_id is None
    assert iterable_result.rotation_task_ids == ("task-model",)
    assert result.plan_id == "plan-key-rotation"
    assert result.rotation_task_ids == ("task-token",)
    assert result.ignored_task_ids == ("task-review",)
    assert noop.records == ()
    assert noop.rotation_task_ids == ()
    assert noop.ignored_task_ids == ("task-review",)
    assert noop.to_dicts() == []
    assert noop.summary == {
        "task_count": 1,
        "rotation_task_count": 0,
        "ignored_task_ids": ["task-review"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "surface_counts": {},
        "present_safeguard_counts": {
            "dual_read_window": 0,
            "staged_key_rollout": 0,
            "rollback_key_retention": 0,
            "rotation_audit_log": 0,
            "data_re_encryption_job": 0,
            "key_ownership": 0,
            "expiry_monitoring": 0,
            "incident_fallback": 0,
        },
        "rotation_task_ids": [],
    }
    assert "No encryption key rotation readiness recommendations" in noop.to_markdown()
    assert "Ignored tasks: task-review" in noop.to_markdown()


def _plan(tasks, plan_id="plan-key-rotation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-key-rotation",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
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
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-key-rotation",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
