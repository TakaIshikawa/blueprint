import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_api_key_lifecycle_readiness import (
    TaskAPIKeyLifecycleReadinessPlan,
    analyze_task_api_key_lifecycle_readiness,
    build_task_api_key_lifecycle_readiness_plan,
    recommend_task_api_key_lifecycle_readiness,
    summarize_task_api_key_lifecycle_readiness,
    summarize_task_api_key_lifecycle_readiness_plan,
    task_api_key_lifecycle_readiness_plan_to_dict,
    task_api_key_lifecycle_readiness_plan_to_dicts,
    task_api_key_lifecycle_readiness_plan_to_markdown,
)


def test_complete_api_key_lifecycle_task_is_ready():
    result = build_task_api_key_lifecycle_readiness_plan(
        _plan(
            [
                _task(
                    "keys-ready",
                    title="Add API key lifecycle management",
                    description="Support API key creation, rotation, revocation, expiration, scopes, storage, and audit events.",
                    acceptance_criteria=[
                        "Lifecycle state model covers created, active, rotating, revoked, disabled, and expired states.",
                        "Secure storage uses hashed key digests in the secrets manager with no plaintext storage.",
                        "Scope permission handling enforces least privilege permissions for each key.",
                        "Rotation and revocation path supports dual key overlap, cutover, disable, and revoke actions.",
                        "Audit logging records creation event, rotation event, revocation event, usage, and access events.",
                        "Customer communication includes developer notice, migration guide, release notes, and docs update.",
                        "Validation coverage includes unit tests, integration tests, contract tests, and smoke tests.",
                    ],
                    files_or_modules=["src/api/api_key_lifecycle.py"],
                ),
                _task("copy", title="Update copy", description="Refresh settings copy."),
            ]
        )
    )

    assert isinstance(result, TaskAPIKeyLifecycleReadinessPlan)
    assert result.impacted_task_ids == ("keys-ready",)
    assert result.ignored_task_ids == ("copy",)
    record = result.records[0]
    assert record.detected_signals == (
        "api_key_lifecycle",
        "key_creation",
        "key_rotation",
        "key_revocation",
        "key_expiration",
        "key_scopes",
        "key_storage",
        "key_audit",
    )
    assert record.present_criteria == (
        "lifecycle_state_model",
        "secure_storage",
        "scope_permission_handling",
        "rotation_revocation_path",
        "audit_logging",
        "customer_communication",
        "validation_coverage",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_api_key_lifecycle_task_reports_deterministic_actionable_gaps():
    result = analyze_task_api_key_lifecycle_readiness(
        [
            _task(
                "keys-partial",
                title="Rotate API keys",
                description="Rotate API keys and emit audit events.",
            )
        ]
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("rotation_revocation_path", "audit_logging")
    assert record.missing_criteria == (
        "lifecycle_state_model",
        "secure_storage",
        "scope_permission_handling",
        "customer_communication",
        "validation_coverage",
    )
    assert record.recommended_follow_up_actions == (
        "Define the lifecycle state model, including created, active, rotating, revoked, disabled, expired, or equivalent state transitions.",
        "Document secure storage for API keys with hashing, encryption, a vault, secrets manager, KMS, or no plaintext storage.",
        "Add scope or permission handling with least privilege, RBAC, access policies, entitlements, or scoped keys.",
        "Plan customer communication for affected clients, consumers, developers, migration guides, notices, release notes, or documentation updates.",
        "Add validation coverage with unit, integration, contract, smoke, regression, pytest, or acceptance checks.",
    )


def test_file_path_hints_and_nested_metadata_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "keys-paths",
                title="Credential endpoints",
                description="Add client migration support for credential lifecycle work.",
                files_or_modules=[
                    "src/api/key_rotation.py",
                    "src/api/key_revocation.py",
                    "src/auth/key_scopes.py",
                    "infra/credential_storage.tf",
                ],
                metadata={
                    "security": {
                        "storage": "Hashing with KMS encryption protects key digests.",
                        "owner": "Platform validates the active and revoked states with pytest checks.",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_api_key_lifecycle_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == (
        "api_key_lifecycle",
        "key_rotation",
        "key_revocation",
        "key_scopes",
        "key_storage",
        "client_migration",
    )
    assert record.present_criteria == (
        "lifecycle_state_model",
        "secure_storage",
        "scope_permission_handling",
        "rotation_revocation_path",
        "validation_coverage",
    )
    assert record.missing_criteria == ("audit_logging", "customer_communication")
    assert any("metadata.security.storage" in item for item in record.evidence)
    assert any("files_or_modules: src/api/key_rotation.py" in item for item in record.evidence)
    assert any("files_or_modules: src/api/key_revocation.py" in item for item in record.evidence)
    assert any("files_or_modules[2]: src/auth/key_scopes.py" in item for item in record.evidence)
    assert any("files_or_modules[3]: infra/credential_storage.tf" in item for item in record.evidence)


def test_no_impact_and_conversion_helpers_are_stable():
    result = summarize_task_api_key_lifecycle_readiness(
        _plan(
            [
                _task(
                    "keys-noop",
                    title="Docs refresh",
                    description="No API key rotation changes are required for this documentation update.",
                ),
                _task("keys-partial", title="Add key expiration", description="Add API key expiration policy."),
            ],
            plan_id="plan-api-key-lifecycle-sort",
        )
    )

    payload = task_api_key_lifecycle_readiness_plan_to_dict(result)
    markdown = task_api_key_lifecycle_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["keys-partial"]
    assert result.ignored_task_ids == ("keys-noop",)
    assert analyze_task_api_key_lifecycle_readiness(result) is result
    assert summarize_task_api_key_lifecycle_readiness_plan(result) is result
    assert recommend_task_api_key_lifecycle_readiness(result) == result.records
    assert task_api_key_lifecycle_readiness_plan_to_dicts(result) == payload["records"]
    assert task_api_key_lifecycle_readiness_plan_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-api-key-lifecycle-sort"
    assert markdown.startswith("# Task API Key Lifecycle Readiness: plan-api-key-lifecycle-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_api_key_lifecycle_readiness_plan(42).records == ()
    assert build_task_api_key_lifecycle_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_api_key_lifecycle_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-api-key-lifecycle"):
    return {"id": plan_id, "implementation_brief_id": "brief-api-key-lifecycle", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
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
    if metadata is not None:
        task["metadata"] = metadata
    return task
