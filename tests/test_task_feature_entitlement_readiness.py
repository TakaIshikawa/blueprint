import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_feature_entitlement_readiness import (
    TaskFeatureEntitlementReadinessPlan,
    TaskFeatureEntitlementReadinessRecord,
    analyze_task_feature_entitlement_readiness,
    build_task_feature_entitlement_readiness_plan,
    derive_task_feature_entitlement_readiness,
    extract_task_feature_entitlement_readiness,
    generate_task_feature_entitlement_readiness,
    recommend_task_feature_entitlement_readiness,
    summarize_task_feature_entitlement_readiness,
    task_feature_entitlement_readiness_plan_to_dict,
    task_feature_entitlement_readiness_plan_to_dicts,
    task_feature_entitlement_readiness_plan_to_markdown,
    task_feature_entitlement_readiness_to_dicts,
)


def test_detects_entitlement_signals_from_task_fields_paths_metadata_and_validation_commands():
    result = build_task_feature_entitlement_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Add feature entitlement checks",
                    description="Gate the feature by pricing tier and check entitlement before access.",
                    files_or_modules=[
                        "src/entitlements/plan_gate.py",
                        "src/entitlements/seat_limit.py",
                        "src/entitlements/usage_limit.py",
                        "src/entitlements/trial_access.py",
                        "tests/entitlements/cache_invalidation_tests.py",
                    ],
                    acceptance_criteria=[
                        "Grandfathered account behavior uses the entitlement matrix.",
                        "Migration tests cover legacy entitlement rows.",
                        "Downgrade tests cover revoke access and read-only mode.",
                    ],
                    metadata={
                        "admin_override": "Admin override requires an override audit log.",
                        "permission_sync": "Permission sync propagates the entitlement change.",
                    },
                    validation_commands={
                        "entitlements": [
                            "pytest tests/entitlements/test_support_playbook.py",
                            "pytest tests/entitlements/test_downgrade_behavior.py",
                        ]
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskFeatureEntitlementReadinessPlan)
    record = result.records[0]
    assert isinstance(record, TaskFeatureEntitlementReadinessRecord)
    assert record.detected_signals == (
        "plan_gate",
        "entitlement_check",
        "seat_limit",
        "usage_limit",
        "grandfathered_account",
        "admin_override",
        "downgrade_behavior",
        "trial_access",
        "permission_sync",
    )
    assert record.present_safeguards == (
        "entitlement_matrix",
        "migration_tests",
        "downgrade_tests",
        "override_audit_log",
        "cache_invalidation_tests",
        "support_playbook",
    )
    assert record.missing_safeguards == ()
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert any("files_or_modules:" in item for item in record.evidence)
    assert any("metadata.admin_override" in item for item in record.evidence)
    assert any("validation_commands:" in item for item in record.evidence)


def test_high_medium_and_low_impact_examples_and_recommended_checks_are_inferred():
    result = analyze_task_feature_entitlement_readiness(
        _plan(
            [
                _task(
                    "task-high",
                    title="Launch plan gate",
                    description="Plan gate and entitlement check customer access for paid tiers.",
                ),
                _task(
                    "task-medium",
                    title="Seat limit downgrade behavior",
                    description="Seat limit and downgrade behavior apply when a team changes plan.",
                    acceptance_criteria=[
                        "Entitlement matrix is documented.",
                        "Migration tests cover existing teams.",
                        "Downgrade tests cover limit exceeded states.",
                        "Override audit log records manual exceptions.",
                        "Cache invalidation tests refresh entitlement state.",
                    ],
                ),
                _task(
                    "task-low",
                    title="Sync internal permissions",
                    description="Permission sync updates RBAC state after admin override.",
                    acceptance_criteria=[
                        "Entitlement matrix, migration tests, downgrade tests, override audit log, "
                        "cache invalidation tests, and support playbook are complete.",
                    ],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-high"].impact == "high"
    assert by_id["task-high"].readiness == "weak"
    assert by_id["task-high"].missing_safeguards == (
        "entitlement_matrix",
        "migration_tests",
        "downgrade_tests",
        "override_audit_log",
        "cache_invalidation_tests",
        "support_playbook",
    )
    assert by_id["task-high"].recommended_checks[0].startswith("Document the plan")
    assert by_id["task-high"].recommendations == by_id["task-high"].recommended_checks
    assert by_id["task-high"].recommended_actions == by_id["task-high"].recommended_checks
    assert by_id["task-medium"].impact == "medium"
    assert by_id["task-medium"].readiness == "partial"
    assert by_id["task-medium"].missing_safeguards == ("support_playbook",)
    assert by_id["task-low"].impact == "low"
    assert by_id["task-low"].readiness == "strong"
    assert result.entitlement_task_ids == ("task-high", "task-medium", "task-low")
    assert result.impacted_task_ids == result.entitlement_task_ids
    assert result.summary["readiness_counts"] == {"weak": 1, "partial": 1, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["missing_safeguard_counts"]["support_playbook"] == 2


def test_no_impact_empty_invalid_markdown_and_summary_are_stable():
    result = build_task_feature_entitlement_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update settings copy", description="Text only."),
                _task(
                    "task-entitlement",
                    title="Trial access update",
                    description="Trial access uses an entitlement matrix.",
                ),
            ]
        )
    )
    empty = build_task_feature_entitlement_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_feature_entitlement_readiness_plan(13)
    no_signal = build_task_feature_entitlement_readiness_plan(
        _plan([_task("task-copy", title="Update helper copy", description="Static text only.")])
    )

    assert result.entitlement_task_ids == ("task-entitlement",)
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["no_impact_task_ids"] == ["task-copy"]
    assert empty.records == ()
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.no_impact_task_ids == ("task-copy",)
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Feature Entitlement Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Entitlement task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task feature entitlement readiness records were inferred.",
        ]
    )
    assert "No-impact tasks: task-copy" in no_signal.to_markdown()


def test_model_objects_serialization_markdown_aliases_and_no_source_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add usage limit support view",
        description="Usage limit support playbook is needed for agents.",
        files_or_modules=["src/entitlements/usage_limit_support_playbook.py"],
        acceptance_criteria=["Override audit log records admin override events."],
        metadata={"cache_invalidation_tests": "Refresh entitlement cache after usage changes."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Plan gate | entitlement check",
            description="Plan gate uses entitlement check for paid plan access.",
            acceptance_criteria=["Entitlement matrix documents access."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Grandfather legacy plan",
                description="Grandfathered account access is preserved during entitlement migration.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_feature_entitlement_readiness(plan)
    object_result = build_task_feature_entitlement_readiness_plan([object_task])
    model_result = generate_task_feature_entitlement_readiness(ExecutionPlan.model_validate(plan))
    payload = task_feature_entitlement_readiness_plan_to_dict(result)
    markdown = task_feature_entitlement_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskFeatureEntitlementReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_feature_entitlement_readiness_plan_to_dicts(result) == payload["records"]
    assert task_feature_entitlement_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_feature_entitlement_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_feature_entitlement_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_feature_entitlement_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_feature_entitlement_readiness(plan).to_dict() == result.to_dict()
    assert analyze_task_feature_entitlement_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "entitlement_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "impact",
        "recommended_checks",
        "evidence",
    ]
    assert result.entitlement_task_ids == ("task-model", "task-a")
    assert result.no_impact_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Feature Entitlement Readiness: plan-serialization")
    assert "Plan gate \\| entitlement check" in markdown
    assert (
        "| Task | Title | Readiness | Impact | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-feature-entitlement"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-feature-entitlement",
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
    tags=None,
    metadata=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-feature-entitlement",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
