import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_account_deletion_readiness import (
    TaskAccountDeletionReadinessPlan,
    TaskAccountDeletionReadinessRecord,
    analyze_task_account_deletion_readiness,
    build_task_account_deletion_readiness,
    build_task_account_deletion_readiness_plan,
    derive_task_account_deletion_readiness,
    extract_task_account_deletion_readiness,
    generate_task_account_deletion_readiness,
    recommend_task_account_deletion_readiness,
    summarize_task_account_deletion_readiness,
    task_account_deletion_readiness_plan_to_dict,
    task_account_deletion_readiness_plan_to_dicts,
    task_account_deletion_readiness_plan_to_markdown,
)


def test_strong_account_deletion_readiness_detects_all_signals_and_safeguards():
    result = build_task_account_deletion_readiness_plan(
        _plan(
            [
                _task(
                    "task-delete-ready",
                    title="Implement account deletion and erasure workflow",
                    description=(
                        "Delete user accounts, erase profile PII, anonymize analytics, enqueue a deletion "
                        "queue purge job, and propagate processor deletion requests."
                    ),
                    files_or_modules=[
                        "src/accounts/account_deletion_flow.py",
                        "src/workers/deletion_queue_worker.py",
                        "src/privacy/processor_erasure.py",
                    ],
                    acceptance_criteria=[
                        "Irreversible confirmation requires the user to type DELETE before final erasure.",
                        "Restore window handling keeps accounts recoverable for 30 days before purge.",
                        "Background purge job removes data after the restore window closes.",
                        "Downstream processor propagation sends deletion requests to Stripe and analytics.",
                        "Audit logs record retention exceptions for legal hold and compliance.",
                        "Customer notification emails confirm scheduled and completed deletion.",
                        "Validation coverage includes unit and integration tests for restore and purge paths.",
                    ],
                    validation_commands={
                        "test": ["poetry run pytest tests/privacy/test_account_deletion_purge.py"]
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskAccountDeletionReadinessPlan)
    assert result.impacted_task_ids == ("task-delete-ready",)
    record = result.records[0]
    assert isinstance(record, TaskAccountDeletionReadinessRecord)
    assert record.matched_signals == (
        "account_deletion",
        "erasure",
        "anonymization",
        "retention_exception",
        "deletion_queue",
        "processor_deletion",
        "restore_window",
        "audit_log",
    )
    assert record.present_safeguards == (
        "irreversible_confirmation",
        "restore_window_handling",
        "background_purge_job",
        "downstream_processor_propagation",
        "audit_retention_exception_handling",
        "customer_notification",
        "validation_coverage",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "strong"
    assert record.recommended_checks == ()
    assert any("files_or_modules: src/workers/deletion_queue_worker.py" in item for item in record.evidence)
    assert any("validation_commands:" in item and "account_deletion_purge" in item for item in record.evidence)
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["signal_counts"]["processor_deletion"] == 1


def test_partial_readiness_uses_metadata_validation_commands_and_only_recommends_missing_checks():
    result = analyze_task_account_deletion_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add user erasure endpoint",
                    description="Allow users to request account deletion.",
                    metadata={
                        "account_deletion": {
                            "irreversible_confirmation": "Require typed confirmation before delete.",
                            "customer_notification": "Email user when deletion is scheduled.",
                            "validation_commands": {
                                "test": ["pytest tests/accounts/test_delete_account_confirmation.py"]
                            },
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "irreversible_confirmation",
        "customer_notification",
        "validation_coverage",
    )
    assert record.missing_safeguards == (
        "restore_window_handling",
        "background_purge_job",
        "downstream_processor_propagation",
        "audit_retention_exception_handling",
    )
    assert record.readiness_level == "partial"
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    assert all("typed confirmation" not in check.lower() for check in record.recommended_checks)
    assert any("metadata.account_deletion.irreversible_confirmation" in item for item in record.evidence)
    assert any("validation_commands:" in item for item in record.evidence)


def test_weak_readiness_detects_paths_and_reports_all_missing_safeguards():
    result = build_task_account_deletion_readiness(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Wire privacy cleanup worker",
                    description="Cleanup deleted account records.",
                    files_or_modules=[
                        "src/privacy/account_deletion.py",
                        "src/jobs/processor_delete_queue.py",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"account_deletion", "deletion_queue", "processor_deletion"} <= set(record.matched_signals)
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "irreversible_confirmation",
        "restore_window_handling",
        "background_purge_job",
        "downstream_processor_propagation",
        "audit_retention_exception_handling",
        "customer_notification",
        "validation_coverage",
    )
    assert record.readiness_level == "weak"
    assert result.summary["missing_safeguard_counts"]["background_purge_job"] == 1


def test_no_impact_invalid_empty_iterable_and_model_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Delete account audit trail",
        description="Record audit logs and retention exception approvals for erasure requests.",
        acceptance_criteria=["Customer notification is sent after deletion completes."],
        validation_commands={"test": ["pytest tests/test_account_delete_audit.py"]},
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Restore window for account erasure",
            description="Add restore window handling before permanent delete.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_account_deletion_readiness_plan([object_task])
    task_result = generate_task_account_deletion_readiness(task_model)
    plan_result = extract_task_account_deletion_readiness(plan_model)
    invalid = build_task_account_deletion_readiness_plan(17)
    empty = build_task_account_deletion_readiness_plan({"id": "empty-plan", "tasks": []})
    no_impact = build_task_account_deletion_readiness_plan(
        _plan([_task("task-copy", title="Update settings copy", description="Change labels only.")])
    )

    assert iterable_result.records[0].task_id == "task-object"
    assert "audit_retention_exception_handling" in iterable_result.records[0].present_safeguards
    assert task_result.records[0].task_id == "task-model"
    assert plan_result.plan_id == "plan-model"
    assert invalid.records == ()
    assert empty.plan_id == "empty-plan"
    assert no_impact.records == ()
    assert no_impact.impacted_task_ids == ()
    assert no_impact.no_impact_task_ids == ("task-copy",)
    assert "No task account deletion readiness records were inferred." in no_impact.to_markdown()
    assert "No-impact tasks: task-copy" in no_impact.to_markdown()


def test_serialization_markdown_aliases_sorting_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Deletion ready | strong",
                description="Account deletion includes restore window and processor deletion.",
                acceptance_criteria=[
                    "Irreversible confirmation is required.",
                    "Restore window handling is documented.",
                    "Background purge job runs after retention expires.",
                    "Downstream processor propagation is verified.",
                    "Audit logs capture retention exceptions.",
                    "Customer notification is sent.",
                    "Validation coverage includes deletion tests.",
                ],
            ),
            _task(
                "task-a",
                title="Erase account data",
                description="Account erasure endpoint with typed confirmation.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_account_deletion_readiness(plan)
    payload = task_account_deletion_readiness_plan_to_dict(result)
    markdown = task_account_deletion_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_account_deletion_readiness_plan_to_dicts(result) == payload["records"]
    assert task_account_deletion_readiness_plan_to_dicts(result.records) == payload["records"]
    assert derive_task_account_deletion_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_account_deletion_readiness(plan).to_dict() == result.to_dict()
    assert result.impacted_task_ids == ("task-a", "task-z")
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.readiness_level for record in result.records] == ["partial", "strong"]
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Account Deletion Readiness: plan-account-deletion")
    assert "Deletion ready \\| strong" in markdown
    assert "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


def _plan(tasks, *, plan_id="plan-account-deletion"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-account-deletion",
        "target_engine": "codex",
        "target_repo": "blueprint",
        "project_type": "python",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
    validation_commands=None,
    validation_plan=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "metadata": {} if metadata is None else metadata,
    }
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    if validation_plan is not None:
        task["validation_plan"] = validation_plan
    return task
