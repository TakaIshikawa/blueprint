import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_dependency_waivers import (
    DependencyWaiverRecord,
    PlanDependencyWaiverRegister,
    build_plan_dependency_waiver_register,
    plan_dependency_waiver_register_to_dict,
    plan_dependency_waiver_register_to_markdown,
    summarize_plan_dependency_waivers,
)


def test_explicit_metadata_waivers_are_normalized_with_evidence():
    result = build_plan_dependency_waiver_register(
        _plan(
            [
                _task(
                    "task-api",
                    metadata={
                        "dependency_waivers": [
                            {
                                "type": "dependency_exception",
                                "reason": (
                                    "Schema dependency exception accepted while the "
                                    "API contract is finalized."
                                ),
                                "expires": "2099-01-01",
                                "status": "approved",
                            }
                        ],
                        "manual_sequencing": {
                            "reason": "Manual sequencing required after task-schema lands.",
                            "until": "task-schema completes",
                            "status": "active",
                        },
                    },
                )
            ]
        )
    )

    assert result.plan_id == "plan-waivers"
    assert result.waivers == (
        DependencyWaiverRecord(
            task_id="task-api",
            waiver_type="dependency_exception",
            reason=(
                "Schema dependency exception accepted while the API contract is finalized."
            ),
            expiry_signal="2099-01-01",
            status="active",
            evidence=(
                "metadata.dependency_waivers: Schema dependency exception accepted "
                "while the API contract is finalized.",
                "metadata.dependency_waivers.status: approved",
                "metadata.dependency_waivers.expires: 2099-01-01",
            ),
        ),
        DependencyWaiverRecord(
            task_id="task-api",
            waiver_type="manual_sequencing",
            reason="Manual sequencing required after task-schema lands.",
            expiry_signal="task-schema completes",
            status="active",
            evidence=(
                "metadata.manual_sequencing: Manual sequencing required after task-schema lands.",
                "metadata.manual_sequencing.status: active",
                "metadata.manual_sequencing.until: task-schema completes",
            ),
        ),
    )
    assert result.summary == {
        "total": 2,
        "active": 2,
        "expired": 0,
        "unresolved": 0,
        "waiver_type_counts": {
            "dependency_waiver": 0,
            "dependency_exception": 1,
            "dependency_override": 0,
            "manual_sequencing": 1,
            "missing_dependency_acceptance": 0,
            "blocked_reason": 0,
        },
    }


def test_inferred_text_waivers_from_description_blocked_reason_and_acceptance():
    result = build_plan_dependency_waiver_register(
        _plan(
            [
                _task(
                    "task-ui",
                    description=(
                        "Override dependency on task-api until mocked endpoint is removed."
                    ),
                    blocked_reason=(
                        "Dependency waiver accepted until product signs off on ordering."
                    ),
                    acceptance_criteria=[
                        "Missing dependency acceptance documented before autonomous dispatch.",
                    ],
                )
            ]
        )
    )

    assert [
        (waiver.waiver_type, waiver.status, waiver.expiry_signal)
        for waiver in result.waivers
    ] == [
        ("dependency_override", "active", "mocked endpoint is removed"),
        ("missing_dependency_acceptance", "unresolved", "unspecified"),
        ("blocked_reason", "active", "product signs off on ordering"),
    ]
    assert result.summary["total"] == 3
    assert result.summary["active"] == 2
    assert result.summary["unresolved"] == 1
    assert result.waivers[0].evidence == (
        "description: Override dependency on task-api until mocked endpoint is removed.",
    )


def test_expired_waivers_are_counted_from_status_and_past_dates():
    result = build_plan_dependency_waiver_register(
        _plan(
            [
                _task(
                    "task-data",
                    metadata={
                        "dependency_override": {
                            "reason": "Bypass upstream dependency until 2000-01-01.",
                            "status": "active",
                        },
                        "dependency_exception": {
                            "reason": "Temporary dependency exception for the old ETL.",
                            "expiry_signal": "review after fixture migration",
                            "status": "expired",
                        },
                    },
                )
            ]
        )
    )

    assert [waiver.status for waiver in result.waivers] == ["expired", "expired"]
    assert result.summary["expired"] == 2
    assert result.summary["active"] == 0
    assert result.summary["unresolved"] == 0


def test_empty_plan_has_no_waivers_and_stable_markdown():
    result = build_plan_dependency_waiver_register({"id": "plan-empty", "tasks": []})

    assert result.waivers == ()
    assert result.summary == {
        "total": 0,
        "active": 0,
        "expired": 0,
        "unresolved": 0,
        "waiver_type_counts": {
            "dependency_waiver": 0,
            "dependency_exception": 0,
            "dependency_override": 0,
            "manual_sequencing": 0,
            "missing_dependency_acceptance": 0,
            "blocked_reason": 0,
        },
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Dependency Waivers: plan-empty",
            "",
            "No dependency waivers were found.",
        ]
    )


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-worker",
                    metadata={
                        "waiver": {
                            "reason": "Dependency waiver accepted until task-api publishes stubs.",
                            "status": "accepted",
                        }
                    },
                )
            ]
        )
    )

    result = summarize_plan_dependency_waivers(plan)
    payload = plan_dependency_waiver_register_to_dict(result)

    assert isinstance(result, PlanDependencyWaiverRegister)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["waivers"]
    assert list(payload) == ["plan_id", "waivers", "summary"]
    assert list(payload["waivers"][0]) == [
        "task_id",
        "waiver_type",
        "reason",
        "expiry_signal",
        "status",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_dependency_waiver_register_to_markdown(result) == "\n".join(
        [
            "# Plan Dependency Waivers: plan-waivers",
            "",
            "| Task | Type | Status | Expiry Signal | Reason | Evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            "| task-worker | dependency_waiver | active | task-api publishes stubs | "
            "Dependency waiver accepted until task-api publishes stubs. | "
            "metadata.waiver: Dependency waiver accepted until task-api publishes stubs.; "
            "metadata.waiver.status: accepted |",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-waivers",
        "implementation_brief_id": "brief-waivers",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    description=None,
    blocked_reason=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
    if blocked_reason is not None:
        task["blocked_reason"] = blocked_reason
    return task
