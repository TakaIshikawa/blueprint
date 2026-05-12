from blueprint.task_secrets_rotation_readiness import (
    build_task_secrets_rotation_readiness_plan,
    recommend_task_secrets_rotation_readiness,
    task_secrets_rotation_readiness_plan_to_dict,
)


def test_detects_secret_rotation_and_missing_readiness_criteria():
    result = build_task_secrets_rotation_readiness_plan(
        _plan(
            [
                _task(
                    "rotate-api-keys",
                    "Rotate partner API keys",
                    "Rotate client secrets and API keys for partner callbacks.",
                    files_or_modules=["infra/secrets/partner_rotation.tf"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == ("secret_rotation",)
    assert record.readiness == "needs_planning"
    assert record.missing_criteria == (
        "inventory",
        "staged_rollout",
        "rollback",
        "owner_coordination",
        "validation",
        "monitoring",
    )
    assert "inventory" in result.summary["missing_criterion_counts"]


def test_complete_rotation_covers_overlap_rollback_owners_validation_and_monitoring():
    result = build_task_secrets_rotation_readiness_plan(
        _plan(
            [
                _task(
                    "rotate-token",
                    "Token rollover",
                    "Rotate OAuth tokens after inventory maps each consumer and service owner.",
                    acceptance_criteria=[
                        "Use dual-read overlap window with old and new tokens during canary rollout.",
                        "Rollback keeps previous secret active as fallback.",
                        "Coordinate with on-call owners and notify consumer teams.",
                        "Post-rotation validation runs smoke tests.",
                        "Monitor dashboards and alerts for auth failures.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_criteria == (
        "inventory",
        "staged_rollout",
        "rollback",
        "owner_coordination",
        "validation",
        "monitoring",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_unrelated_or_negated_tasks_are_ignored_and_serialization_is_stable():
    result = build_task_secrets_rotation_readiness_plan(
        _plan(
            [
                _task("docs", "Update docs", "Document the settings screen."),
                _task("noop", "Auth copy", "No credential rotation changes are required."),
            ]
        )
    )

    assert result.records == ()
    assert result.ignored_task_ids == ("docs", "noop")
    assert recommend_task_secrets_rotation_readiness(result) == ()
    assert list(task_secrets_rotation_readiness_plan_to_dict(result)) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]


def _plan(tasks):
    return {"id": "plan-secrets", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}

