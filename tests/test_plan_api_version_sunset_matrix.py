import json

from blueprint.plan_api_version_sunset_matrix import (
    PlanApiVersionSunsetMatrix,
    PlanApiVersionSunsetRow,
    analyze_plan_api_version_sunset_matrix,
    build_plan_api_version_sunset_matrix,
    plan_api_version_sunset_matrix_to_dict,
    plan_api_version_sunset_matrix_to_dicts,
    plan_api_version_sunset_matrix_to_markdown,
    summarize_plan_api_version_sunset_matrix,
)


def test_api_version_sunset_rows_capture_operational_readiness():
    result = build_plan_api_version_sunset_matrix(
        _plan(
            [
                _task(
                    "task-sunset-v1",
                    title="Sunset API v1 endpoint",
                    description="Deprecate the REST /v1/orders endpoint and migrate partner clients.",
                    acceptance_criteria=[
                        "Owner: API Platform.",
                        "Dependent clients and traffic are inventoried.",
                        "Compatibility window and sunset date are published.",
                        "Rollout order uses brownout, rate limit, then block.",
                        "Verification uses contract tests and traffic monitors.",
                        "Rollback extends the deadline and restores fallback routing.",
                        "Customer notice appears in docs, changelog, and email with audit evidence.",
                    ],
                ),
                _task("task-copy", title="Refresh labels", description="Update labels."),
            ]
        )
    )

    assert isinstance(result, PlanApiVersionSunsetMatrix)
    assert isinstance(result.rows[0], PlanApiVersionSunsetRow)
    assert result.sunset_task_ids == ("task-sunset-v1",)
    assert result.no_sunset_task_ids == ("task-copy",)
    assert result.rows[0].readiness == "ready"
    assert result.rows[0].customer_notice == "present"
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 1}


def test_missing_notice_or_verification_blocks_api_sunset_readiness_and_aliases_work():
    result = build_plan_api_version_sunset_matrix(
        _plan(
            [
                _task(
                    "task-v2-sunset",
                    title="Sunset API v2 webhook",
                    description="Owner retires webhook API version v2 after client migration window.",
                    acceptance_criteria=[
                        "Dependent clients are mapped.",
                        "Rollback can extend compatibility.",
                    ],
                )
            ]
        )
    )

    row = result.rows[0]
    payload = plan_api_version_sunset_matrix_to_dict(result)

    assert row.readiness == "blocked"
    assert "Missing verification." in row.gaps
    assert "Missing customer notice." in row.gaps
    assert analyze_plan_api_version_sunset_matrix(result) is result
    assert summarize_plan_api_version_sunset_matrix(result) == result.summary
    assert plan_api_version_sunset_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert "Plan API Version Sunset Matrix" in plan_api_version_sunset_matrix_to_markdown(result)


def test_unrelated_api_plan_is_all_clear():
    result = build_plan_api_version_sunset_matrix(_plan([_task("task-api", title="Add API endpoint", description="Add a new endpoint.")]))

    assert result.rows == ()
    assert result.summary["sunset_task_count"] == 0
    assert "No API version sunset rows were inferred." in result.to_markdown()


def _plan(tasks):
    return {"id": "plan-api-sunset", "implementation_brief_id": "brief-api", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
