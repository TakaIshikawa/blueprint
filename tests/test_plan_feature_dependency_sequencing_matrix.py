import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_feature_dependency_sequencing_matrix import (
    PlanFeatureDependencySequencingMatrix,
    PlanFeatureDependencySequencingRow,
    build_plan_feature_dependency_sequencing_matrix,
    plan_feature_dependency_sequencing_matrix_to_dict,
    plan_feature_dependency_sequencing_matrix_to_markdown,
)


def test_explicit_dependencies_and_text_references_are_reported():
    result = build_plan_feature_dependency_sequencing_matrix(
        _plan(
            [
                _task("task-foundation", title="Build prerequisite feature foundation"),
                _task(
                    "task-api",
                    title="Build API",
                    description="Implement API after task-foundation.",
                    depends_on=["task-foundation"],
                ),
                _task(
                    "task-ui",
                    title="Build UI",
                    description="Blocked by task-api before the UI can render data.",
                ),
                _task(
                    "task-launch",
                    title="Launch",
                    description="Requires task-missing before release.",
                ),
            ]
        )
    )

    row = result.rows[0]

    assert isinstance(result, PlanFeatureDependencySequencingMatrix)
    assert isinstance(row, PlanFeatureDependencySequencingRow)
    assert row.category == "prerequisite_feature"
    assert row.status == "missing"
    assert row.dependency_ids == ("task-foundation",)
    assert row.affected_task_ids == ("task-foundation", "task-api", "task-ui", "task-launch")
    assert row.missing_dependency_notes == (
        "Add task-api to task-ui.depends_on.",
        "task-launch references unknown prerequisite 'task-missing'.",
    )
    assert row.recommended_ordering == (
        "task-foundation before task-api",
        "task-api before task-ui",
        "task-missing before task-launch",
    )


def test_implicit_schema_api_and_api_ui_ordering_statuses_are_deterministic():
    result = build_plan_feature_dependency_sequencing_matrix(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Add account schema",
                    files_or_modules=["src/db/account_schema.py"],
                ),
                _task(
                    "task-api",
                    title="Build account API endpoint",
                    files_or_modules=["src/api/accounts.py"],
                ),
                _task(
                    "task-ui",
                    title="Build account UI page",
                    depends_on=["task-api"],
                    files_or_modules=["src/ui/accounts.tsx"],
                ),
            ]
        )
    )

    assert [(row.category, row.status) for row in result.rows] == [
        ("prerequisite_feature", "covered"),
        ("schema_before_api", "partial"),
        ("api_before_ui", "covered"),
    ]

    schema_row = result.rows[1]
    assert schema_row.affected_task_ids == ("task-schema", "task-api")
    assert schema_row.missing_dependency_notes == (
        "Encode task-schema as a dependency of task-api before parallel execution.",
    )
    assert schema_row.recommended_ordering == ("task-schema before task-api",)

    api_ui_row = result.rows[2]
    assert api_ui_row.dependency_ids == ("task-api",)
    assert api_ui_row.recommended_ordering == ("task-api before task-ui",)


def test_migration_before_backfill_missing_when_backfill_appears_first():
    result = build_plan_feature_dependency_sequencing_matrix(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Backfill existing invoices",
                    files_or_modules=["scripts/backfill_invoices.py"],
                ),
                _task(
                    "task-migration",
                    title="Add invoice migration",
                    files_or_modules=["migrations/20260503_invoice.sql"],
                ),
            ]
        )
    )

    assert [(row.category, row.status) for row in result.rows] == [
        ("migration_before_backfill", "missing"),
    ]
    assert result.rows[0].affected_task_ids == ("task-migration", "task-backfill")
    assert result.rows[0].missing_dependency_notes == (
        "Move task-migration before task-backfill and add an explicit dependency before parallel execution.",
    )
    assert result.summary == {
        "row_count": 1,
        "covered_count": 0,
        "partial_count": 0,
        "missing_count": 1,
        "affected_task_count": 2,
    }


def test_other_dependency_categories_are_detected_in_category_order():
    result = build_plan_feature_dependency_sequencing_matrix(
        _plan(
            [
                _task("task-flag", title="Add feature flag kill switch"),
                _task("task-docs", title="Update runbook documentation"),
                _task("task-rollout", title="Rollout production launch"),
                _task("task-contract", title="Publish OpenAPI contract"),
                _task("task-integration", title="Build partner webhook integration"),
                _task("task-launch-docs", title="Launch support workflow"),
            ]
        )
    )

    assert [(row.category, row.status) for row in result.rows] == [
        ("flag_before_rollout", "partial"),
        ("contract_before_integration", "partial"),
        ("docs_before_launch", "partial"),
    ]
    assert result.rows[0].recommended_ordering == (
        "task-flag before task-rollout",
        "task-flag before task-launch-docs",
    )
    assert result.rows[1].recommended_ordering == ("task-contract before task-integration",)
    assert result.rows[2].recommended_ordering == (
        "task-docs before task-rollout",
        "task-docs before task-launch-docs",
    )


def test_no_risk_plans_return_empty_matrix_and_markdown_message():
    result = build_plan_feature_dependency_sequencing_matrix(
        _plan(
            [_task("task-copy", title="Update local copy", description="Edit static copy.")],
            plan_id="plan-empty",
        )
    )

    assert result.plan_id == "plan-empty"
    assert result.rows == ()
    assert result.summary == {
        "row_count": 0,
        "covered_count": 0,
        "partial_count": 0,
        "missing_count": 0,
        "affected_task_count": 0,
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Feature Dependency Sequencing Matrix: plan-empty",
            "",
            "## Summary",
            "",
            "- Row count: 0",
            "- Covered: 0",
            "- Partial: 0",
            "- Missing: 0",
            "",
            "No feature dependency sequencing risks were detected.",
        ]
    )


def test_dict_and_markdown_serialization_are_stable_for_model_input():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task("task-api", title="Build billing API | endpoint"),
                _task(
                    "task-ui",
                    title="Build billing UI",
                    depends_on=["task-api"],
                    acceptance_criteria=["UI | calls billing API"],
                ),
            ],
            plan_id="plan-model",
        )
    )

    result = build_plan_feature_dependency_sequencing_matrix(plan)
    payload = plan_feature_dependency_sequencing_matrix_to_dict(result)
    markdown = plan_feature_dependency_sequencing_matrix_to_markdown(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "category",
        "affected_task_ids",
        "status",
        "evidence",
        "dependency_ids",
        "missing_dependency_notes",
        "recommended_ordering",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Feature Dependency Sequencing Matrix: plan-model")
    assert "task-api before task-ui" in markdown
    assert "Build billing API \\| endpoint" not in markdown


def _plan(tasks, *, plan_id="plan-feature-dependency-sequencing"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-feature-dependency-sequencing",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web_app",
        "milestones": [{"name": "Build"}, {"name": "Launch"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
