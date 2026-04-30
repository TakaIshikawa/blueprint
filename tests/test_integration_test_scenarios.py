import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.integration_test_scenarios import (
    IntegrationTestScenario,
    IntegrationTestScenarioPlan,
    build_integration_test_scenarios,
    integration_test_scenarios_to_dict,
)


def test_multiple_brief_integration_points_create_scenarios_in_brief_order():
    result = build_integration_test_scenarios(
        _brief(
            integration_points=["Stripe webhook", "GitHub API"],
            data_requirements="Seed account fixtures and repository metadata.",
            validation_plan="Run integration smoke tests.",
        ),
        _plan(
            [
                _task(
                    "task-stripe",
                    title="Implement Stripe webhook",
                    description="Process incoming Stripe webhook events.",
                    files_or_modules=["src/webhooks/stripe.py"],
                    acceptance_criteria=["Stripe webhook creates a payment event."],
                ),
                _task(
                    "task-github",
                    title="Add GitHub API sync",
                    files_or_modules=["src/clients/github_api.py"],
                    acceptance_criteria=["GitHub API returns repository metadata."],
                ),
            ]
        ),
    )

    assert isinstance(result, IntegrationTestScenarioPlan)
    assert result.scenarios[:2] == (
        IntegrationTestScenario(
            name="Validate Stripe webhook",
            integration_point="Stripe webhook",
            impacted_task_ids=("task-stripe",),
            setup_notes=(
                "Prepare data requirements: Seed account fixtures and repository metadata.",
                "Exercise files/modules: src/webhooks/stripe.py",
                "Coordinate task outputs: task-stripe",
            ),
            assertion="Stripe webhook: Stripe webhook creates a payment event.",
            validation_type="automated",
            manual_validation_notes=("Fallback validation: Run integration smoke tests.",),
        ),
        IntegrationTestScenario(
            name="Validate GitHub API",
            integration_point="GitHub API",
            impacted_task_ids=("task-github",),
            setup_notes=(
                "Prepare data requirements: Seed account fixtures and repository metadata.",
                "Exercise files/modules: src/clients/github_api.py",
                "Coordinate task outputs: task-github",
            ),
            assertion="GitHub API: GitHub API returns repository metadata.",
            validation_type="automated",
            manual_validation_notes=("Fallback validation: Run integration smoke tests.",),
        ),
    )
    assert result.summary == "Recommended 3 integration test scenarios covering 2 impacted task(s)."


def test_task_level_importer_exporter_boundary_generates_cross_component_scenario():
    result = build_integration_test_scenarios(
        _brief(integration_points=[], data_requirements="Use a round-trip customer CSV."),
        _plan(
            [
                _task(
                    "task-importer",
                    title="Build customer importer",
                    description="Ingest customer CSV rows into normalized records.",
                    files_or_modules=["src/importers/customer_csv.py"],
                ),
                _task(
                    "task-exporter",
                    title="Build customer exporter",
                    description="Export normalized customer records to CSV.",
                    files_or_modules=["src/exporters/customer_csv.py"],
                    acceptance_criteria=["Exported CSV preserves imported customer identifiers."],
                ),
            ]
        ),
    )

    assert result.scenarios == (
        IntegrationTestScenario(
            name="Validate Importer/exporter boundary",
            integration_point="Importer/exporter boundary",
            impacted_task_ids=("task-importer", "task-exporter"),
            setup_notes=(
                "Prepare data requirements: Use a round-trip customer CSV.",
                (
                    "Exercise files/modules: src/importers/customer_csv.py, "
                    "src/exporters/customer_csv.py"
                ),
                "Coordinate task outputs: task-importer, task-exporter",
            ),
            assertion="Importer/exporter boundary: Build customer importer is complete.",
            validation_type="manual",
            manual_validation_notes=(
                "Fallback validation: manually verify Importer/exporter boundary.",
            ),
        ),
    )


def test_no_integration_signals_returns_empty_scenario_list_and_summary():
    result = build_integration_test_scenarios(
        _brief(integration_points=[], data_requirements=None),
        _plan(
            [
                _task(
                    "task-copy",
                    title="Refresh onboarding copy",
                    description="Update README prose.",
                    files_or_modules=["README.md"],
                )
            ],
            test_strategy=None,
        ),
    )

    assert result.scenarios == ()
    assert result.summary == (
        "No integration test scenarios recommended because no brief-level "
        "integration points or task-level integration signals were found."
    )


def test_duplicate_and_near_identical_scenarios_collapse_deterministically():
    result = build_integration_test_scenarios(
        _brief(
            integration_points=["Payments API", "payments api integration"],
            validation_plan="Run API contract tests.",
        ),
        _plan(
            [
                _task(
                    "task-api",
                    title="Add Payments API endpoint",
                    files_or_modules=["src/api/payments.py"],
                    acceptance_criteria=["Payments API returns created charge ids."],
                ),
                _task(
                    "task-client",
                    title="Update payments client",
                    files_or_modules=["src/clients/payments_api.py"],
                    acceptance_criteria=["Client handles Payments API charge ids."],
                ),
            ]
        ),
    )

    assert [
        (scenario.integration_point, scenario.impacted_task_ids) for scenario in result.scenarios
    ] == [
        ("Payments API", ("task-api", "task-client")),
        ("Api/client boundary", ("task-api", "task-client")),
    ]


def test_accepts_models_and_serializes_stably():
    brief_model = ImplementationBrief.model_validate(
        _brief(
            integration_points=["Warehouse API"],
            data_requirements="Use warehouse fixture data.",
            validation_plan="Run pytest tests/test_warehouse_api.py.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-warehouse",
                    title="Connect Warehouse API client",
                    files_or_modules=["src/clients/warehouse_api.py"],
                    test_command="poetry run pytest tests/test_warehouse_api.py",
                )
            ]
        )
    )

    result = build_integration_test_scenarios(brief_model, plan_model)
    payload = integration_test_scenarios_to_dict(result)

    assert payload == result.to_dict()
    assert list(payload) == ["brief_id", "plan_id", "scenarios", "summary"]
    assert list(payload["scenarios"][0]) == [
        "name",
        "integration_point",
        "impacted_task_ids",
        "setup_notes",
        "assertion",
        "validation_type",
        "manual_validation_notes",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_identical_payloads_produce_stable_ordering():
    brief = _brief(
        integration_points=["Repository API", "Audit webhook"],
        data_requirements="Seed repository and audit records.",
        validation_plan="Run regression tests.",
    )
    plan = _plan(
        [
            _task(
                "task-z",
                title="Build audit webhook handler",
                files_or_modules=["src/webhooks/audit.py"],
            ),
            _task(
                "task-a",
                title="Build repository API client",
                files_or_modules=["src/clients/repository_api.py"],
            ),
            _task(
                "task-m",
                title="Update repository API endpoint",
                files_or_modules=["src/api/repository.py"],
            ),
        ]
    )

    first = build_integration_test_scenarios(brief, plan).to_dict()
    second = build_integration_test_scenarios(brief, plan).to_dict()

    assert first == second
    assert [
        (scenario["integration_point"], scenario["impacted_task_ids"])
        for scenario in first["scenarios"]
    ] == [
        ("Repository API", ["task-a", "task-m"]),
        ("Audit webhook", ["task-z"]),
        ("Api/client boundary", ["task-a", "task-m"]),
    ]


def _brief(
    *,
    integration_points,
    data_requirements="Use representative integration fixtures.",
    validation_plan="Manual validation.",
):
    return {
        "id": "brief-integration-scenarios",
        "source_brief_id": "source-integration-scenarios",
        "title": "Integration Scenarios",
        "problem_statement": "Need cross-component validation.",
        "mvp_goal": "Recommend integration tests.",
        "scope": ["Build planner"],
        "non_goals": [],
        "assumptions": [],
        "data_requirements": data_requirements,
        "integration_points": integration_points,
        "risks": [],
        "validation_plan": validation_plan,
        "definition_of_done": ["Integration scenarios are reviewable"],
        "status": "planned",
    }


def _plan(tasks, *, test_strategy="Run focused validation."):
    return {
        "id": "plan-integration-scenarios",
        "implementation_brief_id": "brief-integration-scenarios",
        "milestones": [],
        "tasks": tasks,
        "test_strategy": test_strategy,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {title or task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or [f"{title or task_id} is complete."],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    return task
