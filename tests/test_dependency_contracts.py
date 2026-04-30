import copy
import json

from blueprint.dependency_contracts import (
    DependencyContract,
    build_dependency_contracts,
    dependency_contracts_to_dicts,
)
from blueprint.domain.models import ExecutionPlan


def test_known_dependencies_produce_contract_for_each_task_dependency_pair():
    contracts = build_dependency_contracts(
        _plan(
            [
                _task("task-schema", "Create Settings Schema"),
                _task("task-api", "Build Settings API"),
                _task(
                    "task-ui",
                    "Render Settings UI",
                    depends_on=["task-schema", "task-api"],
                ),
            ]
        )
    )

    assert all(isinstance(contract, DependencyContract) for contract in contracts)
    assert [(contract.task_id, contract.dependency_id) for contract in contracts] == [
        ("task-ui", "task-schema"),
        ("task-ui", "task-api"),
    ]


def test_acceptance_criteria_metadata_artifacts_blocked_reasons_and_assumptions_are_points():
    contracts = build_dependency_contracts(
        _plan(
            [
                _task(
                    "task-api",
                    "Build Orders API",
                    acceptance=[
                        "Orders endpoint returns id and status",
                        "Errors use the shared envelope",
                    ],
                    metadata={
                        "expected_artifacts": {
                            "schema": "docs/orders-openapi.json",
                            "sample": "tests/fixtures/orders-response.json",
                        }
                    },
                    blocked_reason="Waiting for auth scopes from security review",
                ),
                _task(
                    "task-ui",
                    "Render Orders UI",
                    depends_on=["task-api"],
                    blocked_reason="Blocked until task-api provides stable payloads",
                    metadata={
                        "dependency_assumptions": {
                            "task-api": [
                                "task-api keeps status values stable",
                                "Unrelated database seed data is available",
                            ]
                        }
                    },
                ),
            ]
        )
    )

    assert contracts[0].to_dict() == {
        "task_id": "task-ui",
        "dependency_id": "task-api",
        "contract_points": [
            "prerequisite title: Build Orders API",
            "acceptance criteria: Orders endpoint returns id and status",
            "acceptance criteria: Errors use the shared envelope",
            "artifact: sample: tests/fixtures/orders-response.json",
            "artifact: schema: docs/orders-openapi.json",
            "blocked reason: Waiting for auth scopes from security review",
            "blocked reason: Blocked until task-api provides stable payloads",
            "dependent assumption: task-api keeps status values stable",
        ],
        "missing_contract_points": [],
        "review_required": False,
    }


def test_missing_dependency_tasks_are_represented_with_review_required():
    contracts = build_dependency_contracts(
        _plan([_task("task-ui", "Render UI", depends_on=["task-missing"])])
    )

    assert contracts[0].to_dict() == {
        "task_id": "task-ui",
        "dependency_id": "task-missing",
        "contract_points": [
            "missing dependency: task-missing is declared but no task record exists"
        ],
        "missing_contract_points": ["dependency task record"],
        "review_required": True,
    }


def test_dependencies_without_acceptance_criteria_or_artifacts_report_missing_points():
    contracts = build_dependency_contracts(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    acceptance=[],
                    metadata={"owner": "platform"},
                ),
                _task("task-ui", "Build UI", depends_on=["task-api"]),
            ]
        )
    )

    assert contracts[0].contract_points == ("prerequisite title: Build API",)
    assert contracts[0].missing_contract_points == (
        "prerequisite acceptance criteria",
        "prerequisite metadata artifacts",
    )
    assert contracts[0].review_required is True


def test_model_inputs_serialize_stably_and_do_not_mutate_input():
    plan = _plan(
        [
            _task(
                "task-api",
                "Build API",
                metadata={"artifacts": ["docs/api.md"]},
            ),
            _task("task-ui", "Build UI", depends_on=["task-api"]),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    contracts = build_dependency_contracts(model)
    payload = dependency_contracts_to_dicts(contracts)

    assert plan == original
    assert payload == [contract.to_dict() for contract in contracts]
    assert list(payload[0]) == [
        "task_id",
        "dependency_id",
        "contract_points",
        "missing_contract_points",
        "review_required",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-dependency-contracts",
        "implementation_brief_id": "brief-dependency-contracts",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    depends_on=None,
    acceptance=None,
    metadata=None,
    blocked_reason=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [f"src/blueprint/{task_id}.py"],
        "acceptance_criteria": (
            [f"{title} is complete"] if acceptance is None else acceptance
        ),
        "metadata": metadata or {},
        "blocked_reason": blocked_reason,
    }
