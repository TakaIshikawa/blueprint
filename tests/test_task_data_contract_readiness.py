import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_contract_readiness import (
    TaskDataContractReadinessFinding,
    TaskDataContractReadinessPlan,
    analyze_task_data_contract_readiness,
    build_task_data_contract_readiness_plan,
    extract_task_data_contract_readiness,
    generate_task_data_contract_readiness,
    recommend_task_data_contract_readiness,
    summarize_task_data_contract_readiness,
    task_data_contract_readiness_plan_to_dict,
    task_data_contract_readiness_plan_to_dicts,
    task_data_contract_readiness_plan_to_markdown,
)


def test_complete_data_contract_task_has_no_actionable_gaps():
    result = analyze_task_data_contract_readiness(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Update stream data contract",
                    description=(
                        "Change Kafka schema registry stream contract and event contract for invoice events. "
                        "The schema owner is data platform and the producer owner approves it. "
                        "Use backward compatible schema evolution with a versioned contract. "
                        "Update producer and consumer fixtures plus golden payload examples. "
                        "Notify consumers with release notes and downstream migration timing. "
                        "Add producer contract tests and consumer contract test coverage."
                    ),
                    files_or_modules=["src/streams/invoices/schema_registry.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskDataContractReadinessPlan)
    assert result.plan_id == "plan-data-contracts"
    assert result.data_contract_task_ids == ("task-complete",)
    finding = result.findings[0]
    assert isinstance(finding, TaskDataContractReadinessFinding)
    assert finding.detected_signals == ("event_contract", "payload_contract", "stream_contract")
    assert finding.present_requirements == (
        "schema_ownership",
        "compatibility_mode",
        "fixture_updates",
        "consumer_notification",
        "contract_test_coverage",
    )
    assert finding.missing_requirements == ()
    assert finding.risk_level == "low"
    assert finding.actionable_gaps == ()
    assert any("description:" in ev for ev in finding.evidence)


def test_detects_event_payload_api_warehouse_and_stream_contracts_with_stable_gaps():
    result = build_task_data_contract_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Revise API contract",
                    description="Update API contract and response payload contract for mobile clients.",
                    acceptance_criteria=["Schema owner approves the endpoint contract."],
                    files_or_modules=["src/api/contracts/order_response.py"],
                ),
                _task(
                    "task-warehouse",
                    title="Add warehouse contract",
                    description=(
                        "Add warehouse table contract for revenue analytics with dbt contract tests "
                        "and fixture updates."
                    ),
                    files_or_modules=["warehouse/marts/revenue_contract.sql"],
                ),
                _task(
                    "task-stream",
                    title="Change event schema",
                    description=(
                        "Change event payload and topic schema for account events with backward "
                        "compatible mode."
                    ),
                    files_or_modules=["src/events/account_payload_schema.json"],
                ),
            ]
        )
    )

    assert result.data_contract_task_ids == ("task-api", "task-stream", "task-warehouse")
    by_id = {finding.task_id: finding for finding in result.findings}
    assert by_id["task-api"].detected_signals == ("payload_contract", "api_contract")
    assert by_id["task-api"].missing_requirements == (
        "compatibility_mode",
        "fixture_updates",
        "consumer_notification",
        "contract_test_coverage",
    )
    assert by_id["task-api"].risk_level == "high"
    assert by_id["task-warehouse"].detected_signals == ("warehouse_contract",)
    assert by_id["task-warehouse"].present_requirements == (
        "fixture_updates",
        "contract_test_coverage",
    )
    assert by_id["task-stream"].detected_signals == ("event_contract", "payload_contract", "stream_contract")
    assert by_id["task-stream"].missing_requirements == (
        "schema_ownership",
        "fixture_updates",
        "consumer_notification",
        "contract_test_coverage",
    )
    assert result.summary["missing_requirement_count"] == 11


def test_empty_and_no_impact_tasks_are_not_applicable():
    empty = build_task_data_contract_readiness_plan(_plan([]))
    no_impact = extract_task_data_contract_readiness(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update docs copy",
                    description="Clarify help text in the admin dashboard.",
                ),
                _task(
                    "task-no-contract",
                    title="Refactor parser",
                    description="No data contract, event contract, or schema changes are in scope.",
                ),
            ]
        )
    )

    assert empty.findings == ()
    assert empty.records == ()
    assert empty.data_contract_task_ids == ()
    assert empty.not_applicable_task_ids == ()
    assert empty.to_dicts() == []
    assert empty.summary == _empty_summary(0, [])
    assert no_impact.findings == ()
    assert no_impact.not_applicable_task_ids == ("task-copy", "task-no-contract")
    assert no_impact.summary == _empty_summary(2, ["task-copy", "task-no-contract"])
    assert "No data contract readiness findings were inferred." in no_impact.to_markdown()


def test_model_object_aliases_and_serialization_are_stable():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Update event contract",
                description=(
                    "Update event contract with schema ownership, backward compatible mode, "
                    "fixture updates, consumer notification, and contract tests."
                ),
                metadata={"event_contract": "account.created"},
            ),
            _task(
                "task-weak",
                title="Payload contract",
                description="Payload contract update for billing event.",
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    build_result = build_task_data_contract_readiness_plan(plan)
    summarize_result = summarize_task_data_contract_readiness(model)
    generate_result = generate_task_data_contract_readiness(model)
    recommend_result = recommend_task_data_contract_readiness(model)
    dict_payload = task_data_contract_readiness_plan_to_dict(generate_result)
    dicts_payload = task_data_contract_readiness_plan_to_dicts(generate_result)
    markdown = task_data_contract_readiness_plan_to_markdown(generate_result)
    task_result = build_task_data_contract_readiness_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    object_result = build_task_data_contract_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Warehouse data contract",
            description="Warehouse schema contract with schema owner.",
            files_or_modules=["warehouse/schema/contracts.yml"],
        )
    )

    assert plan == original
    assert build_result.to_dict() == summarize_result.to_dict() == generate_result.to_dict()
    assert recommend_result.to_dict() == generate_result.to_dict()
    assert dict_payload["plan_id"] == "plan-data-contracts"
    assert len(dicts_payload) == 2
    assert "# Task Data Contract Readiness: plan-data-contracts" in markdown
    assert "| Task | Title | Risk |" in markdown
    assert task_result.findings[0].task_id == "task-model"
    assert object_result.findings[0].task_id == "task-object"
    assert json.loads(json.dumps(dict_payload, sort_keys=True))["plan_id"] == "plan-data-contracts"


def test_compatibility_aliases_return_plan_instances():
    plan = _plan(
        [
            _task(
                "task-alias",
                title="API data contract",
                description="API contract with schema owner and compatibility mode.",
            )
        ]
    )

    results = [
        build_task_data_contract_readiness_plan(plan),
        analyze_task_data_contract_readiness(plan),
        summarize_task_data_contract_readiness(plan),
        extract_task_data_contract_readiness(plan),
        generate_task_data_contract_readiness(plan),
        recommend_task_data_contract_readiness(plan),
    ]

    assert all(isinstance(result, TaskDataContractReadinessPlan) for result in results)
    assert all(result.plan_id == "plan-data-contracts" for result in results)
    assert all(len(result.findings) == 1 for result in results)


def _empty_summary(total_task_count: int, not_applicable_task_ids: list[str]) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "data_contract_task_count": 0,
        "not_applicable_task_ids": not_applicable_task_ids,
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "event_contract": 0,
            "payload_contract": 0,
            "api_contract": 0,
            "warehouse_contract": 0,
            "stream_contract": 0,
        },
        "present_requirement_counts": {
            "schema_ownership": 0,
            "compatibility_mode": 0,
            "fixture_updates": 0,
            "consumer_notification": 0,
            "contract_test_coverage": 0,
        },
        "missing_requirement_counts": {
            "schema_ownership": 0,
            "compatibility_mode": 0,
            "fixture_updates": 0,
            "consumer_notification": 0,
            "contract_test_coverage": 0,
        },
    }


def _plan(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "plan-data-contracts",
        "implementation_brief_id": "brief-data-contracts",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id: str,
    *,
    title: str = "",
    description: str = "",
    files_or_modules: list[str] | None = None,
    acceptance_criteria: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "description": description or title,
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
