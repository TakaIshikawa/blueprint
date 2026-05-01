import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_transaction_safety import (
    TaskTransactionSafetyPlan,
    generate_task_transaction_safety_plans,
    task_transaction_safety_plans_to_dicts,
)


def test_transaction_lock_isolation_deadlock_and_rollback_signals_are_detected():
    records = generate_task_transaction_safety_plans(
        _plan(
            [
                _task(
                    "task-tx",
                    title="Add transactional row lock for invoice settlement",
                    description=(
                        "Use SELECT FOR UPDATE inside a transaction with serializable isolation, "
                        "deadlock retry handling, consistency invariants, and rollback behavior."
                    ),
                    files_or_modules=["src/db/invoice_transactions.py"],
                    acceptance_criteria=[
                        "Concurrent settlement preserves ledger consistency after rollback.",
                    ],
                    test_command="poetry run pytest tests/db/test_invoice_transactions.py",
                ),
                _task(
                    "task-copy",
                    title="Update billing labels",
                    description="Copy-only UI text change.",
                ),
            ]
        )
    )

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TaskTransactionSafetyPlan)
    assert record.task_id == "task-tx"
    assert record.transaction_risks == (
        "transaction",
        "row_lock",
        "isolation_level",
        "deadlock",
        "consistency",
        "rollback",
    )
    assert record.write_profile == "high_risk_write"
    assert any("lock timeout" in safeguard.lower() for safeguard in record.required_safeguards)
    assert any("rollback" in condition.lower() for condition in record.stop_conditions)
    assert (
        "Run database transaction validation command: poetry run pytest tests/db/test_invoice_transactions.py"
        in record.validation_evidence
    )
    assert "files_or_modules: src/db/invoice_transactions.py" in record.evidence


def test_bulk_and_idempotent_writes_get_stricter_safeguards_than_read_only_database_tasks():
    records = generate_task_transaction_safety_plans(
        _plan(
            [
                _task(
                    "task-read",
                    title="Tune read-only database report query",
                    description="Optimize SELECT query on a read-only replica for account reports.",
                    files_or_modules=["src/repository/report_queries.py"],
                ),
                _task(
                    "task-bulk",
                    title="Backfill idempotent writes for customer balances",
                    description=(
                        "Run a bulk update with upsert guards and idempotency key handling "
                        "so retry-safe writes preserve balance consistency."
                    ),
                    files_or_modules=["src/db/backfills/customer_balance_backfill.py"],
                    metadata={
                        "validation_commands": {
                            "test": ["poetry run pytest tests/persistence/test_balance_backfill.py"]
                        }
                    },
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in records}
    assert [record.task_id for record in records] == ["task-bulk", "task-read"]
    assert by_id["task-bulk"].transaction_risks == (
        "consistency",
        "bulk_write",
        "idempotent_write",
    )
    assert by_id["task-bulk"].write_profile == "high_risk_write"
    assert any("Dry-run" in safeguard for safeguard in by_id["task-bulk"].required_safeguards)
    assert any("idempotency key" in safeguard for safeguard in by_id["task-bulk"].required_safeguards)
    assert any("row counts" in item for item in by_id["task-bulk"].validation_evidence)

    assert by_id["task-read"].transaction_risks == ("read_only_database",)
    assert by_id["task-read"].write_profile == "read_only"
    assert len(by_id["task-read"].required_safeguards) < len(by_id["task-bulk"].required_safeguards)
    assert any("remains read-only" in safeguard for safeguard in by_id["task-read"].required_safeguards)
    assert any("introduces writes" in condition for condition in by_id["task-read"].stop_conditions)


def test_model_task_input_matches_mapping_input_without_mutation_and_serializes():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Change database transaction behavior",
                description="Update transaction commit semantics and rollback handling for billing exports.",
                acceptance_criteria=["Database rollback tests cover failed export writes."],
            )
        ],
        plan_id="plan-transaction-model",
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    task_model = ExecutionTask.model_validate(plan["tasks"][0])

    mapping_records = generate_task_transaction_safety_plans(plan)
    model_records = generate_task_transaction_safety_plans(model)
    direct_records = generate_task_transaction_safety_plans(task_model)
    payload = task_transaction_safety_plans_to_dicts(model_records)

    assert plan == original
    assert payload == task_transaction_safety_plans_to_dicts(mapping_records)
    assert direct_records[0].task_id == "task-model"
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "task_id",
        "title",
        "transaction_risks",
        "write_profile",
        "required_safeguards",
        "validation_evidence",
        "stop_conditions",
        "evidence",
    ]


def test_deterministic_ordering_and_deduped_evidence_across_runs():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Database transaction update",
                description="Update database transaction behavior.",
                acceptance_criteria=["Transaction behavior is tested."],
                test_command="poetry run pytest tests/db/test_transactions.py",
            ),
            _task(
                "task-a",
                title="Bulk write account records",
                description="Bulk update account records inside a transaction with rollback support.",
                files_or_modules=[
                    "src/db/backfills/accounts.py",
                    "src/db/backfills/accounts.py",
                ],
                validation_commands=["poetry run pytest tests/db/test_bulk_accounts.py"],
            ),
        ]
    )

    first = generate_task_transaction_safety_plans(plan)
    second = generate_task_transaction_safety_plans(plan)

    assert [record.task_id for record in first] == ["task-a", "task-z"]
    assert task_transaction_safety_plans_to_dicts(first) == task_transaction_safety_plans_to_dicts(second)
    assert first[0].transaction_risks == ("transaction", "rollback", "bulk_write")
    assert len(first[0].evidence) == len(set(first[0].evidence))


def test_empty_unaffected_or_malformed_plans_return_empty_list():
    assert generate_task_transaction_safety_plans(_plan([])) == []
    assert generate_task_transaction_safety_plans(
        _plan([_task("task-docs", title="Update README", description="Documentation only.")])
    ) == []
    assert generate_task_transaction_safety_plans({"tasks": "not a list"}) == []
    assert generate_task_transaction_safety_plans("not a plan") == []
    assert generate_task_transaction_safety_plans(None) == []


def _plan(tasks, *, plan_id="plan-transaction-safety"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-transaction-safety",
        "milestones": [{"name": "Transaction Safety"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
    validation_commands=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": (
            ["Behavior is validated."]
            if acceptance_criteria is None
            else acceptance_criteria
        ),
        "metadata": {} if metadata is None else metadata,
    }
    if test_command is not None:
        task["test_command"] = test_command
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
