import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.task_saved_payment_method_readiness import (
    TaskSavedPaymentMethodReadinessPlan,
    TaskSavedPaymentMethodReadinessRecord,
    analyze_task_saved_payment_method_readiness,
    build_task_saved_payment_method_readiness_plan,
    extract_task_saved_payment_method_readiness,
    generate_task_saved_payment_method_readiness,
    recommend_task_saved_payment_method_readiness,
    summarize_task_saved_payment_method_readiness,
    task_saved_payment_method_readiness_plan_to_dict,
    task_saved_payment_method_readiness_plan_to_dicts,
    task_saved_payment_method_readiness_plan_to_markdown,
)


def test_saved_card_without_controls_recommends_tokenization_and_pci_readiness():
    result = build_task_saved_payment_method_readiness_plan(
        _plan(
            [
                _task(
                    "task-cards",
                    title="Add saved cards to billing settings",
                    description="Customers can save cards and update payment method details.",
                    files_or_modules=["src/billing/payment_methods/saved_cards.py"],
                    acceptance_criteria=["Customer portal shows card on file updates."],
                )
            ]
        )
    )

    assert isinstance(result, TaskSavedPaymentMethodReadinessPlan)
    assert result.saved_payment_method_task_ids == ("task-cards",)
    record = result.records[0]
    assert isinstance(record, TaskSavedPaymentMethodReadinessRecord)
    assert record.payment_method_scenarios == (
        "saved_card",
        "payment_method_update",
        "card_vaulting",
        "customer_payment_update",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "provider_tokenization",
        "pci_scope_avoidance",
        "customer_notification",
        "sca_step_up_handling",
        "support_visibility",
        "test_coverage",
    )
    assert record.risk_level == "high"
    assert any("PCI scope" in check for check in record.recommended_checks)
    assert "files_or_modules: src/billing/payment_methods/saved_cards.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["saved_payment_method_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["provider_tokenization"] == 1
    assert result.summary["scenario_counts"]["saved_card"] == 1


def test_metadata_validation_commands_and_high_risk_scenarios_are_detected():
    result = analyze_task_saved_payment_method_readiness(
        _plan(
            [
                _task(
                    "task-provider",
                    title="Vault Stripe payment methods and retry failed invoices",
                    description=(
                        "Use Stripe setup intent tokenization for card vaulting, default payment method changes, "
                        "and payment retry flows."
                    ),
                    files_or_modules=[
                        "src/billing/stripe/card_vault.py",
                        "src/billing/default_payment_method.py",
                        "src/billing/payment_retry.py",
                    ],
                    tags=["delete payment method", "wallet update"],
                    metadata={
                        "payment_method": {
                            "pci_scope_avoidance": "Hosted fields avoid PCI scope and never store PAN or CVV.",
                            "customer_notification": "Notify customer by billing email after saved payment method changes.",
                            "support_visibility": "Support console shows brand, last4, status, and provider reference.",
                        }
                    },
                    validation_commands={
                        "test": [
                            "poetry run pytest tests/billing/test_saved_payment_method_retry_idempotency.py"
                        ]
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.payment_method_scenarios == (
        "saved_card",
        "payment_method_update",
        "wallet_update",
        "default_payment_instrument",
        "card_vaulting",
        "tokenization",
        "provider_reference",
        "payment_method_deletion",
        "retry_flow",
    )
    assert record.present_safeguards == (
        "provider_tokenization",
        "pci_scope_avoidance",
        "customer_notification",
        "retry_idempotency_behavior",
        "support_visibility",
        "test_coverage",
    )
    assert record.missing_safeguards == (
        "default_method_audit_trail",
        "sca_step_up_handling",
        "deletion_semantics",
    )
    assert record.risk_level == "high"
    assert any("metadata.payment_method.pci_scope_avoidance" in item for item in record.evidence)
    assert any("validation_commands:" in item for item in record.evidence)
    assert any("tags[0]" in item for item in record.evidence)


def test_fully_covered_saved_payment_method_workflow_is_low_risk():
    result = build_task_saved_payment_method_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship saved payment methods, wallets, and default cards",
                    description="Customers update saved cards, wallet payment methods, and default payment instrument.",
                    acceptance_criteria=[
                        "Provider tokenization uses Stripe setup intent payment method tokens.",
                        "PCI scope avoidance uses hosted fields and never stores raw card numbers, PAN, or CVV.",
                        "Default method audit trail records actor, previous default, new default, and timestamp.",
                        "Customer notification sends billing email after add, remove, replace, and default changes.",
                        "Retry idempotency behavior uses idempotency keys and duplicate charge prevention.",
                        "SCA step-up handling covers 3DS challenge flow and requires_action states.",
                        "Deletion semantics define detach behavior, cannot delete default, and retain billing history.",
                        "Support visibility shows safe last4, brand, status, default flag, and provider reference.",
                        "Payment method tests cover card vault tests, wallet tests, retry tests, SCA tests, and PCI tests.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.payment_method_scenarios == (
        "saved_card",
        "payment_method_update",
        "wallet_update",
        "default_payment_instrument",
        "card_vaulting",
        "tokenization",
        "provider_reference",
        "payment_method_deletion",
        "retry_flow",
    )
    assert record.present_safeguards == (
        "provider_tokenization",
        "pci_scope_avoidance",
        "default_method_audit_trail",
        "customer_notification",
        "retry_idempotency_behavior",
        "sca_step_up_handling",
        "deletion_semantics",
        "support_visibility",
        "test_coverage",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_unrelated_tasks_are_not_applicable_with_stable_empty_summary():
    result = build_task_saved_payment_method_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update invoice copy",
                    description="Adjust dashboard labels and loading states.",
                    files_or_modules=["src/ui/billing_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.recommendations == ()
    assert result.saved_payment_method_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "saved_payment_method_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "provider_tokenization": 0,
            "pci_scope_avoidance": 0,
            "default_method_audit_trail": 0,
            "customer_notification": 0,
            "retry_idempotency_behavior": 0,
            "sca_step_up_handling": 0,
            "deletion_semantics": 0,
            "support_visibility": 0,
            "test_coverage": 0,
        },
        "scenario_counts": {},
    }
    assert "No saved payment method readiness records were inferred." in result.to_markdown()
    assert "Not-applicable tasks: task-copy" in result.to_markdown()


def test_serialization_markdown_aliases_sorting_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Saved cards | ready",
                description=(
                    "Saved cards include provider tokenization, PCI scope avoidance, customer notification, "
                    "SCA step-up handling, support visibility, and test coverage."
                ),
            ),
            _task(
                "task-a",
                title="Delete default payment method",
                description="Delete payment method and default card changes are planned.",
            ),
            _task("task-copy", title="Copy update", description="Change helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_saved_payment_method_readiness(plan)
    payload = task_saved_payment_method_readiness_plan_to_dict(result)
    markdown = task_saved_payment_method_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_saved_payment_method_readiness_plan_to_dicts(result) == payload["records"]
    assert task_saved_payment_method_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_saved_payment_method_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_saved_payment_method_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_saved_payment_method_readiness(plan).to_dict() == result.to_dict()
    assert result.saved_payment_method_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "saved_payment_method_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "payment_method_scenarios",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_checks",
    ]
    assert markdown.startswith("# Task Saved Payment Method Readiness: plan-payment-methods")
    assert "Saved cards \\| ready" in markdown
    assert "| Task | Title | Risk | Scenarios | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


def test_execution_plan_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add customer wallet update",
        description="Customer updates payment wallet with SCA tests.",
        acceptance_criteria=["Wallet tests cover step-up challenge flow."],
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add provider payment method reference",
                    description="Stripe provider payment method id includes support visibility and PCI scope avoidance.",
                    metadata={"test_coverage": "Payment method tests cover provider references."},
                )
            ],
            plan_id="plan-model",
        )
    )

    object_result = build_task_saved_payment_method_readiness_plan([object_task])
    model_result = build_task_saved_payment_method_readiness_plan(plan_model)
    invalid = build_task_saved_payment_method_readiness_plan(17)

    assert object_result.records[0].task_id == "task-object"
    assert object_result.records[0].payment_method_scenarios == ("wallet_update", "customer_payment_update")
    assert "test_coverage" in object_result.records[0].present_safeguards
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"
    assert model_result.records[0].payment_method_scenarios == (
        "payment_method_update",
        "provider_reference",
    )
    assert invalid.records == ()


def _plan(tasks, *, plan_id="plan-payment-methods"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-payment-methods",
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
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_commands=None,
    tags=None,
):
    payload = {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
    }
    if files_or_modules is not None:
        payload["files_or_modules"] = files_or_modules
    if metadata is not None:
        payload["metadata"] = metadata
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    if tags is not None:
        payload["tags"] = tags
    return payload
