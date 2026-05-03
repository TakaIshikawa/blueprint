import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_3ds_payment_authentication_readiness import (
    Task3dsPaymentAuthenticationReadinessPlan,
    Task3dsPaymentAuthenticationReadinessRecord,
    analyze_task_3ds_payment_authentication_readiness,
    build_task_3ds_payment_authentication_readiness_plan,
    extract_task_3ds_payment_authentication_readiness,
    generate_task_3ds_payment_authentication_readiness,
    summarize_task_3ds_payment_authentication_readiness,
    task_3ds_payment_authentication_readiness_plan_to_dict,
    task_3ds_payment_authentication_readiness_plan_to_dicts,
    task_3ds_payment_authentication_readiness_plan_to_markdown,
)


def test_high_severity_3ds_requires_action_without_safeguards():
    result = build_task_3ds_payment_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-3ds",
                    title="Implement Stripe 3DS requires_action checkout",
                    description=(
                        "Add 3-D Secure and SCA handling for PaymentIntent requires_action "
                        "during checkout."
                    ),
                    files_or_modules=[
                        "src/payments/stripe/requires_action_handler.py",
                        "tests/payments/test_3ds_checkout.py",
                    ],
                    acceptance_criteria=["Checkout can request payment authentication."],
                )
            ]
        )
    )

    assert isinstance(result, Task3dsPaymentAuthenticationReadinessPlan)
    assert result.payment_authentication_task_ids == ("task-3ds",)
    record = result.records[0]
    assert isinstance(record, Task3dsPaymentAuthenticationReadinessRecord)
    assert record.detected_categories == ("challenge_flow", "provider_webhook", "test_evidence")
    assert record.present_acceptance_criteria == ("challenge_flow",)
    assert record.missing_acceptance_criteria == (
        "frictionless_flow",
        "failure_fallback",
        "liability_shift",
        "provider_webhook",
        "test_evidence",
    )
    assert record.severity == "high"
    assert record.evidence_paths == (
        "src/payments/stripe/requires_action_handler.py",
        "tests/payments/test_3ds_checkout.py",
    )
    assert result.summary["severity_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["category_counts"]["challenge_flow"] == 1
    assert result.summary["missing_acceptance_criteria_counts"]["failure_fallback"] == 1


def test_low_severity_when_challenge_fallback_webhook_and_validation_are_complete():
    result = analyze_task_3ds_payment_authentication_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Harden 3DS payment authentication",
                    description=(
                        "Support SCA challenge flow, frictionless exemptions, liability shift "
                        "metadata, saved payment methods, and Stripe provider webhooks."
                    ),
                    acceptance_criteria=[
                        "3DS requires_action challenge completes and returns to checkout successfully.",
                        "Frictionless and SCA exemption authorizations succeed without a challenge.",
                        "Canceled, timed-out, failed, and declined authentication use the retry fallback.",
                        "ECI, CAVV, authentication result, and liability shift are stored.",
                        "Stripe webhook events payment_intent.succeeded and payment_intent.payment_failed are handled.",
                        "Saved card setup intent, mandate, and off-session payment method authentication are covered.",
                        "Validation commands run 3DS test card, webhook fixture, and fallback tests.",
                    ],
                    metadata={
                        "validation_commands": {
                            "payments": [
                                "poetry run pytest tests/payments/test_3ds_requires_action.py",
                                "stripe trigger payment_intent.succeeded",
                            ]
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_categories == (
        "challenge_flow",
        "frictionless_flow",
        "failure_fallback",
        "liability_shift",
        "provider_webhook",
        "saved_payment_method",
        "test_evidence",
    )
    assert record.present_acceptance_criteria == record.detected_categories
    assert record.missing_acceptance_criteria == ()
    assert record.severity == "low"
    assert record.suggested_test_evidence == ()
    assert result.summary["missing_acceptance_criteria_count"] == 0
    assert result.summary["severity_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_metadata_tags_paths_and_validation_commands_detect_readiness():
    result = build_task_3ds_payment_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-meta",
                    title="Update billing authorization",
                    description="Add payment authentication for card on file renewals.",
                    files_or_modules=["src/billing/three_d_secure/saved_payment_method.py"],
                    metadata={
                        "sca": {"requires_action": "SetupIntent requires_action must complete for mandates."},
                        "provider_webhook": "Handle setup_intent.succeeded and setup_intent.setup_failed.",
                        "validation_commands": {
                            "auth": ["pytest tests/billing/test_setup_intent_requires_action.py"]
                        },
                    },
                    tags=["frictionless exemption covered", "liability shift ECI recorded"],
                    acceptance_criteria=[
                        "Saved payment method setup intent and off-session mandate authentication are covered.",
                        "Provider webhook setup_intent.succeeded is handled.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_categories == (
        "challenge_flow",
        "frictionless_flow",
        "liability_shift",
        "provider_webhook",
        "saved_payment_method",
        "test_evidence",
    )
    assert record.present_acceptance_criteria == (
        "challenge_flow",
        "frictionless_flow",
        "provider_webhook",
        "saved_payment_method",
        "test_evidence",
    )
    assert record.missing_acceptance_criteria == (
        "failure_fallback",
        "liability_shift",
    )
    assert record.severity == "medium"
    assert "src/billing/three_d_secure/saved_payment_method.py" in record.evidence_paths
    assert any("metadata.provider_webhook" in item for item in record.evidence)
    assert any(
        "metadata.validation_commands.auth[0]: pytest tests/billing/test_setup_intent_requires_action.py" in item
        for item in record.evidence
    )


def test_unrelated_tasks_are_no_impact_with_stable_summary_and_markdown():
    result = build_task_3ds_payment_authentication_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update checkout copy",
                    description="No 3DS or SCA changes are in scope for this copy-only task.",
                    files_or_modules=["src/checkout/copy.py"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.payment_authentication_task_ids == ()
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "payment_authentication_task_count": 0,
        "no_impact_task_ids": ["task-copy"],
        "missing_acceptance_criteria_count": 0,
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "category_counts": {
            "challenge_flow": 0,
            "frictionless_flow": 0,
            "failure_fallback": 0,
            "liability_shift": 0,
            "provider_webhook": 0,
            "saved_payment_method": 0,
            "test_evidence": 0,
        },
        "present_acceptance_criteria_counts": {
            "challenge_flow": 0,
            "frictionless_flow": 0,
            "failure_fallback": 0,
            "liability_shift": 0,
            "provider_webhook": 0,
            "saved_payment_method": 0,
            "test_evidence": 0,
        },
        "missing_acceptance_criteria_counts": {
            "challenge_flow": 0,
            "frictionless_flow": 0,
            "failure_fallback": 0,
            "liability_shift": 0,
            "provider_webhook": 0,
            "saved_payment_method": 0,
            "test_evidence": 0,
        },
    }
    markdown = result.to_markdown()
    assert "No 3DS/SCA payment authentication readiness records were inferred." in markdown
    assert "No-impact tasks: task-copy" in markdown


def test_serialization_aliases_prebuilt_report_markdown_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="3DS challenge | fallback",
                description="SCA challenge flow with failed authentication fallback and provider webhook handling.",
                acceptance_criteria=[
                    "3DS challenge completes.",
                    "Authentication failed fallback retries checkout.",
                    "Payment webhook receives payment_intent.succeeded.",
                ],
            ),
            _task(
                "task-a",
                title="SCA payment auth ready",
                description="SCA challenge flow covers frictionless, fallback, liability shift, webhooks, and tests.",
                acceptance_criteria=[
                    "requires_action challenge completes.",
                    "Frictionless exemption succeeds without a challenge.",
                    "Authentication failed fallback retries checkout.",
                    "Liability shift ECI and CAVV are stored.",
                    "Stripe webhook payment_intent.succeeded is handled.",
                    "3DS test card validation command covers webhook fixture.",
                ],
            ),
            _task("task-copy", title="Profile UI copy", description="Adjust labels."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_3ds_payment_authentication_readiness(plan)
    payload = task_3ds_payment_authentication_readiness_plan_to_dict(result)
    markdown = task_3ds_payment_authentication_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_3ds_payment_authentication_readiness_plan_to_dicts(result) == payload["records"]
    assert task_3ds_payment_authentication_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_3ds_payment_authentication_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_3ds_payment_authentication_readiness(plan).to_dict() == result.to_dict()
    assert build_task_3ds_payment_authentication_readiness_plan(result) is result
    assert result.affected_task_ids == result.payment_authentication_task_ids
    assert result.not_applicable_task_ids == result.no_impact_task_ids
    assert result.payment_authentication_task_ids == ("task-z", "task-a")
    assert list(payload) == [
        "plan_id",
        "records",
        "payment_authentication_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_categories",
        "present_acceptance_criteria",
        "missing_acceptance_criteria",
        "severity",
        "evidence",
        "evidence_paths",
        "suggested_test_evidence",
    ]
    assert [record.severity for record in result.records] == ["medium", "low"]
    assert markdown.startswith("# Task 3DS Payment Authentication Readiness: plan-3ds")
    assert "3DS challenge \\| fallback" in markdown


def test_execution_plan_task_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add 3DS for saved cards",
        description=(
            "Saved payment method SCA covers requires_action challenge, frictionless exemption, "
            "failure fallback, liability shift, provider webhook, and test evidence."
        ),
        files_or_modules=["src/payments/3ds/saved_cards.py"],
        acceptance_criteria=[
            "requires_action challenge completes for saved cards.",
            "Frictionless exemption succeeds.",
            "Authentication failed fallback retries checkout.",
            "Liability shift ECI and CAVV are stored.",
            "Provider webhook payment_intent.succeeded is handled.",
            "Saved payment method setup intent mandate is covered.",
            "3DS test card validation command runs.",
        ],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Stripe 3DS requires_action",
            description="Handle requires_action challenge with webhook completion.",
            acceptance_criteria=["3DS challenge completes."],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    iterable_result = build_task_3ds_payment_authentication_readiness_plan([object_task])
    task_result = build_task_3ds_payment_authentication_readiness_plan(task_model)
    plan_result = build_task_3ds_payment_authentication_readiness_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].severity == "low"
    assert task_result.records[0].task_id == "task-model"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-3ds"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-3ds",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
