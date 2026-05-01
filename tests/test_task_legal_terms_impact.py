import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_legal_terms_impact import (
    TaskLegalTermsImpactPlan,
    TaskLegalTermsImpactRecord,
    analyze_task_legal_terms_impact,
    build_task_legal_terms_impact_plan,
    summarize_task_legal_terms_impact,
    task_legal_terms_impact_plan_to_dict,
    task_legal_terms_impact_plan_to_markdown,
)


def test_terms_privacy_consent_and_subscription_surfaces_are_detected():
    result = build_task_legal_terms_impact_plan(
        _plan(
            [
                _task(
                    "task-legal-copy",
                    title="Update checkout legal acceptance",
                    description=(
                        "Require terms of service acceptance, update the privacy policy, "
                        "add cookie consent, and capture marketing consent before checkout."
                    ),
                    files_or_modules=["src/legal/checkout_terms.py"],
                    acceptance_criteria=[
                        "Legal approves terms acceptance, cookie opt-in, and unsubscribe behavior."
                    ],
                    metadata={
                        "subscription_terms": "Auto-renewal and cancellation terms require review.",
                        "payments": "Payment terms and tax disclosure must match checkout.",
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskLegalTermsImpactPlan)
    assert result.plan_id == "plan-legal"
    assert result.legal_task_ids == ("task-legal-copy",)
    record = result.records[0]
    assert isinstance(record, TaskLegalTermsImpactRecord)
    assert record.impact_level == "high"
    assert record.legal_surfaces == (
        "privacy_policy",
        "terms_of_service",
        "subscription_terms",
        "payment_terms",
        "cookie_consent",
        "marketing_consent",
    )
    assert record.required_reviewers == (
        "legal counsel",
        "product owner",
        "privacy counsel",
        "finance or revenue operations",
    )
    assert any("Updated terms of service" in value for value in record.required_artifacts)
    assert any("Payment authorization" in value for value in record.required_artifacts)
    assert any("Block launch" in value for value in record.safeguards)
    assert any("customer notice" in value for value in record.follow_up_questions)
    assert record.evidence[:2] == (
        "files_or_modules: src/legal/checkout_terms.py",
        "description: Require terms of service acceptance, update the privacy policy, add cookie consent, and capture marketing consent before checkout.",
    )
    assert result.summary["impact_counts"] == {"critical": 0, "high": 1, "medium": 0, "none": 0}
    assert result.summary["surface_counts"]["terms_of_service"] == 1


def test_dpa_sla_refund_age_and_regulated_claims_are_critical_with_reviewers():
    result = analyze_task_legal_terms_impact(
        _plan(
            [
                _task(
                    "task-contracts",
                    title="Add enterprise contract obligations",
                    description=(
                        "Update the DPA for subprocessors, add SLA service credits, "
                        "revise refund policy rules, and add age verification for minors."
                    ),
                    files_or_modules=["src/contracts/dpa.py", "src/support/sla.py"],
                    metadata={
                        "claim_review": "Marketing page includes a guaranteed return regulated claim."
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level == "critical"
    assert record.legal_surfaces == (
        "regulated_claims",
        "age_restrictions",
        "data_processing_agreement",
        "refund_policy",
        "service_level_agreement",
    )
    assert record.required_reviewers == (
        "legal counsel",
        "product owner",
        "privacy counsel",
        "finance or revenue operations",
        "compliance reviewer",
        "security reviewer",
        "support owner",
    )
    assert any("explicit legal approval" in value for value in record.safeguards)
    assert any("subprocessors" in value for value in record.follow_up_questions)
    assert any("regulated claim" in value for value in record.follow_up_questions)


def test_deduplicates_evidence_and_artifacts_deterministically():
    result = build_task_legal_terms_impact_plan(
        [
            _task(
                "task-consent",
                title="Add cookie consent",
                description="Add cookie consent and cookie consent preference persistence.",
                files_or_modules=[
                    "src/privacy/cookie_consent.py",
                    "src/privacy/cookie_consent.py",
                ],
                acceptance_criteria=[
                    "Verify cookie consent opt-in.",
                    "Verify cookie consent opt-in.",
                ],
            )
        ]
    )

    record = result.records[0]

    assert record.impact_level == "medium"
    assert record.legal_surfaces == ("cookie_consent",)
    assert len(record.evidence) == len(set(record.evidence))
    assert len(record.required_artifacts) == len(set(record.required_artifacts))
    assert record.evidence.count("files_or_modules: src/privacy/cookie_consent.py") == 1


def test_model_inputs_empty_invalid_and_serialization_are_stable():
    task_dict = _task(
        "task-pipes",
        title="Privacy policy | DPA wording",
        description="Update privacy policy wording and DPA transfer basis.",
        files_or_modules={"main": "legal/privacy-policy.md", "none": None},
        acceptance_criteria={"approval": "Legal review signs off."},
        metadata={"artifacts": [{"path": "legal/dpa.md"}, None, 7]},
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Add terms of service acceptance",
            description="Display terms of service acceptance during signup.",
            files_or_modules=["src/signup/terms_acceptance.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")]))

    first = summarize_task_legal_terms_impact(
        _plan([task_dict, task_model.model_dump(mode="python")])
    )
    second = build_task_legal_terms_impact_plan(plan_model)
    empty = build_task_legal_terms_impact_plan(_plan([]))
    invalid = build_task_legal_terms_impact_plan(7)
    payload = task_legal_terms_impact_plan_to_dict(first)
    markdown = task_legal_terms_impact_plan_to_markdown(first)

    assert task_dict == original
    assert task_legal_terms_impact_plan_to_dict(second)["records"][0]["task_id"] == "task-model"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert invalid.records == ()
    assert first.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "legal_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "impact_level",
        "legal_surfaces",
        "required_reviewers",
        "required_artifacts",
        "safeguards",
        "follow_up_questions",
        "evidence",
    ]
    assert markdown.startswith("# Task Legal Terms Impact Plan: plan-legal")
    assert "Privacy policy \\| DPA wording" in markdown
    assert payload["records"][0]["impact_level"] == "critical"


def test_unrelated_documentation_task_is_not_false_positive():
    result = build_task_legal_terms_impact_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Refresh subscription docs",
                    description=(
                        "Document subscription status fields and payment event examples "
                        "for the developer guide."
                    ),
                    files_or_modules=["docs/subscription-events.md"],
                    acceptance_criteria=["Docs reviewed for clarity."],
                )
            ]
        )
    )

    assert result.legal_task_ids == ()
    assert result.records == (
        TaskLegalTermsImpactRecord(
            task_id="task-docs",
            title="Refresh subscription docs",
            impact_level="none",
            legal_surfaces=(),
            required_reviewers=(),
            required_artifacts=(),
            safeguards=(),
            follow_up_questions=(),
            evidence=(),
        ),
    )
    assert result.summary["impact_counts"] == {"critical": 0, "high": 0, "medium": 0, "none": 1}


def _plan(tasks):
    return {
        "id": "plan-legal",
        "implementation_brief_id": "brief-legal",
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
    return task
