import copy
import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_privacy_review_matrix import (
    PlanPrivacyReviewMatrixRow,
    build_plan_privacy_review_matrix,
    plan_privacy_review_matrix_to_dict,
    plan_privacy_review_matrix_to_markdown,
)


def test_brief_only_input_groups_privacy_review_categories():
    result = build_plan_privacy_review_matrix(
        _brief(
            data_requirements=(
                "Collect customer data and PII for account lookup. "
                "Honor GDPR consent before analytics tracking."
            ),
            risks=["DPA coverage is unclear for the analytics processor."],
        )
    )

    assert result.brief_id == "brief-privacy"
    assert result.plan_id is None
    assert [row.category for row in result.rows] == [
        "pii",
        "customer_data",
        "consent",
        "analytics",
        "gdpr",
        "dpa",
    ]
    assert result.rows[0] == PlanPrivacyReviewMatrixRow(
        category="pii",
        review_status="requires_review",
        affected_task_ids=(),
        evidence=(
            "brief.data_requirements: Collect customer data and PII for account lookup. "
            "Honor GDPR consent before analytics tracking",
        ),
        recommended_reviewers=("privacy", "security", "data owner"),
        required_review_questions=(
            "Which exact personal data fields are collected, stored, displayed, or transformed?",
            "Can the implementation minimize, mask, or redact any personal data before processing?",
        ),
    )
    assert _row(result, "gdpr").review_status == "requires_legal_review"
    assert _row(result, "dpa").recommended_reviewers == (
        "legal",
        "privacy",
        "vendor management",
    )


def test_plan_input_uses_task_ids_and_task_evidence():
    result = build_plan_privacy_review_matrix(
        _plan(
            [
                _task(
                    "task-retention",
                    title="Add retention purge for account deletion",
                    description=(
                        "Delete customer records after the retention window and keep audit logs."
                    ),
                ),
                _task(
                    "task-transfer",
                    title="Send patient data to EU processor",
                    description=(
                        "HIPAA review is required before cross-border transfer under SCCs."
                    ),
                ),
            ]
        )
    )

    assert result.brief_id is None
    assert result.plan_id == "plan-privacy"
    assert _row(result, "customer_data").affected_task_ids == ("task-retention",)
    assert _row(result, "retention").affected_task_ids == ("task-retention",)
    assert _row(result, "deletion").affected_task_ids == ("task-retention",)
    assert _row(result, "audit_logs").affected_task_ids == ("task-retention",)
    assert _row(result, "hipaa").affected_task_ids == ("task-transfer",)
    assert _row(result, "cross_border_transfer").affected_task_ids == ("task-transfer",)
    assert _row(result, "cross_border_transfer").required_review_questions == (
        "Which regions send and receive the data, and what transfer mechanism applies?",
        "Are data residency, SCC, or transfer impact assessment requirements satisfied?",
    )


def test_combined_brief_plus_plan_merges_category_evidence_without_mutating_inputs():
    brief = _brief(
        data_requirements="Use customer data for usage analytics with opt-out consent.",
    )
    plan = _plan(
        [
            _task(
                "task-analytics",
                title="Gate analytics events",
                description="Emit product analytics only after consent is present.",
                metadata={"privacy": {"review": "Confirm CCPA opt-out behavior."}},
            )
        ]
    )
    original_brief = copy.deepcopy(brief)
    original_plan = copy.deepcopy(plan)

    result = build_plan_privacy_review_matrix(
        ImplementationBrief.model_validate(brief),
        ExecutionPlan.model_validate(plan),
    )
    payload = plan_privacy_review_matrix_to_dict(result)

    assert brief == original_brief
    assert plan == original_plan
    assert result.brief_id == "brief-privacy"
    assert result.plan_id == "plan-privacy"
    assert _row(result, "analytics").affected_task_ids == ("task-analytics",)
    assert len(_row(result, "analytics").evidence) == 3
    assert _row(result, "consent").affected_task_ids == ("task-analytics",)
    assert _row(result, "ccpa").review_status == "requires_legal_review"
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "category",
        "review_status",
        "affected_task_ids",
        "evidence",
        "recommended_reviewers",
        "required_review_questions",
    ]


def test_no_signal_input_returns_empty_matrix_and_markdown():
    result = build_plan_privacy_review_matrix(
        _brief(data_requirements="Store display preferences for the dashboard."),
        _plan(
            [
                _task(
                    "task-ui",
                    title="Add dashboard layout",
                    description="Render saved widgets and theme controls.",
                )
            ]
        ),
    )

    assert result.rows == ()
    assert result.summary == {
        "category_count": 0,
        "affected_task_count": 0,
        "legal_review_count": 0,
    }
    assert result.to_dict() == {
        "brief_id": "brief-privacy",
        "plan_id": "plan-privacy",
        "summary": {
            "category_count": 0,
            "affected_task_count": 0,
            "legal_review_count": 0,
        },
        "rows": [],
    }
    assert plan_privacy_review_matrix_to_markdown(result) == (
        "# Plan Privacy Review Matrix: brief brief-privacy, plan plan-privacy\n\n"
        "No privacy review rows were inferred."
    )


def test_stable_category_ordering_and_markdown_rendering():
    result = build_plan_privacy_review_matrix(
        _plan(
            [
                _task(
                    "task-all",
                    title=(
                        "Review cross-border DPA for HIPAA, CCPA, GDPR, analytics, consent, "
                        "audit logs, deletion, retention, customer data, and PII"
                    ),
                    description="Implement privacy review gates.",
                )
            ]
        )
    )

    assert [row.category for row in result.rows] == [
        "pii",
        "customer_data",
        "consent",
        "retention",
        "deletion",
        "audit_logs",
        "analytics",
        "gdpr",
        "ccpa",
        "hipaa",
        "dpa",
        "cross_border_transfer",
    ]
    markdown = result.to_markdown()
    assert markdown.startswith("# Plan Privacy Review Matrix: plan plan-privacy")
    assert (
        "| pii | requires_review | task-all | task.task-all.title: Review cross-border"
    ) in markdown
    assert "| cross_border_transfer | requires_legal_review | task-all |" in markdown


def _row(result, category):
    return next(row for row in result.rows if row.category == category)


def _brief(*, data_requirements=None, risks=None):
    return {
        "id": "brief-privacy",
        "source_brief_id": "source-privacy",
        "title": "Privacy-sensitive implementation",
        "domain": "operations",
        "target_user": "support admins",
        "buyer": "support leadership",
        "workflow_context": "Account operations",
        "problem_statement": "Support needs safer account operations.",
        "mvp_goal": "Ship the first workflow.",
        "product_surface": "Admin console",
        "scope": ["Admin workflow"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Use existing service boundaries.",
        "data_requirements": data_requirements or "Account state and workflow status.",
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Run focused tests.",
        "definition_of_done": ["Tests pass"],
        "status": "draft",
    }


def _plan(tasks):
    return {
        "id": "plan-privacy",
        "implementation_brief_id": "brief-privacy",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, metadata=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
