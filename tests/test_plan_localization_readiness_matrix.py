import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_localization_readiness_matrix import (
    PlanLocalizationReadinessMatrix,
    PlanLocalizationReadinessRow,
    build_plan_localization_readiness_matrix,
    plan_localization_readiness_matrix_to_dict,
    plan_localization_readiness_matrix_to_markdown,
)


def test_localization_signals_from_text_paths_and_metadata_create_deduped_rows():
    result = build_plan_localization_readiness_matrix(
        _plan(
            [
                _task(
                    "task-checkout-l10n",
                    title="Localized checkout copy and currency",
                    description=(
                        "Translate checkout user-facing copy with locale-specific currency, date format, "
                        "pluralization for item counts, RTL layout, and GDPR consent copy."
                    ),
                    files_or_modules=[
                        "src/frontend/i18n/checkout.json",
                        "src/frontend/i18n/checkout.json",
                        "src/frontend/rtl/checkout_currency.tsx",
                    ],
                    acceptance_criteria=[
                        "Localized QA includes pseudo-localization screenshots and copy expansion checks."
                    ],
                    metadata={
                        "translation": {"vendor": "handoff ready"},
                        "regional_compliance_copy": "Legal approves GDPR consent copy.",
                    },
                ),
                _task(
                    "task-cache",
                    title="Tune account cache",
                    description="Adjust backend cache TTL for account summary queries.",
                    files_or_modules=["src/backend/cache/account_cache.py"],
                ),
            ]
        )
    )

    assert isinstance(result, PlanLocalizationReadinessMatrix)
    assert result.plan_id == "plan-localization"
    assert result.localized_task_ids == ("task-checkout-l10n",)
    assert result.no_signal_task_ids == ("task-cache",)
    assert [row.category for row in result.rows] == [
        "locale_copy",
        "translation",
        "date_time_currency_formatting",
        "rtl_layout",
        "pluralization",
        "regional_compliance_copy",
        "localized_qa",
    ]

    translation = _row(result, "translation")
    assert isinstance(translation, PlanLocalizationReadinessRow)
    assert translation.severity == "medium"
    assert translation.evidence.count("files_or_modules: src/frontend/i18n/checkout.json") == 1
    assert "metadata.translation: translation" in translation.evidence
    assert any("translation resources" in item for item in translation.required_artifacts)
    assert any("translation keys frozen" in item for item in translation.follow_up_questions)

    compliance = _row(result, "regional_compliance_copy")
    assert compliance.severity == "critical"
    assert (
        "metadata.regional_compliance_copy: Legal approves GDPR consent copy."
        in compliance.evidence
    )
    assert any("Legal or policy approval" in item for item in compliance.required_artifacts)

    assert result.summary["task_count"] == 2
    assert result.summary["localized_task_count"] == 1
    assert result.summary["no_signal_task_count"] == 1
    assert result.summary["severity_counts"] == {
        "critical": 1,
        "high": 3,
        "medium": 2,
        "low": 1,
    }
    assert result.summary["category_counts"]["rtl_layout"] == 1


def test_execution_plan_input_serialization_ordering_and_markdown_escaping_are_stable():
    plan = _plan(
        [
            _task(
                "task-copy | pipe",
                title="Email copy | locale",
                description="Update locale copy for transactional email content strings.",
                files_or_modules=["src/content/emails/welcome_copy.ts"],
            ),
            _task(
                "task-date",
                title="Format price and date",
                description="Use locale-aware date format, timezone, number format, and currency display.",
                files_or_modules=["src/billing/currency/date_format.ts"],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_localization_readiness_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_localization_readiness_matrix_to_dict(result)
    markdown = plan_localization_readiness_matrix_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "localized_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "category",
        "severity",
        "required_artifacts",
        "evidence",
        "follow_up_questions",
    ]
    assert [(row.task_id, row.category) for row in result.rows] == [
        ("task-copy | pipe", "locale_copy"),
        ("task-copy | pipe", "translation"),
        ("task-date", "translation"),
        ("task-date", "date_time_currency_formatting"),
    ]
    assert markdown.startswith("# Plan Localization Readiness Matrix: plan-localization")
    assert (
        "| Task | Category | Severity | Required Artifacts | Evidence | Follow-up Questions |"
        in markdown
    )
    assert "`task-copy \\| pipe`" in markdown
    assert plan_localization_readiness_matrix_to_markdown(result) == result.to_markdown()


def test_empty_invalid_and_no_signal_inputs_render_deterministic_empty_outputs():
    empty = build_plan_localization_readiness_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_localization_readiness_matrix(17)
    no_signal = build_plan_localization_readiness_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Optimize API pagination",
                    description="Tune backend query limits for account search.",
                    files_or_modules=["src/api/search.py"],
                )
            ]
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "localized_task_ids": [],
        "no_signal_task_ids": [],
        "summary": {
            "task_count": 0,
            "localized_task_count": 0,
            "no_signal_task_count": 0,
            "severity_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0},
            "category_counts": {
                "locale_copy": 0,
                "translation": 0,
                "date_time_currency_formatting": 0,
                "rtl_layout": 0,
                "pluralization": 0,
                "regional_compliance_copy": 0,
                "localized_qa": 0,
            },
        },
    }
    assert "No localization readiness rows were inferred." in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert no_signal.rows == ()
    assert no_signal.no_signal_task_ids == ("task-api",)
    assert "No localization signals: task-api" in no_signal.to_markdown()


def _row(result, category):
    return next(row for row in result.rows if row.category == category)


def _plan(tasks, *, plan_id="plan-localization"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-localization",
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
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
