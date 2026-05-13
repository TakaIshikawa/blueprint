import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_localization_readiness import (
    analyze_task_api_localization_readiness,
    build_task_api_localization_readiness_plan,
    extract_task_api_localization_readiness,
    summarize_task_api_localization_readiness,
    task_api_localization_readiness_plan_to_dict,
    task_api_localization_readiness_plan_to_dicts,
    task_api_localization_readiness_plan_to_markdown,
)


def test_complete_api_localization_task_is_ready():
    result = build_task_api_localization_readiness_plan(
        _plan(
            [
                _task(
                    "l10n-ready",
                    "Add API localization negotiation",
                    (
                        "Localized API responses use locale negotiation from the Accept-Language header. "
                        "Translation catalogs include catalog validation and missing key coverage. "
                        "Fallback locale policy handles unsupported locale and missing translation fallback. "
                        "Currency formatting tests, date formatting tests, and pluralization tests pass. "
                        "RTL tests cover right-to-left content. Vary: Accept-Language protects response caching."
                    ),
                    ["src/api/i18n/accept_language.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.impact == "medium"
    assert record.detected_signals == (
        "api_localization",
        "locale_negotiation",
        "accept_language",
        "translation_catalog",
        "fallback_locale",
        "localized_formatting",
        "rtl_support",
    )
    assert record.present_safeguards == (
        "accept_language_tests",
        "locale_fallback_policy",
        "catalog_validation",
        "formatting_tests",
        "rtl_coverage",
        "cache_vary_headers",
    )
    assert record.missing_safeguards == ()


def test_partial_localization_task_reports_missing_safeguards_and_summary_counts():
    result = analyze_task_api_localization_readiness(
        _plan(
            [
                _task(
                    "l10n-partial",
                    "Parse Accept-Language for API responses",
                    "Implement locale resolver with fallback locale for unsupported languages.",
                    ["src/api/localization/locale_resolver.py"],
                ),
                _task("copy", "Docs", "Update unrelated endpoint docs.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("l10n-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert {"locale_negotiation", "accept_language", "fallback_locale"} <= set(record.detected_signals)
    assert record.present_safeguards == ("locale_fallback_policy",)
    assert record.readiness == "partial"
    assert record.impact == "high"
    assert "accept_language_tests" in record.missing_safeguards
    assert "catalog_validation" in record.missing_safeguards
    assert result.summary["localization_task_count"] == 1
    assert result.summary["signal_counts"]["accept_language"] == 1
    assert result.summary["missing_safeguard_counts"]["catalog_validation"] == 1


def test_mapping_models_objects_commands_and_mutation_safety_are_supported():
    source = _plan(
        [
            _task(
                "l10n-object",
                "Translation catalogs",
                "Load message catalogs.",
                ["src/api/translations/catalogs.py"],
                validation_commands={"pytest": "pytest tests/api/test_accept_language.py"},
            )
        ]
    )
    original = copy.deepcopy(source)
    dict_result = extract_task_api_localization_readiness(source)
    task = ExecutionTask(
        id="model-task",
        title="Localized formatting",
        description="Add currency formatting and date formatting tests.",
        files_or_modules=["src/api/formatting.py"],
        acceptance_criteria=["Number formatting tests pass."],
    )
    plan_result = build_task_api_localization_readiness_plan(
        ExecutionPlan(id="model-plan", implementation_brief_id="brief", milestones=[], tasks=[task])
    )

    class TaskLike:
        id = "object-task"
        title = "RTL API localization"
        description = "RTL coverage for Arabic localized API payloads."
        files_or_modules = ["src/api/rtl.py"]
        acceptance_criteria = ["RTL tests pass."]

    object_result = summarize_task_api_localization_readiness(TaskLike())

    assert source == original
    assert dict_result.records[0].task_id == "l10n-object"
    assert "accept_language_tests" in dict_result.records[0].present_safeguards
    assert plan_result.plan_id == "model-plan"
    assert plan_result.records[0].task_id == "model-task"
    assert object_result.records[0].task_id == "object-task"


def test_serialization_and_markdown_are_stable():
    result = build_task_api_localization_readiness_plan(
        _plan([_task("alias", "API localization", "Accept-Language tests cover fallback locale.", [])])
    )
    payload = task_api_localization_readiness_plan_to_dict(result)

    assert task_api_localization_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    markdown = task_api_localization_readiness_plan_to_markdown(result)
    assert "# Task API Localization Readiness: plan-localization" in markdown
    assert "| `alias` | API localization |" in markdown


def _plan(tasks):
    return {"id": "plan-localization", "tasks": tasks}


def _task(task_id, title, description, files, **extra):
    payload = {"id": task_id, "title": title, "description": description, "files_or_modules": files}
    payload.update(extra)
    return payload
