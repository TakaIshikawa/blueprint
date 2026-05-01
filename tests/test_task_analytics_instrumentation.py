import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_analytics_instrumentation import (
    TaskAnalyticsInstrumentationFinding,
    TaskAnalyticsInstrumentationPlan,
    build_task_analytics_instrumentation_plan,
    summarize_task_analytics_instrumentation,
    task_analytics_instrumentation_plan_to_dict,
    task_analytics_instrumentation_plan_to_markdown,
)


def test_product_analytics_signals_are_detected_and_ranked_by_readiness():
    result = build_task_analytics_instrumentation_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Checkout funnel analytics",
                    description=(
                        "Instrument analytics event tracking for checkout funnel conversion. "
                        "Tracking plan includes event name, properties, user id, event schema, "
                        "privacy consent, dashboard, and Segment validation fixtures."
                    ),
                    files_or_modules=["src/analytics/checkout_funnel_events.ts"],
                    metadata={
                        "validation_commands": {"test": ["poetry run pytest tests/analytics/test_checkout_schema.py"]}
                    },
                ),
                _task(
                    "task-partial",
                    title="Activation experiment dashboard",
                    description=(
                        "Add A/B test experiment reporting for activation metrics with variant exposure, "
                        "conversion, retention cohorts, and Amplitude dashboard panels."
                    ),
                    files_or_modules=["analytics/dashboards/activation_experiment.yml"],
                ),
                _task(
                    "task-missing",
                    title="Capture attribution",
                    description="Track campaign attribution with UTM source and referrer for signup.",
                    files_or_modules=["src/marketing/utm_capture.ts"],
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify labels on account settings.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskAnalyticsInstrumentationPlan)
    assert result.plan_id == "plan-analytics"
    assert result.instrumented_task_ids == ("task-missing", "task-partial", "task-ready")
    assert result.non_instrumentation_task_ids == ("task-copy",)
    assert result.summary["readiness_counts"] == {
        "missing": 1,
        "partial": 1,
        "ready": 1,
        "not_applicable": 1,
    }
    assert result.summary["signal_counts"]["event_tracking"] == 1
    assert result.summary["signal_counts"]["funnel"] == 1
    assert result.summary["signal_counts"]["metrics"] == 1
    assert result.summary["signal_counts"]["dashboard"] == 2
    assert result.summary["signal_counts"]["experiment"] == 1
    assert result.summary["signal_counts"]["conversion"] == 2
    assert result.summary["signal_counts"]["activation"] == 1
    assert result.summary["signal_counts"]["retention"] == 1
    assert result.summary["signal_counts"]["attribution"] == 1
    assert result.summary["signal_counts"]["analytics_integration"] == 2
    assert result.summary["signal_counts"]["telemetry_schema"] == 1

    ready = _finding(result, "task-ready")
    assert isinstance(ready, TaskAnalyticsInstrumentationFinding)
    assert ready.readiness == "ready"
    assert ready.instrumentation_signals == (
        "event_tracking",
        "funnel",
        "dashboard",
        "conversion",
        "analytics_integration",
        "telemetry_schema",
    )
    assert any("ordered funnel step events" in item for item in ready.required_event_definitions)
    assert any("provider-specific validation" in item for item in ready.validation_recommendations)
    assert any("PII" in item for item in ready.privacy_cues)
    assert any("funnel dashboard" in item for item in ready.dashboard_updates)
    assert any("warehouse backfill" in item for item in ready.backfill_considerations)
    assert "validation_commands: poetry run pytest tests/analytics/test_checkout_schema.py" in ready.evidence

    partial = _finding(result, "task-partial")
    assert partial.readiness == "partial"
    assert partial.instrumentation_signals == (
        "metrics",
        "dashboard",
        "experiment",
        "conversion",
        "activation",
        "retention",
        "analytics_integration",
    )
    assert any("exposure, assignment, variant" in item for item in partial.required_event_definitions)
    assert any("control and treatment variants" in item for item in partial.validation_recommendations)

    missing = _finding(result, "task-missing")
    assert missing.readiness == "missing"
    assert missing.instrumentation_signals == ("attribution",)
    assert any("UTM property" in item for item in missing.required_event_definitions)
    assert any("regional privacy rules" in item for item in missing.privacy_cues)


def test_tags_metadata_paths_dependencies_and_validation_commands_contribute_evidence():
    result = summarize_task_analytics_instrumentation(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Implement product telemetry schema",
                    description="Emit typed product events.",
                    depends_on=["task-dashboard"],
                    files_or_modules={
                        "schema": "src/telemetry/schema/product_event_contract.py",
                        "duplicate": "src/telemetry/schema/product_event_contract.py",
                    },
                    acceptance_criteria={"qa": "GA4 debug view receives the signup event."},
                    metadata={
                        "tags": ["event tracking", "metrics"],
                        "tracking_plan": {
                            "event_name": "signup_completed",
                            "properties": ["plan_id", "workspace_id"],
                            "privacy": "no PII; consent checked",
                        },
                        "validation_commands": {"test": ["poetry run pytest tests/telemetry/test_event_schema.py"]},
                    },
                )
            ]
        )
    )

    finding = result.findings[0]

    assert finding.task_id == "task-metadata"
    assert finding.readiness == "ready"
    assert finding.instrumentation_signals == (
        "event_tracking",
        "metrics",
        "dashboard",
        "analytics_integration",
        "telemetry_schema",
    )
    assert finding.evidence.count("files_or_modules: src/telemetry/schema/product_event_contract.py") == 1
    assert "metadata.tracking_plan.event_name: event name: signup_completed" in finding.evidence
    assert "metadata.tracking_plan.privacy: no PII; consent checked" in finding.evidence
    assert "validation_commands: poetry run pytest tests/telemetry/test_event_schema.py" in finding.evidence
    assert result.summary["signal_counts"]["telemetry_schema"] == 1


def test_empty_invalid_no_signal_serialization_markdown_and_escaping_are_stable():
    task_dict = _task(
        "task-funnel | pipe",
        title="Activation funnel | dashboard",
        description="Create funnel dashboard panels and conversion metrics.",
        files_or_modules=["analytics/dashboards/activation_funnel.yml"],
    )
    original = copy.deepcopy(task_dict)

    result = build_task_analytics_instrumentation_plan(_plan([task_dict]))
    payload = task_analytics_instrumentation_plan_to_dict(result)
    markdown = task_analytics_instrumentation_plan_to_markdown(result)
    empty = build_task_analytics_instrumentation_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_analytics_instrumentation_plan(13)
    no_signal = build_task_analytics_instrumentation_plan(
        _plan([_task("task-ui", title="Add profile UI", description="Render profile settings.")])
    )

    assert task_dict == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["findings"]
    assert result.records == result.findings
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "instrumented_task_ids",
        "non_instrumentation_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "readiness",
        "instrumentation_signals",
        "required_event_definitions",
        "validation_recommendations",
        "privacy_cues",
        "dashboard_updates",
        "backfill_considerations",
        "evidence",
    ]
    assert markdown.startswith("# Task Analytics Instrumentation Plan: plan-analytics")
    assert "Summary: 1 instrumentation tasks" in markdown
    assert "Activation funnel \\| dashboard" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.findings == ()
    assert empty.summary["task_count"] == 0
    assert invalid.plan_id is None
    assert invalid.findings == ()
    assert no_signal.findings == ()
    assert no_signal.non_instrumentation_task_ids == ("task-ui",)
    assert "No analytics instrumentation findings were inferred." in no_signal.to_markdown()
    assert "Non-instrumentation tasks: task-ui" in no_signal.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Retention cohort dashboard",
        description="Build dashboard metrics for cohort retention and activation.",
        files_or_modules=["analytics/dashboards/retention_cohorts.yml"],
        acceptance_criteria=["Cohort chart validates retained users."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Segment event tracking",
            description="Track trial start conversion event with properties.",
            files_or_modules=["src/analytics/trial_start.ts"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_analytics_instrumentation_plan([object_task])
    task_result = build_task_analytics_instrumentation_plan(task_model)
    plan_result = build_task_analytics_instrumentation_plan(plan_model)

    assert iterable_result.findings[0].task_id == "task-object"
    assert "retention" in iterable_result.findings[0].instrumentation_signals
    assert task_result.findings[0].task_id == "task-model"
    assert task_result.findings[0].readiness == "partial"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.findings[0].task_id == "task-model"


def _finding(result, task_id):
    return next(finding for finding in result.findings if finding.task_id == task_id)


def _plan(tasks, plan_id="plan-analytics"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-analytics",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
