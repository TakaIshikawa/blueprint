import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_dark_launch_readiness import (
    TaskDarkLaunchReadinessPlan,
    TaskDarkLaunchReadinessRecommendation,
    build_task_dark_launch_readiness_plan,
    generate_task_dark_launch_readiness,
    recommend_task_dark_launch_readiness,
    summarize_task_dark_launch_readiness,
    task_dark_launch_readiness_plan_to_dict,
    task_dark_launch_readiness_plan_to_markdown,
    task_dark_launch_readiness_to_dicts,
)


def test_mapping_input_detects_dark_launch_signals_from_text_paths_and_metadata():
    result = build_task_dark_launch_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Prepare release plumbing",
                    description="Move implementation only.",
                    files_or_modules=[
                        "src/releases/dark_launch_checkout.py",
                        "src/shadow_mode/order_dual_run.py",
                        "config/traffic_mirror.yaml",
                    ],
                ),
                _task(
                    "task-metadata",
                    title="Run hidden rollout",
                    description="Silent release for the new search stack.",
                    risks=["Beta cohort could include production accounts."],
                    dependencies=["internal-only launch gate must land first"],
                    metadata={"release": {"mode": "hidden rollout", "audience": "private beta cohort"}},
                ),
            ]
        )
    )

    assert isinstance(result, TaskDarkLaunchReadinessPlan)
    assert result.plan_id == "plan-dark-launch-readiness"
    by_id = {record.task_id: record for record in result.recommendations}
    assert set(by_id) == {"task-paths", "task-metadata"}
    assert {"dark_launch", "shadow_mode", "traffic_mirroring"} <= set(
        by_id["task-paths"].dark_launch_signals
    )
    assert {"silent_release", "hidden_rollout", "internal_only_launch", "beta_cohort"} <= set(
        by_id["task-metadata"].dark_launch_signals
    )
    assert "rollback_trigger" in by_id["task-paths"].missing_safeguards
    assert any("files_or_modules" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.release.mode" in item for item in by_id["task-metadata"].evidence)


def test_recommendations_include_only_missing_safeguards_with_evidence_snippets():
    result = build_task_dark_launch_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Shadow mode account scoring",
                    description="Run account scoring in shadow mode with no writes.",
                    acceptance_criteria=[
                        "Audience isolation uses allowlist only targeting rules.",
                        "Telemetry comparison checks parity metrics against the current scorer.",
                        "Rollback trigger uses a kill switch.",
                        "Data write safety keeps the task read-only and suppresses writes.",
                        "Success metric review happens at the promotion gate.",
                    ],
                )
            ]
        )
    )

    record = result.recommendations[0]
    assert record.missing_safeguards == ("support_visibility",)
    assert record.risk_level == "medium"
    assert any(item.startswith("title: Shadow mode account scoring") for item in record.evidence)
    assert any("description: Run account scoring in shadow mode" in item for item in record.evidence)
    assert result.summary["missing_safeguard_counts"]["support_visibility"] == 1
    assert result.summary["missing_safeguard_counts"]["rollback_trigger"] == 0


def test_execution_plan_input_risk_scoring_and_sorting_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-medium",
                    title="Internal-only release notes preview",
                    description="Internal-only launch with allowlist only audience isolation.",
                    acceptance_criteria=[
                        "Rollback trigger uses a kill switch.",
                        "Data write safety keeps the preview read-only.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Mirror checkout traffic",
                    description="Traffic mirroring will parallel run checkout and may persist audit writes.",
                ),
                _task(
                    "task-low",
                    title="Dark launch completed safeguards",
                    description="Dark launch the profile card.",
                    acceptance_criteria=[
                        "Audience isolation uses internal only audience targeting rules.",
                        "Telemetry comparison reviews old vs new parity metrics.",
                        "Rollback trigger uses a kill switch.",
                        "Support visibility includes support runbook notes.",
                        "Data write safety is read-only with no writes.",
                        "Success metric review happens at go/no-go.",
                    ],
                ),
            ]
        )
    )

    result = build_task_dark_launch_readiness_plan(plan)

    assert result.dark_launch_task_ids == ("task-high", "task-medium", "task-low")
    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-high"].risk_level == "high"
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-low"].risk_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert {"traffic_mirroring"} <= set(by_id["task-high"].dark_launch_signals)
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 1}


def test_no_match_and_empty_plans_return_empty_recommendations_with_summary_counts():
    no_match = build_task_dark_launch_readiness_plan(
        _plan([_task("task-docs", title="Update settings docs", description="Document settings only.")])
    )
    empty = build_task_dark_launch_readiness_plan(_plan([]))

    assert no_match.recommendations == ()
    assert no_match.dark_launch_task_ids == ()
    assert no_match.suppressed_task_ids == ("task-docs",)
    assert no_match.summary["task_count"] == 1
    assert no_match.summary["dark_launch_task_count"] == 0
    assert no_match.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert empty.recommendations == ()
    assert empty.suppressed_task_ids == ()
    assert empty.summary["task_count"] == 0
    assert empty.summary["dark_launch_task_count"] == 0
    assert empty.to_markdown().endswith("No dark-launch readiness recommendations were inferred.")
    assert recommend_task_dark_launch_readiness({"tasks": "not a list"}) == ()
    assert recommend_task_dark_launch_readiness(None) == ()


def test_serialization_no_mutation_and_alias_compatibility_are_stable():
    source = _plan(
        [
            _task(
                "task-z",
                title="Silent rollout for notifications",
                description="Quiet rollout for beta users.",
                metadata={
                    "safeguards": {
                        "audience_isolation": "allowlist only",
                        "telemetry_comparison": "compare telemetry",
                        "rollback_trigger": "kill switch",
                        "support_visibility": "support runbook",
                        "data_write_safety": "read-only dry run",
                        "success_metric_review": "metric sign-off",
                    }
                },
            ),
            _task(
                "task-a",
                title="Dark launch checkout",
                description="Dark launch checkout in shadow mode.",
            ),
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = build_task_dark_launch_readiness_plan(model)
    alias_result = summarize_task_dark_launch_readiness(source)
    records = recommend_task_dark_launch_readiness(model)
    generated = generate_task_dark_launch_readiness(source)
    payload = task_dark_launch_readiness_plan_to_dict(result)
    markdown = task_dark_launch_readiness_plan_to_markdown(result)

    assert source == original
    assert isinstance(result.recommendations[0], TaskDarkLaunchReadinessRecommendation)
    assert alias_result.to_dict() == build_task_dark_launch_readiness_plan(source).to_dict()
    assert records == result.recommendations
    assert generated == build_task_dark_launch_readiness_plan(source).recommendations
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["recommendations"]
    assert task_dark_launch_readiness_to_dicts(records) == payload["recommendations"]
    assert task_dark_launch_readiness_to_dicts(result) == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "dark_launch_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "dark_launch_signals",
        "missing_safeguards",
        "risk_level",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Dark Launch Readiness: plan-dark-launch-readiness")


def _plan(tasks):
    return {
        "id": "plan-dark-launch-readiness",
        "implementation_brief_id": "brief-dark-launch-readiness",
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
    risks=None,
    dependencies=None,
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if risks is not None:
        task["risks"] = risks
    if dependencies is not None:
        task["dependencies"] = dependencies
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
