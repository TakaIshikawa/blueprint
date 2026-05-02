import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_realtime_reconnect_readiness import (
    TaskRealtimeReconnectReadinessPlan,
    TaskRealtimeReconnectReadinessRecord,
    build_task_realtime_reconnect_readiness_plan,
    generate_task_realtime_reconnect_readiness,
    recommend_task_realtime_reconnect_readiness,
    summarize_task_realtime_reconnect_readiness,
    task_realtime_reconnect_readiness_plan_to_dict,
    task_realtime_reconnect_readiness_plan_to_markdown,
    task_realtime_reconnect_readiness_to_dicts,
)


def test_realtime_tasks_produce_ranked_readiness_records_from_text_paths_and_metadata():
    result = build_task_realtime_reconnect_readiness_plan(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add WebSocket presence live updates",
                    description="WebSocket presence stream handles reconnect after network loss.",
                    acceptance_criteria=[
                        "Reconnect backoff uses capped exponential backoff with jitter.",
                        "Heartbeat timeout detects missed ping/pong keepalive failures.",
                        "Duplicate event handling uses event id sequence numbers and idempotent replay.",
                        "Stale subscription cleanup unsubscribes orphaned presence listeners.",
                        "Offline resume behavior uses resume token and last seen event recovery.",
                    ],
                ),
                _task(
                    "task-missing",
                    title="Build SSE live feed",
                    description="Server-sent events streaming endpoint publishes live updates.",
                    files_or_modules=["src/realtime/sse_live_feed.py"],
                ),
                _task(
                    "task-partial",
                    title="Subscription reconnect handling",
                    description="Subscription reconnect flow with heartbeat interval and duplicate event dedupe.",
                    metadata={"realtime": {"surface": "pub/sub subscription", "mode": "offline resume"}},
                ),
            ]
        )
    )

    assert isinstance(result, TaskRealtimeReconnectReadinessPlan)
    assert result.plan_id == "plan-realtime-reconnect-readiness"
    assert result.realtime_task_ids == ("task-missing", "task-partial", "task-strong")
    assert result.summary["readiness_counts"] == {"strong": 1, "partial": 1, "missing": 1}
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-missing"].readiness == "missing"
    assert {"sse", "streaming", "live_updates"} <= set(by_id["task-missing"].matched_realtime_signals)
    assert by_id["task-missing"].missing_safeguards == (
        "reconnect_backoff",
        "heartbeat_timeout",
        "duplicate_event_handling",
        "stale_subscription_cleanup",
        "offline_resume_behavior",
    )
    assert by_id["task-partial"].readiness == "partial"
    assert {"subscription", "reconnect", "heartbeat", "offline"} <= set(
        by_id["task-partial"].matched_realtime_signals
    )
    assert by_id["task-strong"].readiness == "strong"
    assert by_id["task-strong"].missing_safeguards == ()
    assert any("files_or_modules" in item for item in by_id["task-missing"].evidence)
    assert any("metadata.realtime.mode" in item for item in by_id["task-partial"].evidence)


def test_recommended_checks_are_deterministic_for_missing_reconnect_safeguards():
    result = build_task_realtime_reconnect_readiness_plan(
        _plan(
            [
                _task(
                    "task-live",
                    title="Add live updates over WebSocket",
                    description="WebSocket live updates for active dashboards.",
                    acceptance_criteria=[
                        "Heartbeat timeout detects missed ping/pong failures.",
                        "Stale subscription cleanup removes orphaned channel listeners.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert isinstance(record, TaskRealtimeReconnectReadinessRecord)
    assert record.readiness == "partial"
    assert record.missing_safeguards == (
        "reconnect_backoff",
        "duplicate_event_handling",
        "offline_resume_behavior",
    )
    assert record.recommended_checks == (
        "Verify reconnect backoff uses capped exponential delay with jitter and retry limits.",
        "Validate duplicate event handling with event IDs, sequence numbers, or idempotent replay.",
        "Test offline/resume behavior for network loss, resume cursors, and missed event recovery.",
    )
    assert result.summary["missing_safeguard_counts"]["heartbeat_timeout"] == 0
    assert result.summary["missing_safeguard_counts"]["reconnect_backoff"] == 1


def test_non_realtime_and_malformed_inputs_return_empty_recommendation_sets():
    no_match = build_task_realtime_reconnect_readiness_plan(
        _plan([_task("task-docs", title="Update settings docs", description="Document settings only.")])
    )
    malformed = build_task_realtime_reconnect_readiness_plan({"tasks": "not a list"})

    assert no_match.records == ()
    assert no_match.recommendations == ()
    assert no_match.realtime_task_ids == ()
    assert no_match.not_applicable_task_ids == ("task-docs",)
    assert no_match.summary["task_count"] == 1
    assert no_match.summary["realtime_task_count"] == 0
    assert no_match.summary["readiness_counts"] == {"strong": 0, "partial": 0, "missing": 0}
    assert malformed.records == ()
    assert malformed.summary["task_count"] == 0
    assert generate_task_realtime_reconnect_readiness({"tasks": "not a list"}) == ()
    assert recommend_task_realtime_reconnect_readiness("not a plan") == ()
    assert generate_task_realtime_reconnect_readiness(None) == ()


def test_serializers_include_metadata_rows_and_stable_empty_state_without_mutation():
    source = _plan(
        [
            _task(
                "task-row | pipe",
                title="Presence reconnect | resume",
                description="Presence live updates use WebSocket reconnect.",
                metadata={
                    "safeguards": {
                        "reconnect_backoff": "exponential backoff with jitter",
                        "heartbeat_timeout": "heartbeat timeout after missed pongs",
                        "duplicate_event_handling": "event id dedupe",
                        "stale_subscription_cleanup": "unsubscribe stale subscriptions",
                        "offline_resume_behavior": "resume token and offline queue",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = build_task_realtime_reconnect_readiness_plan(model)
    alias_result = summarize_task_realtime_reconnect_readiness(source)
    generated = generate_task_realtime_reconnect_readiness(model)
    payload = task_realtime_reconnect_readiness_plan_to_dict(result)
    markdown = task_realtime_reconnect_readiness_plan_to_markdown(result)
    empty = build_task_realtime_reconnect_readiness_plan({"id": "empty-plan", "tasks": []})

    assert source == original
    assert alias_result.to_dict() == build_task_realtime_reconnect_readiness_plan(source).to_dict()
    assert generated == result.records
    assert result.recommendations == result.records
    assert result.to_dicts() == payload["records"]
    assert task_realtime_reconnect_readiness_to_dicts(result) == payload["records"]
    assert task_realtime_reconnect_readiness_to_dicts(generated) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "realtime_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_realtime_signals",
        "readiness",
        "missing_safeguards",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Realtime Reconnect Readiness: plan-realtime-reconnect-readiness")
    assert "Presence reconnect \\| resume" in markdown
    assert "Summary: 1 realtime tasks across 1 total tasks" in markdown
    assert empty.to_markdown().endswith("No realtime reconnect readiness recommendations were inferred.")


def _plan(tasks):
    return {
        "id": "plan-realtime-reconnect-readiness",
        "implementation_brief_id": "brief-realtime-reconnect-readiness",
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
