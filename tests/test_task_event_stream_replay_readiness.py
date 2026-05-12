import json

from blueprint.task_event_stream_replay_readiness import (
    analyze_task_event_stream_replay_readiness,
    build_task_event_stream_replay_readiness_plan,
    task_event_stream_replay_readiness_plan_to_dict,
    task_event_stream_replay_readiness_plan_to_dicts,
    task_event_stream_replay_readiness_plan_to_markdown,
)


def test_complete_event_stream_replay_task_is_ready():
    result = build_task_event_stream_replay_readiness_plan(
        _plan(
            [
                _task(
                    "replay-ready",
                    "Replay invoice events",
                    (
                        "Event replay for stream catch-up and consumer lag recovery. Replay scope covers topic, "
                        "partition, tenant, and time window. Idempotency uses dedupe keys. Ordering preserves "
                        "partition order. Checkpoint handling records offsets and resume points. Monitoring "
                        "dashboards include alerts and abort stop conditions."
                    ),
                    ["src/streams/replay_offsets/backfill.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == ("replay_scope", "idempotency", "ordering", "checkpoint_handling", "monitoring_abort")
    assert record.missing_criteria == ()


def test_detects_replay_offset_lag_backfill_and_reprocessing_paths():
    result = analyze_task_event_stream_replay_readiness(
        _plan(
            [
                _task("offset", "Offset reset", "Reset offsets for lagging consumer.", ["src/consumer_offsets/reset.py"]),
                _task("backfill", "Event backfill", "Backfill historical events and reprocess failures.", ["src/streams/backfill/reprocess.py"]),
                _task("docs", "Docs", "No event replay changes are required.", []),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("backfill", "offset")
    assert result.ignored_task_ids == ("docs",)
    assert "offset_reset" in by_id["offset"].detected_signals
    assert "event_backfill" in by_id["backfill"].detected_signals
    assert "reprocessing" in by_id["backfill"].detected_signals


def test_serialization_and_markdown_are_stable():
    result = build_task_event_stream_replay_readiness_plan(_plan([_task("alias", "Replay", "Replay events with idempotency.", ["streams/replay.py"])]))
    payload = task_event_stream_replay_readiness_plan_to_dict(result)

    assert task_event_stream_replay_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Event Stream Replay Readiness: plan-event-stream-replay" in task_event_stream_replay_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-event-stream-replay", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
