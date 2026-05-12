from blueprint.task_shadow_traffic_readiness import (
    build_task_shadow_traffic_readiness_plan,
    recommend_task_shadow_traffic_readiness,
)


def test_detects_shadow_traffic_dark_reads_and_replay_tasks():
    result = build_task_shadow_traffic_readiness_plan(
        _plan(
            [
                _task(
                    "shadow",
                    "Shadow traffic checkout",
                    "Mirror production checkout requests in shadow mode.",
                    files_or_modules=["src/rollout/traffic_mirror.py"],
                ),
                _task("dark", "Dark read search", "Run dark reads against the new search index."),
                _task("replay", "Replay rollout", "Replay recorded traffic through the parser."),
            ]
        )
    )

    assert result.impacted_task_ids == ("dark", "replay", "shadow")
    assert {record.task_id for record in result.records} == {"shadow", "dark", "replay"}
    assert result.summary["impacted_task_count"] == 3


def test_scores_scope_isolation_metrics_kill_switch_and_observability():
    result = build_task_shadow_traffic_readiness_plan(
        _plan(
            [
                _task(
                    "ready",
                    "Mirrored reads rollout",
                    "Mirror read requests for allowlisted tenants only.",
                    acceptance_criteria=[
                        "Scope is limited to 5 percent of search endpoints.",
                        "Data isolation is read-only with no writes or side effects.",
                        "Comparison metrics track parity, mismatch rate, latency, and error rate.",
                        "Kill switch feature flag can disable mirrored traffic.",
                        "Observability dashboards, traces, logs, and alerts cover the shadow run.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_criteria == (
        "mirroring_scope",
        "data_isolation",
        "comparison_metrics",
        "kill_switch",
        "observability",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_incomplete_and_unrelated_tasks_return_actionable_missing_messages():
    result = build_task_shadow_traffic_readiness_plan(
        _plan(
            [
                _task("partial", "Parallel run scoring", "Parallel run scoring with compare parity metrics."),
                _task("docs", "Docs", "Update release notes only."),
            ]
        )
    )

    record = result.records[0]
    assert record.task_id == "partial"
    assert record.readiness == "partial"
    assert record.missing_criteria == (
        "mirroring_scope",
        "data_isolation",
        "kill_switch",
        "observability",
    )
    assert all(action for action in record.recommended_follow_up_actions)
    assert result.ignored_task_ids == ("docs",)
    assert recommend_task_shadow_traffic_readiness({"tasks": []}) == ()


def _plan(tasks):
    return {"id": "plan-shadow", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}

