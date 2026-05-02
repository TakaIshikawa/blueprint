import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_realtime_collaboration_conflict_readiness import (
    TaskRealtimeCollaborationConflictReadinessPlan,
    TaskRealtimeCollaborationConflictReadinessRecord,
    analyze_task_realtime_collaboration_conflict_readiness,
    build_task_realtime_collaboration_conflict_readiness_plan,
    extract_task_realtime_collaboration_conflict_readiness,
    generate_task_realtime_collaboration_conflict_readiness,
    recommend_task_realtime_collaboration_conflict_readiness,
    task_realtime_collaboration_conflict_readiness_plan_to_dict,
    task_realtime_collaboration_conflict_readiness_plan_to_dicts,
    task_realtime_collaboration_conflict_readiness_plan_to_markdown,
)


def test_detects_realtime_collaboration_categories_from_task_fields_paths_and_metadata():
    result = build_task_realtime_collaboration_conflict_readiness_plan(
        _plan(
            [
                _task(
                    "task-presence",
                    title="Add websocket presence and live cursors",
                    description="Realtime websocket presence shows online users and typing indicators.",
                    files_or_modules=["src/realtime/presence_live_cursors.ts"],
                    acceptance_criteria=["Reconnect replay catches up missed events after disconnects."],
                ),
                _task(
                    "task-edit",
                    title="Support collaborative document editing",
                    description="Concurrent edits and concurrent writes update a shared document.",
                    files_or_modules=["src/collaboration/shared_doc_editor.ts"],
                    validation_plan=["Multi-client offline merge tests cover reconnect conflicts."],
                ),
                _task(
                    "task-sync",
                    title="Queue optimistic local-first updates",
                    description="Optimistic UI stores pending operations in an outbox for offline mode.",
                    metadata={
                        "version_vectors": "Version vectors define causal merge conflict resolution.",
                        "transport": {"ordering_guarantee": "Sequence numbers and operation ids are required."},
                    },
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert isinstance(result, TaskRealtimeCollaborationConflictReadinessPlan)
    assert all(
        isinstance(record, TaskRealtimeCollaborationConflictReadinessRecord)
        for record in result.records
    )
    assert by_id["task-presence"].categories == ("realtime_transport", "presence_state")
    assert by_id["task-edit"].categories == ("concurrent_editing", "offline_reconcile")
    assert by_id["task-sync"].categories == (
        "optimistic_sync",
        "conflict_resolution",
        "offline_reconcile",
    )
    assert by_id["task-presence"].present_safeguards == ("reconnect_replay",)
    assert by_id["task-edit"].present_safeguards == ("offline_merge_tests",)
    assert by_id["task-sync"].present_safeguards == (
        "conflict_resolution_strategy",
        "ordering_guarantee",
    )
    assert "files_or_modules: src/realtime/presence_live_cursors.ts" in by_id["task-presence"].evidence
    assert any("validation_plan[0]" in item for item in by_id["task-edit"].evidence)
    assert any("metadata.version_vectors" in item for item in by_id["task-sync"].evidence)
    assert result.summary["category_counts"]["offline_reconcile"] == 2
    assert result.summary["realtime_task_count"] == 3


def test_conflict_heavy_task_without_safeguards_is_high_risk():
    result = build_task_realtime_collaboration_conflict_readiness_plan(
        _plan(
            [
                _task(
                    "task-conflict",
                    title="Enable collaborative editor conflicts",
                    description=(
                        "Collaborative editing allows simultaneous edits, merge conflict handling, "
                        "optimistic updates, and concurrent writes."
                    ),
                    files_or_modules=["src/editor/concurrent_editing.ts"],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.risk_level == "high"
    assert record.missing_safeguards == (
        "server_authority",
        "conflict_resolution_strategy",
        "reconnect_replay",
        "ordering_guarantee",
        "offline_merge_tests",
        "observability",
    )
    assert record.recommended_checks == tuple(
        [
            "Define the authoritative write path and server-side validation for collaborative state changes.",
            "Specify the merge, CRDT, operational transform, or version-vector strategy for concurrent edits.",
            "Cover reconnect replay or catch-up behavior for missed realtime events and queued local changes.",
            "Add ordering, idempotency, acknowledgement, or deduplication guarantees for operations.",
            "Add tests for offline edits, reconnect merges, and multi-client conflict scenarios.",
            "Instrument conflict rates, dropped events, reconnects, sync lag, and failed merges.",
        ]
    )
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_detected_safeguards_lower_risk_and_missing_recommendations_are_deterministic():
    result = analyze_task_realtime_collaboration_conflict_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship collaborative offline editor",
                    description="Concurrent editing uses optimistic updates and offline edits.",
                    acceptance_criteria=[
                        "Authoritative server validates the canonical server state.",
                        "Conflict resolution strategy uses CRDT and version vectors.",
                        "Reconnect replay backfills missed events and queued mutation replay.",
                        "Ordering guarantee uses monotonic sequence numbers and operation ids.",
                        "Offline merge tests cover multi-client reconnect conflicts.",
                        "Observability tracks conflict rate, sync lag, dropped events, and alerts.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.present_safeguards == (
        "server_authority",
        "conflict_resolution_strategy",
        "reconnect_replay",
        "ordering_guarantee",
        "offline_merge_tests",
        "observability",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert record.risk_level == "low"
    assert result.summary["missing_safeguards_count"] == 0


def test_empty_no_impact_and_invalid_inputs_have_stable_empty_markdown_behavior():
    result = build_task_realtime_collaboration_conflict_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update editor empty state copy",
                    description=(
                        "Change placeholder labels with no realtime collaboration or concurrent edits "
                        "in scope."
                    ),
                    files_or_modules=["src/ui/editor_empty_state.tsx"],
                )
            ]
        )
    )
    invalid = build_task_realtime_collaboration_conflict_readiness_plan(42)

    assert result.records == ()
    assert result.recommendations == ()
    assert result.findings == ()
    assert result.realtime_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "realtime_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguards_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "category_counts": {
            "realtime_transport": 0,
            "presence_state": 0,
            "concurrent_editing": 0,
            "optimistic_sync": 0,
            "conflict_resolution": 0,
            "offline_reconcile": 0,
        },
        "missing_safeguard_counts": {
            "server_authority": 0,
            "conflict_resolution_strategy": 0,
            "reconnect_replay": 0,
            "ordering_guarantee": 0,
            "offline_merge_tests": 0,
            "observability": 0,
        },
        "realtime_task_ids": [],
    }
    assert "No realtime collaboration conflict readiness records" in result.to_markdown()
    assert "Not applicable tasks: task-copy" in result.to_markdown()
    assert invalid.records == ()
    assert invalid.to_markdown().startswith("# Task Realtime Collaboration Conflict Readiness")


def test_serialization_markdown_alias_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Presence updates | live cursors",
                description="Realtime websocket live cursors use reconnect replay.",
            ),
            _task(
                "task-a",
                title="Concurrent document writes",
                description="Concurrent writes use CRDT merge strategy and ordered delivery.",
            ),
            _task("task-copy", title="Update copy", description="Change button text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = recommend_task_realtime_collaboration_conflict_readiness(plan)
    payload = task_realtime_collaboration_conflict_readiness_plan_to_dict(result)
    markdown = task_realtime_collaboration_conflict_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_realtime_collaboration_conflict_readiness_plan_to_dicts(result) == payload["records"]
    assert task_realtime_collaboration_conflict_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_realtime_collaboration_conflict_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_realtime_collaboration_conflict_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.findings == result.records
    assert result.realtime_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
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
        "categories",
        "risk_level",
        "present_safeguards",
        "missing_safeguards",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Realtime Collaboration Conflict Readiness: plan-realtime")
    assert "Presence updates \\| live cursors" in markdown
    assert "| Task | Title | Risk | Categories | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


def test_execution_plan_execution_task_iterable_and_object_inputs_are_supported():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Add model presence",
            description="Websocket presence publishes live cursors and reconnect replay.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-plan",
                    title="Plan concurrent editing",
                    description="Collaborative editing uses operational transform and sequence numbers.",
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_realtime_collaboration_conflict_readiness_plan(
        [
            _task(
                "task-iter",
                title="Iter optimistic sync",
                description="Optimistic updates queue pending operations for offline reconciliation.",
            )
        ]
    )
    object_result = build_task_realtime_collaboration_conflict_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Object conflict readiness",
            description="Merge conflict handling supports concurrent edits.",
            acceptance_criteria=["Server authority and ordering guarantee are documented."],
        )
    )

    task_result = build_task_realtime_collaboration_conflict_readiness_plan(task_model)
    plan_result = build_task_realtime_collaboration_conflict_readiness_plan(plan_model)

    assert task_result.plan_id is None
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].categories == ("realtime_transport", "presence_state")
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-plan"
    assert plan_result.records[0].present_safeguards == (
        "conflict_resolution_strategy",
        "ordering_guarantee",
    )
    assert iterable_result.realtime_task_ids == ("task-iter",)
    assert object_result.records[0].present_safeguards == (
        "server_authority",
        "ordering_guarantee",
    )


def _plan(tasks, plan_id="plan-realtime"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-realtime",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_plan=None,
    validation_commands=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-realtime",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
