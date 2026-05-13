import json

from blueprint.task_legal_hold_readiness import (
    analyze_task_legal_hold_readiness,
    build_task_legal_hold_readiness_plan,
    extract_task_legal_hold_readiness,
    task_legal_hold_readiness_plan_to_dict,
    task_legal_hold_readiness_plan_to_dicts,
    task_legal_hold_readiness_plan_to_markdown,
)


def test_complete_legal_hold_task_is_ready():
    result = build_task_legal_hold_readiness_plan(
        _plan(
            [
                _task(
                    "hold-ready",
                    "Place legal hold",
                    (
                        "Legal hold placement preserves records for e-discovery. Counsel authorization approves "
                        "the requester and hold scope. Audit trail stores evidence, actors, timestamps, and chain "
                        "of custody. Release workflow lifts hold only with legal approval. Purge-blocking checks "
                        "and deletion guards skip held records. Custodian scope covers employee mailboxes and "
                        "account datasets. Retention conflict handling documents override retention policy. "
                        "Hold notice notifications go to custodians. Verification tests prove purge suppression."
                    ),
                    ["src/compliance/legal_hold/purge_suppression.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.detected_signals == (
        "legal_hold",
        "preservation",
        "purge_suppression",
        "custodian_scope",
        "retention_override",
    )
    assert record.present_criteria == (
        "authorization",
        "audit_trail",
        "release_workflow",
        "purge_blocking_checks",
        "custodian_scope",
        "retention_conflict_handling",
        "notification",
        "verification",
    )
    assert record.missing_criteria == ()


def test_partial_hold_plan_reports_missing_safeguards_and_ignored_tasks():
    result = analyze_task_legal_hold_readiness(
        _plan(
            [
                _task(
                    "hold-partial",
                    "Litigation hold purge block",
                    "Litigation hold must block purge jobs and apply to custodian accounts.",
                    ["jobs/purge_block_litigation_hold.py"],
                ),
                _task("copy", "Docs", "No legal hold changes are required for this docs task.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("hold-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert {"litigation_hold", "purge_suppression", "custodian_scope"} <= set(record.detected_signals)
    assert record.present_criteria == ("purge_blocking_checks", "custodian_scope")
    assert "authorization" in record.missing_criteria
    assert "audit_trail" in record.missing_criteria
    assert "release_workflow" in record.missing_criteria
    assert "notification" in record.missing_criteria
    assert "verification" in record.missing_criteria
    assert any(action.startswith("Define who can request") for action in record.recommended_follow_up_actions)


def test_multiple_input_shapes_and_file_evidence_are_supported():
    object_result = extract_task_legal_hold_readiness(
        [
            {
                "task_id": "object-hold",
                "title": "Preservation override",
                "description": "Preservation requires retention override and an audit log.",
                "files_or_modules": ["compliance/retention_override.py"],
            }
        ]
    )
    string_result = build_task_legal_hold_readiness_plan("Add legal hold release workflow and verification.")

    assert object_result.records[0].task_id == "object-hold"
    assert "retention_override" in object_result.records[0].detected_signals
    assert any("files_or_modules: compliance/retention_override.py" in item for item in object_result.records[0].evidence)
    assert string_result.records[0].task_id == "task-1"
    assert string_result.records[0].present_criteria == ("release_workflow", "verification")


def test_unrelated_tasks_and_negated_hold_scope_are_ignored():
    result = build_task_legal_hold_readiness_plan(
        _plan(
            [
                _task("retention-copy", "Retention docs", "Refresh retention policy copy.", []),
                _task("no-hold", "No hold work", "No litigation hold or preservation changes are needed.", []),
            ]
        )
    )

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("retention-copy", "no-hold")


def test_serialization_and_markdown_are_stable():
    result = build_task_legal_hold_readiness_plan(
        _plan([_task("alias", "Legal hold", "Legal hold must keep an audit trail.", [])])
    )
    payload = task_legal_hold_readiness_plan_to_dict(result)

    assert task_legal_hold_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Legal Hold Readiness: plan-legal-hold" in task_legal_hold_readiness_plan_to_markdown(result)
    assert "| `alias` | Legal hold |" in task_legal_hold_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-legal-hold", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
