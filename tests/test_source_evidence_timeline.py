from copy import deepcopy
import json

from blueprint.domain.models import ExecutionPlan, SourceBrief
from blueprint.source_evidence_timeline import (
    SourceEvidenceTimelineGroup,
    build_source_evidence_timeline,
    source_evidence_timeline_to_dicts,
)


def test_groups_evidence_by_requirement_and_preserves_source_ids():
    timeline = build_source_evidence_timeline(
        source_briefs=[
            _source_brief(
                "sb-legacy",
                source_id="JIRA-1",
                evidence=[
                    {
                        "source_id": "jira-comment-1",
                        "requirement_id": "req-checkout-retry",
                        "claim": "Checkout retries are required for failed card submissions.",
                        "observed_at": "2026-04-10T09:30:00Z",
                    }
                ],
            )
        ],
        imported_notes=[
            {
                "id": "note-1",
                "evidence": [
                    {
                        "source_id": "note-1#retry",
                        "requirement_ref": "req-checkout-retry",
                        "text": "Support confirms checkout retry messaging is still needed.",
                        "date": "2026-04-12",
                    }
                ],
            }
        ],
    )

    assert isinstance(timeline[0], SourceEvidenceTimelineGroup)
    assert len(timeline) == 1
    group = timeline[0]
    assert group.reference_id == "req-checkout-retry"
    assert group.reference_type == "requirement"
    assert [entry.source_id for entry in group.entries] == [
        "jira-comment-1",
        "note-1#retry",
    ]
    assert [entry.evidence_date for entry in group.entries] == [
        "2026-04-10",
        "2026-04-12",
    ]
    assert group.current_source_ids == ("jira-comment-1", "note-1#retry")


def test_plan_task_metadata_evidence_is_grouped_by_task_reference():
    timeline = build_source_evidence_timeline(
        execution_plan=_plan(
            tasks=[
                _task(
                    "task-api",
                    metadata={
                        "source_evidence": [
                            {
                                "source_id": "decision-42",
                                "claim": "Billing API owns retry policy validation.",
                                "captured_at": "2026-04-18",
                            }
                        ]
                    },
                )
            ]
        )
    )

    assert len(timeline) == 1
    assert timeline[0].reference_id == "task-api"
    assert timeline[0].reference_type == "task"
    assert timeline[0].entries[0].source_kind == "plan_task"
    assert timeline[0].entries[0].source_id == "decision-42"


def test_superseded_evidence_marks_older_entry_stale():
    timeline = build_source_evidence_timeline(
        source_briefs=[
            _source_brief(
                "sb-source",
                evidence=[
                    {
                        "source_id": "brief-v1",
                        "requirement_id": "req-export",
                        "claim": "CSV export only needs task title and owner.",
                        "date": "2026-03-01",
                    },
                    {
                        "source_id": "brief-v2",
                        "requirement_id": "req-export",
                        "claim": "CSV export must include title, owner, and status.",
                        "date": "2026-04-01",
                        "supersedes": "brief-v1",
                    },
                ],
            )
        ]
    )

    group = timeline[0]

    assert [(entry.source_id, entry.status) for entry in group.entries] == [
        ("brief-v1", "superseded"),
        ("brief-v2", "current"),
    ]
    assert group.superseded_source_ids == ("brief-v1",)
    assert group.entries[1].supersedes_source_ids == ("brief-v1",)


def test_newer_contradictory_update_flags_both_entries():
    timeline = build_source_evidence_timeline(
        imported_notes=[
            {
                "id": "note-a",
                "evidence": [
                    {
                        "source_id": "slack-1",
                        "task_id": "task-rollout",
                        "text": "Beta rollout must include email notifications.",
                        "date": "2026-04-02",
                    }
                ],
            },
            {
                "id": "note-b",
                "evidence": [
                    {
                        "source_id": "slack-2",
                        "task_id": "task-rollout",
                        "text": "Beta rollout must not include email notifications.",
                        "date": "2026-04-04",
                    }
                ],
            },
        ]
    )

    group = timeline[0]

    assert [(entry.source_id, entry.status) for entry in group.entries] == [
        ("slack-1", "contradictory"),
        ("slack-2", "contradictory"),
    ]
    assert group.entries[0].contradicted_by_source_ids == ("slack-2",)
    assert group.entries[1].contradicts_source_ids == ("slack-1",)
    assert group.contradictory_source_ids == ("slack-1", "slack-2")


def test_explicit_contradiction_fields_are_preserved_in_structured_output():
    timeline = build_source_evidence_timeline(
        source_briefs=[
            _source_brief(
                "sb-source",
                evidence=[
                    {
                        "source_id": "research-old",
                        "requirement_id": "req-dashboard",
                        "claim": "Dashboard empty state should use long explanatory copy.",
                        "date": "2026-01-10",
                    },
                    {
                        "source_id": "research-new",
                        "requirement_id": "req-dashboard",
                        "claim": "Dashboard empty state should use short actionable copy.",
                        "date": "2026-04-10",
                        "contradicts_source_ids": ["research-old"],
                    },
                ],
            )
        ]
    )

    assert timeline[0].entries[1].contradicts_source_ids == ("research-old",)
    assert timeline[0].entries[0].contradicted_by_source_ids == ("research-new",)


def test_undated_entries_remain_deterministic_after_dated_entries():
    timeline = build_source_evidence_timeline(
        source_briefs=[
            _source_brief(
                "sb-sort",
                evidence=[
                    {
                        "source_id": "undated-b",
                        "requirement_id": "req-sort",
                        "claim": "Second undated note.",
                    },
                    {
                        "source_id": "dated",
                        "requirement_id": "req-sort",
                        "claim": "Dated note.",
                        "date": "2026-02-01",
                    },
                    {
                        "source_id": "undated-a",
                        "requirement_id": "req-sort",
                        "claim": "First undated note.",
                    },
                ],
            )
        ]
    )

    assert [(entry.source_id, entry.status) for entry in timeline[0].entries] == [
        ("dated", "current"),
        ("undated-b", "undated"),
        ("undated-a", "undated"),
    ]


def test_model_inputs_serialize_without_mutation():
    source_brief = _source_brief(
        "sb-model",
    )
    source_brief["source_payload"]["evidence"] = [
        {
            "source_id": "source-model",
            "requirement_id": "req-model",
            "claim": "Model inputs are accepted.",
            "date": "2026-04-20",
        }
    ]
    plan = _plan(
        tasks=[
            _task(
                "task-model",
                metadata={
                    "evidence": [
                        {
                            "source_id": "plan-model",
                            "text": "Plan evidence is accepted.",
                            "date": "2026-04-21",
                        }
                    ]
                },
            )
        ]
    )
    original_source = deepcopy(source_brief)
    original_plan = deepcopy(plan)

    timeline = build_source_evidence_timeline(
        SourceBrief.model_validate(source_brief),
        plan=ExecutionPlan.model_validate(plan),
    )
    payload = source_evidence_timeline_to_dicts(timeline)

    assert source_brief == original_source
    assert plan == original_plan
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "reference_id",
        "reference_type",
        "entries",
        "current_source_ids",
        "superseded_source_ids",
        "contradictory_source_ids",
    ]
    assert list(payload[0]["entries"][0]) == [
        "reference_id",
        "reference_type",
        "source_id",
        "source_kind",
        "label",
        "text",
        "evidence_date",
        "status",
        "supersedes_source_ids",
        "contradicted_by_source_ids",
        "contradicts_source_ids",
    ]


def _source_brief(
    source_brief_id,
    *,
    source_id=None,
    evidence=None,
):
    brief = {
        "id": source_brief_id,
        "title": "Checkout Retry",
        "domain": "payments",
        "summary": "Retry failed checkout submissions.",
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": source_id or source_brief_id,
        "source_payload": {"normalized": {"title": "Checkout Retry"}},
        "source_links": {},
    }
    if evidence is not None:
        brief["evidence"] = evidence
    return brief


def _plan(tasks):
    return {
        "id": "plan-evidence",
        "implementation_brief_id": "brief-evidence",
        "milestones": [{"name": "Build"}],
        "tasks": tasks,
    }


def _task(task_id, *, metadata=None):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "acceptance_criteria": [f"{task_id} works"],
        "metadata": metadata or {},
    }
