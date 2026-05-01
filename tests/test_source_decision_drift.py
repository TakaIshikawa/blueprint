import json

from blueprint.domain.models import ExecutionPlan, SourceBrief
from blueprint.source_decision_drift import (
    SourceDecisionDriftAnalysis,
    SourceDecisionDriftRecord,
    analyze_source_decision_drift,
    detect_source_decision_drift,
    source_decision_drift_analysis_to_dict,
    source_decision_drift_analysis_to_markdown,
    source_decision_drifts_to_dicts,
)


def test_classifies_missing_contradicted_and_weakly_represented_decisions_separately():
    analysis = analyze_source_decision_drift(
        [
            _source(
                source_id="MEET-1",
                payload={
                    "decisions": [
                        "Use Postgres as the source of truth for import state.",
                        "Keep admin retry controls in the dashboard.",
                    ],
                    "metadata": {
                        "decision": "Must preserve audit events for every retry.",
                    },
                },
            ),
            _source(
                source_id="ADR-7",
                summary="Decision: require SSO for export access.",
                payload={
                    "constraints": ["Only support CSV export in the MVP."],
                },
            ),
        ],
        _plan(
            [
                _task(
                    "task-import-storage",
                    description=(
                        "Use Redis for import state instead of Postgres and keep retry "
                        "controls available to admins."
                    ),
                ),
                _task(
                    "task-export",
                    description="Build export access checks.",
                    acceptance_criteria=["CSV export is available in the MVP."],
                ),
            ]
        ),
    )

    assert isinstance(analysis, SourceDecisionDriftAnalysis)
    assert analysis.plan_id == "plan-decision-drift"
    assert analysis.source_count == 2
    assert analysis.decision_count == 5
    assert [
        (drift.source_id, drift.drift_type, drift.decision_text) for drift in analysis.drifts
    ] == [
        (
            "MEET-1",
            "contradicted",
            "Use Postgres as the source of truth for import state.",
        ),
        (
            "MEET-1",
            "missing",
            "Must preserve audit events for every retry.",
        ),
        (
            "MEET-1",
            "weakly_represented",
            "Keep admin retry controls in the dashboard.",
        ),
        (
            "ADR-7",
            "weakly_represented",
            "Only support CSV export in the MVP.",
        ),
        (
            "ADR-7",
            "weakly_represented",
            "require SSO for export access.",
        ),
    ]
    assert analysis.drifts[0].confidence == "high"
    assert analysis.drifts[0].evidence == (
        "source_payload.decisions.001.line_001: Use Postgres as the source of truth for import state.",
        (
            "task.task-import-storage.description.001: Use Redis for import state "
            "instead of Postgres and keep retry controls available to admins."
        ),
    )
    assert analysis.drifts[2].confidence == "low"
    assert "task.task-import-storage.description.001" in analysis.drifts[2].evidence[1]


def test_extracts_decisions_from_headings_bullet_labels_and_metadata_fields():
    analysis = analyze_source_decision_drift(
        _source(
            source_id="NOTES-9",
            summary=(
                "## Decisions\n"
                "- Use signed download URLs for attachments.\n"
                "- Decision: keep exports synchronous for MVP.\n\n"
                "Notes\n"
                "- Decision: require manager approval for bulk delete."
            ),
            payload={
                "frontmatter": {
                    "approved_decisions": ["Use retention policy tags on generated files."],
                },
                "raw_text": "Decision - emit audit metadata on every export.",
            },
        ),
        _plan(
            [
                _task(
                    "task-downloads",
                    description="Build attachment download support.",
                    acceptance_criteria=["Signed URLs are generated for attachments."],
                ),
            ]
        ),
    )

    assert [drift.decision_text for drift in analysis.drifts] == [
        "emit audit metadata on every export.",
        "keep exports synchronous for MVP.",
        "require manager approval for bulk delete.",
        "Use retention policy tags on generated files.",
        "Use signed download URLs for attachments.",
    ]
    assert {drift.drift_type for drift in analysis.drifts} == {
        "missing",
        "weakly_represented",
    }
    assert analysis.decision_count == 5


def test_model_inputs_aliases_and_serialization_are_stable():
    source_model = SourceBrief.model_validate(
        _source(
            source_id="MODEL-1",
            payload={"decisions": ["Use repository adapter for persistence."]},
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-repository",
                    description="Implement repository adapter persistence.",
                )
            ]
        )
    )

    analysis = detect_source_decision_drift([source_model], plan_model)
    payload = source_decision_drift_analysis_to_dict(analysis)

    assert analysis.drifts == ()
    assert payload == {
        "plan_id": "plan-decision-drift",
        "source_count": 1,
        "decision_count": 1,
        "drift_count": 0,
        "drifts": [],
    }
    assert source_decision_drifts_to_dicts(analysis) == []
    assert source_decision_drift_analysis_to_markdown(analysis) == "\n".join(
        [
            "# Source Decision Drift: plan-decision-drift",
            "",
            "No source decision drift detected.",
        ]
    )
    assert json.loads(json.dumps(payload)) == payload


def test_plain_implementation_brief_input_is_supported():
    analysis = analyze_source_decision_drift(
        {"source_briefs": [_source(payload={"decisions": ["Use webhook ingestion."]})]},
        {
            "id": "brief-drift",
            "title": "Webhook Brief",
            "scope": ["Build webhook ingestion."],
            "problem_statement": "Manual imports are slow.",
            "mvp_goal": "Accept webhooks.",
            "validation_plan": "Run tests.",
            "definition_of_done": [],
            "non_goals": [],
            "assumptions": [],
            "risks": [],
        },
    )

    assert analysis.plan_id == "brief-drift"
    assert analysis.drifts == ()


def test_record_to_dict_key_order_is_deterministic():
    record = SourceDecisionDriftRecord(
        source_id="SRC",
        source_index=1,
        decision_text="Use cache warming.",
        drift_type="missing",
        confidence="medium",
        evidence=("source_payload.decisions.001: Use cache warming.",),
        suggested_follow_up="Add this source decision to the implementation brief or plan.",
    )

    assert list(record.to_dict()) == [
        "source_id",
        "source_index",
        "decision_text",
        "drift_type",
        "confidence",
        "evidence",
        "suggested_follow_up",
    ]


def _source(*, source_id="SRC-1", summary="Source summary.", payload=None):
    return {
        "id": f"source-{source_id.lower()}",
        "title": "Decision Source",
        "summary": summary,
        "source_project": "meeting_notes",
        "source_entity_type": "notes",
        "source_id": source_id,
        "source_payload": {} if payload is None else payload,
        "source_links": {},
    }


def _plan(tasks):
    return {
        "id": "plan-decision-drift",
        "implementation_brief_id": "brief-decision-drift",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run pytest",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _task(task_id, *, description, acceptance_criteria=None):
    return {
        "id": task_id,
        "execution_plan_id": "plan-decision-drift",
        "title": f"Task {task_id}",
        "description": description,
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": acceptance_criteria or ["Task is complete."],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": {},
    }
