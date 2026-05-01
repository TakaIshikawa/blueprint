import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_decision_register import (
    PlanDecisionRecord,
    PlanDecisionRegister,
    build_plan_decision_register,
    plan_decision_register_to_dict,
    plan_decision_register_to_markdown,
    summarize_plan_decision_register,
)


def test_explicit_metadata_decisions_are_serialized_without_mutation():
    plan = _plan(
        [
            _task(
                "task-search",
                title="Implement search adapter",
                description="Build the adapter selected in the decision register.",
            )
        ],
        metadata={
            "decisions": [
                {
                    "title": "Use the hosted search adapter",
                    "status": "accepted",
                    "rationale": "Keeps ranking behavior outside the core API.",
                    "alternatives": ["Build an in-process index", "Use database full text search"],
                    "impacted_task_ids": ["task-search"],
                    "evidence": ["brief.architecture_notes: choose hosted search"],
                    "follow_up_actions": ["Create ADR export for search adapter choice"],
                }
            ]
        },
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_plan_decision_register(model)
    payload = plan_decision_register_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanDecisionRegister)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "summary"]
    assert list(payload["records"][0]) == [
        "decision_id",
        "title",
        "status",
        "rationale",
        "alternatives",
        "impacted_task_ids",
        "evidence",
        "follow_up_actions",
    ]

    record = result.records[0]
    assert isinstance(record, PlanDecisionRecord)
    assert record.decision_id == "decision-use-the-hosted-search-adapter"
    assert record.title == "Use the hosted search adapter"
    assert record.status == "decided"
    assert record.rationale == "Keeps ranking behavior outside the core API."
    assert record.alternatives == ("Build an in-process index", "Use database full text search")
    assert record.impacted_task_ids == ("task-search",)
    assert record.evidence == (
        "brief.architecture_notes: choose hosted search",
        "plan.metadata.decisions[0]: Use the hosted search adapter",
    )
    assert record.follow_up_actions == ("Create ADR export for search adapter choice",)
    assert result.summary == {
        "decision_count": 1,
        "decided_count": 1,
        "proposed_count": 0,
        "inferred_count": 0,
        "status_counts": {"decided": 1, "proposed": 0, "inferred": 0},
    }


def test_inferred_decisions_from_task_text_and_brief_metadata():
    result = build_plan_decision_register(
        _plan(
            [
                _task(
                    "task-cache",
                    title="Cache profile summaries",
                    description=(
                        "Choose Redis for profile summary caching because API latency must "
                        "stay below 100ms. Alternative local LRU cache."
                    ),
                    acceptance_criteria=[
                        "Rationale: cache invalidation remains event driven.",
                        "Follow-up: document cache TTL ownership.",
                    ],
                )
            ]
        ),
        implementation_brief={
            "id": "brief-decision",
            "source_brief_id": "source-decision",
            "title": "Decision brief",
            "problem_statement": "Need low-latency profile reads.",
            "mvp_goal": "Fast reads",
            "scope": [],
            "non_goals": [],
            "assumptions": ["Assume event invalidation is available because write volume is low."],
            "risks": ["Tradeoff: Redis adds operational ownership."],
            "validation_plan": "Measure read latency.",
            "definition_of_done": [],
        },
    )

    by_title = {record.title: record for record in result.records}

    assert by_title["Redis for profile summary caching"].status == "decided"
    assert by_title["Redis for profile summary caching"].rationale == (
        "API latency must stay below 100ms"
    )
    assert by_title["Redis for profile summary caching"].impacted_task_ids == ("task-cache",)
    assert by_title["Alternative local LRU cache"].alternatives == ("local LRU cache",)
    assert by_title["Document cache TTL ownership"].follow_up_actions == (
        "document cache TTL ownership",
    )
    assert by_title["Assume event invalidation is available"].rationale == (
        "Assumption: Assume event invalidation is available because write volume is low."
    )
    assert by_title["Tradeoff: Redis adds operational ownership"].rationale == (
        "Risk context: Tradeoff: Redis adds operational ownership."
    )


def test_adr_file_path_signals_create_inferred_decision_records():
    result = build_plan_decision_register(
        _plan(
            [
                _task(
                    "task-adr",
                    title="Wire payment boundary",
                    description="Implement the payment integration boundary.",
                    files_or_modules=["docs/adr/0007-payment-boundary.md"],
                )
            ]
        ),
        source_brief={
            "id": "source-decision",
            "title": "ADR source",
            "summary": "ADR-0007 decided to isolate payment writes.",
            "source_project": "manual",
            "source_entity_type": "adr",
            "source_id": "0007",
            "source_payload": {"path": "docs/adr/0007-payment-boundary.md"},
            "source_links": {"adr": "docs/adr/0007-payment-boundary.md"},
        },
    )

    path_records = [
        record for record in result.records if record.title.startswith("Track ADR reference")
    ]

    assert len(path_records) == 1
    assert path_records[0].status == "inferred"
    assert path_records[0].impacted_task_ids == ("task-adr",)
    assert path_records[0].evidence == (
        "task[task-adr].files_or_modules: docs/adr/0007-payment-boundary.md",
    )
    assert any(
        record.title == "ADR-0007 decided to isolate payment writes" for record in result.records
    )


def test_duplicate_decision_evidence_is_consolidated_deterministically():
    result = build_plan_decision_register(
        _plan(
            [
                _task(
                    "task-api",
                    description="Decided to use Postgres because relational reports need joins.",
                    metadata={
                        "decisions": [
                            {
                                "title": "Use Postgres",
                                "status": "decided",
                                "rationale": "Relational reports need joins.",
                                "evidence": [
                                    "task[task-api].description: Decided to use Postgres because relational reports need joins.",
                                    "task[task-api].description: Decided to use Postgres because relational reports need joins.",
                                ],
                            }
                        ]
                    },
                )
            ]
        )
    )

    postgres = next(record for record in result.records if record.title == "Use Postgres")

    assert postgres.impacted_task_ids == ("task-api",)
    assert postgres.evidence == (
        "task[task-api].description: Decided to use Postgres because relational reports need joins.",
        "task[task-api].metadata.decisions[0]: Use Postgres",
    )


def test_markdown_renderer_escapes_pipes_stably():
    result = build_plan_decision_register(
        _plan(
            [
                _task(
                    "task-pipe",
                    metadata={
                        "decisions": [
                            {
                                "title": "Use queue | worker split",
                                "rationale": "Avoid API | worker coupling.",
                                "alternatives": ["Single process | cron"],
                                "tasks": ["task-pipe"],
                                "followups": ["Write ADR | runbook"],
                            }
                        ]
                    },
                )
            ]
        )
    )

    markdown = plan_decision_register_to_markdown(result)

    assert markdown == result.to_markdown()
    assert "Use queue \\| worker split" in markdown
    assert "Avoid API \\| worker coupling." in markdown
    assert "Single process \\| cron" in markdown
    assert "Write ADR \\| runbook" in markdown


def test_no_decision_plan_returns_empty_register():
    result = build_plan_decision_register(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["docs/settings-copy.md"],
                )
            ]
        )
    )

    assert result.plan_id == "plan-decision"
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "decision_count": 0,
        "decided_count": 0,
        "proposed_count": 0,
        "inferred_count": 0,
        "status_counts": {"decided": 0, "proposed": 0, "inferred": 0},
    }
    assert result.to_markdown() == (
        "# Plan Decision Register: plan-decision\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Decision count: 0\n"
        "- Decided count: 0\n"
        "- Proposed count: 0\n"
        "- Inferred count: 0\n"
        "\n"
        "No implementation decisions were detected."
    )


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-decision",
        "implementation_brief_id": "brief-decision",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
