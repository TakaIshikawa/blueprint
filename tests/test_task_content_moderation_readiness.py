import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.task_content_moderation_readiness import (
    TaskContentModerationReadinessPlan,
    TaskContentModerationReadinessRecord,
    analyze_task_content_moderation_readiness,
    build_task_content_moderation_readiness_plan,
    extract_task_content_moderation_readiness,
    generate_task_content_moderation_readiness,
    recommend_task_content_moderation_readiness,
    summarize_task_content_moderation_readiness,
    task_content_moderation_readiness_plan_to_dict,
    task_content_moderation_readiness_plan_to_dicts,
    task_content_moderation_readiness_plan_to_markdown,
)


def test_chat_and_community_posts_without_safeguards_are_high_risk():
    result = build_task_content_moderation_readiness_plan(
        _plan(
            [
                _task(
                    "task-chat",
                    title="Add chat messages for community posts",
                    description="Users can send chat messages and create public community posts.",
                    files_or_modules=["src/community/posts/chat_messages.py"],
                    acceptance_criteria=["Messages render in the community timeline."],
                )
            ]
        )
    )

    assert isinstance(result, TaskContentModerationReadinessPlan)
    assert result.moderation_task_ids == ("task-chat",)
    record = result.records[0]
    assert isinstance(record, TaskContentModerationReadinessRecord)
    assert record.content_surfaces == ("chat_messages", "community_posts")
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "policy_definition",
        "reporting_flow",
        "reviewer_queue",
        "automated_detection",
        "appeal_process",
        "audit_log",
        "abuse_metrics",
        "test_coverage",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: src/community/posts/chat_messages.py" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["moderation_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["policy_definition"] == 1
    assert result.summary["surface_counts"]["chat_messages"] == 1


def test_reviews_comments_profiles_uploads_and_metadata_surfaces_are_detected():
    result = analyze_task_content_moderation_readiness(
        _plan(
            [
                _task(
                    "task-ugc",
                    title="Launch reviews and comments on public profile bios",
                    description="Support reviews, threaded comments, profile bios, and image uploads.",
                    files_or_modules=[
                        "src/reviews/review_form.tsx",
                        "src/profiles/profile_bio.py",
                        "src/uploads/image_attachments.py",
                    ],
                    tags=["ugc", "community_posts"],
                    metadata={
                        "content_surfaces": {
                            "attachments": True,
                            "chat_messages": "future direct messages need moderation readiness",
                        },
                        "moderation": {
                            "policy_definition": "Community guidelines define prohibited content.",
                            "reporting_flow": "Users can report abuse from each comment.",
                            "reviewer_queue": "Moderator queue triages reports.",
                        },
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.content_surfaces == (
        "comments",
        "reviews",
        "profile_content",
        "images",
        "attachments",
        "chat_messages",
        "community_posts",
    )
    assert record.present_safeguards == (
        "policy_definition",
        "reporting_flow",
        "reviewer_queue",
    )
    assert record.missing_safeguards == (
        "automated_detection",
        "appeal_process",
        "audit_log",
        "abuse_metrics",
        "test_coverage",
    )
    assert record.risk_level == "medium"
    assert any("metadata.moderation.policy_definition" in item for item in record.evidence)
    assert any("tags[0]" in item for item in record.evidence)


def test_fully_covered_moderation_workflow_is_low_risk():
    result = build_task_content_moderation_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Ship moderated comments and reviews",
                    description="Expose user comments and reviews with a complete moderation workflow.",
                    files_or_modules=["src/comments/moderation_queue.py"],
                    acceptance_criteria=[
                        "Content policy and community guidelines define prohibited content.",
                        "Reporting flow lets users flag content and report abuse.",
                        "Reviewer queue supports human review and moderator triage.",
                        "Automated detection covers spam detection and toxicity detection.",
                        "Appeal process lets users appeal decisions.",
                        "Audit log records moderation decisions and enforcement logs.",
                        "Abuse metrics track report rate, queue backlog, and false positives.",
                        "Moderation tests cover reporting, review, appeal, audit, and metrics.",
                    ],
                    validation_commands={
                        "test": ["poetry run pytest tests/moderation/test_comment_moderation.py"]
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "policy_definition",
        "reporting_flow",
        "reviewer_queue",
        "automated_detection",
        "appeal_process",
        "audit_log",
        "abuse_metrics",
        "test_coverage",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0
    assert any("validation_commands:" in item for item in record.evidence)


def test_unrelated_tasks_are_not_applicable_with_stable_empty_summary():
    result = build_task_content_moderation_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust settings labels and loading states.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.recommendations == ()
    assert result.moderation_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "moderation_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "policy_definition": 0,
            "reporting_flow": 0,
            "reviewer_queue": 0,
            "automated_detection": 0,
            "appeal_process": 0,
            "audit_log": 0,
            "abuse_metrics": 0,
            "test_coverage": 0,
        },
        "surface_counts": {},
    }
    assert "No content moderation readiness records were inferred." in result.to_markdown()
    assert "Not-applicable tasks: task-copy" in result.to_markdown()


def test_serialization_markdown_aliases_sorting_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Reviews | ready",
                description=(
                    "Reviews include content policy, reporting flow, reviewer queue, automated detection, "
                    "appeal process, audit log, abuse metrics, and test coverage."
                ),
            ),
            _task(
                "task-a",
                title="Add image attachments",
                description="Let users add image attachments to community posts.",
            ),
            _task("task-copy", title="Copy update", description="Change helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_content_moderation_readiness(plan)
    payload = task_content_moderation_readiness_plan_to_dict(result)
    markdown = task_content_moderation_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_content_moderation_readiness_plan_to_dicts(result) == payload["records"]
    assert task_content_moderation_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_content_moderation_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_content_moderation_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_content_moderation_readiness(plan).to_dict() == result.to_dict()
    assert result.moderation_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "moderation_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "content_surfaces",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_checks",
    ]
    assert markdown.startswith("# Task Content Moderation Readiness: plan-moderation")
    assert "Reviews \\| ready" in markdown
    assert "| Task | Title | Risk | Surfaces | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


def test_execution_plan_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Moderate profile content",
        description="Public profile bios need a report abuse flow.",
        acceptance_criteria=["Content policy is documented."],
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add chat message moderation",
                    description="Chat messages include report button, reviewer queue, audit logs, and moderation tests.",
                    metadata={"abuse_metrics": "Track report rate and queue backlog."},
                )
            ],
            plan_id="plan-model",
        )
    )

    object_result = build_task_content_moderation_readiness_plan([object_task])
    model_result = build_task_content_moderation_readiness_plan(plan_model)
    invalid = build_task_content_moderation_readiness_plan(17)

    assert object_result.records[0].task_id == "task-object"
    assert object_result.records[0].content_surfaces == ("profile_content",)
    assert "policy_definition" in object_result.records[0].present_safeguards
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"
    assert model_result.records[0].content_surfaces == ("chat_messages",)
    assert invalid.records == ()


def _plan(tasks, *, plan_id="plan-moderation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-moderation",
        "target_engine": "codex",
        "target_repo": "blueprint",
        "project_type": "python",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": files_or_modules,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "metadata": {} if metadata is None else metadata,
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
