import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_prompt_injection_readiness import (
    TaskPromptInjectionReadinessPlan,
    TaskPromptInjectionReadinessRecommendation,
    build_task_prompt_injection_readiness_plan,
    summarize_task_prompt_injection_readiness,
    task_prompt_injection_readiness_plan_to_dict,
    task_prompt_injection_readiness_plan_to_markdown,
)


def test_direct_llm_task_detection_returns_recommendation_with_missing_controls():
    plan = build_task_prompt_injection_readiness_plan(
        {
            "id": "plan-ai",
            "tasks": [
                {
                    "id": "task-llm",
                    "title": "Build LLM summary prompt",
                    "description": "Create an OpenAI chat completion that summarizes support tickets.",
                    "files_or_modules": ["src/ai/prompts/support_summary.txt"],
                    "acceptance_criteria": [
                        "Prompt template renders deterministic expected output."
                    ],
                }
            ],
        }
    )

    assert isinstance(plan, TaskPromptInjectionReadinessPlan)
    assert len(plan.recommendations) == 1
    record = plan.recommendations[0]
    assert isinstance(record, TaskPromptInjectionReadinessRecommendation)
    assert record.task_id == "task-llm"
    assert {"llm", "prompt_template"} <= set(record.ai_surfaces)
    assert "prompt_boundary_tests" in record.missing_controls
    assert "output_validation" in record.missing_controls
    assert any("OpenAI chat completion" in item for item in record.evidence)


def test_retrieval_and_tool_use_with_untrusted_content_escalates_to_high_risk():
    plan = build_task_prompt_injection_readiness_plan(
        {
            "id": "plan-risk",
            "tasks": [
                {
                    "id": "task-rag-tool",
                    "title": "Agent answers uploaded PDFs",
                    "description": (
                        "Build an agent that uses RAG over user-uploaded PDFs and executes tool calls "
                        "against account APIs."
                    ),
                    "acceptance_criteria": [
                        "The answer includes retrieved context for each customer document."
                    ],
                    "metadata": {"inputs": "Untrusted external documents and uploaded files."},
                }
            ],
        }
    )

    record = plan.recommendations[0]

    assert record.risk_level == "high"
    assert {"agent", "retrieval", "tool_use", "user_uploaded_content"} <= set(record.ai_surfaces)
    assert record.untrusted_input_signals
    assert "tool_allowlist" in record.missing_controls
    assert "retrieval_citation_checks" in record.missing_controls
    assert plan.summary["risk_counts"]["high"] == 1


def test_complete_safeguards_reduce_missing_control_counts_and_risk():
    controls = [
        "Input source isolation separates trusted instructions from untrusted user uploads.",
        "Prompt boundary tests cover prompt injection and jailbreak attempts.",
        "Tool allowlist limits function calling to read-only account tools.",
        "Retrieval citation checks verify cited sources before answers are returned.",
        "Output validation uses schema validation before sending responses.",
        "Secret exfiltration tests prevent credential leakage and prompt leaks.",
        "Human review path sends uncertain or high-impact answers to a review queue.",
    ]

    plan = build_task_prompt_injection_readiness_plan(
        {
            "tasks": [
                {
                    "id": "task-safe",
                    "title": "Guarded RAG assistant",
                    "description": "LLM agent with retrieval over external documents and tool calls.",
                    "acceptance_criteria": controls,
                }
            ]
        }
    )

    record = plan.recommendations[0]

    assert record.missing_controls == ()
    assert record.risk_level == "low"
    assert all(count == 0 for count in plan.summary["missing_control_counts"].values())


def test_unrelated_tasks_are_suppressed():
    plan = build_task_prompt_injection_readiness_plan(
        {
            "id": "plan-suppressed",
            "tasks": [
                {
                    "id": "task-css",
                    "title": "Adjust billing page styles",
                    "description": "Update responsive spacing on the billing dashboard.",
                    "acceptance_criteria": ["Spacing matches the design spec."],
                },
                {
                    "id": "task-agent",
                    "title": "Add agent prompt",
                    "description": "Create a prompt template for an agent.",
                    "acceptance_criteria": [],
                },
            ],
        }
    )

    assert [record.task_id for record in plan.recommendations] == ["task-agent"]
    assert plan.suppressed_task_ids == ("task-css",)
    assert plan.summary["suppressed_task_count"] == 1


def test_execution_plan_input_and_alias_are_supported():
    execution_plan = ExecutionPlan(
        id="ep-model",
        implementation_brief_id="brief-1",
        milestones=[],
        tasks=[
            {
                "id": "task-model",
                "title": "Classify uploaded documents",
                "description": "Use an LLM classifier on user-uploaded attachments.",
                "acceptance_criteria": ["Output validation rejects invalid labels."],
                "metadata": {"prompt_template": "classification system prompt"},
            }
        ],
    )

    plan = build_task_prompt_injection_readiness_plan(execution_plan)
    alias_plan = summarize_task_prompt_injection_readiness(execution_plan)

    assert alias_plan.to_dict() == plan.to_dict()
    assert plan.plan_id == "ep-model"
    assert plan.recommendations[0].task_id == "task-model"
    assert {"llm", "user_uploaded_content", "prompt_template"} <= set(
        plan.recommendations[0].ai_surfaces
    )


def test_json_and_markdown_serializers_are_stable():
    plan = build_task_prompt_injection_readiness_plan(
        {
            "id": "plan-json",
            "tasks": [
                {
                    "id": "task-json",
                    "title": "Prompt | renderer",
                    "description": "Render a prompt template for an LLM.",
                    "acceptance_criteria": ["Output validation is required."],
                }
            ],
        }
    )

    payload = task_prompt_injection_readiness_plan_to_dict(plan)
    markdown = task_prompt_injection_readiness_plan_to_markdown(plan)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "ai_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "ai_surfaces",
        "untrusted_input_signals",
        "missing_controls",
        "risk_level",
        "evidence",
    ]
    assert plan.to_dicts() == payload["recommendations"]
    assert markdown == plan.to_markdown()
    assert markdown.startswith("# Task Prompt Injection Readiness: plan-json")
    assert "Prompt \\| renderer" in markdown


def test_mapping_input_is_not_mutated_and_output_order_is_deterministic():
    source = {
        "tasks": [
            {
                "id": "task-medium",
                "title": "Build LLM prompt",
                "description": "LLM prompt template for internal summaries.",
                "acceptance_criteria": ["Output validation checks the response schema."],
            },
            {
                "id": "task-high",
                "title": "Tool agent over docs",
                "description": "Agent reads external documents and performs tool calls.",
                "acceptance_criteria": [],
            },
        ],
        "metadata": {"nested": ["unchanged"]},
    }
    original = copy.deepcopy(source)

    plan = build_task_prompt_injection_readiness_plan(source)
    repeat = build_task_prompt_injection_readiness_plan(copy.deepcopy(source))

    assert source == original
    assert plan.to_dict() == repeat.to_dict()
    assert [record.task_id for record in plan.recommendations] == ["task-high", "task-medium"]
