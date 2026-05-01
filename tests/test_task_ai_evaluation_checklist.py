import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_ai_evaluation_checklist import (
    TaskAIEvaluationChecklistPlan,
    TaskAIEvaluationChecklistRecord,
    analyze_task_ai_evaluation_checklist,
    build_task_ai_evaluation_checklist_plan,
    summarize_task_ai_evaluation_checklist,
    task_ai_evaluation_checklist_plan_to_dict,
    task_ai_evaluation_checklist_plan_to_markdown,
)


def test_llm_prompt_task_receives_core_evaluation_artifacts():
    result = build_task_ai_evaluation_checklist_plan(
        _plan(
            [
                _task(
                    "task-prompt",
                    title="Tune LLM prompt for support replies",
                    description="Update the prompt template for AI-generated customer support output.",
                    files_or_modules=["src/blueprint/prompts/support_reply.md"],
                )
            ]
        )
    )

    assert isinstance(result, TaskAIEvaluationChecklistPlan)
    assert result.plan_id == "plan-ai-eval"
    assert result.ai_task_ids == ("task-prompt",)
    assert result.non_ai_task_ids == ()
    assert result.summary["risk_counts"]["medium"] == 1
    record = result.records[0]
    assert isinstance(record, TaskAIEvaluationChecklistRecord)
    assert record.evaluation_risk == "medium"
    assert record.ai_signals == ("llm", "prompt", "generated_output")
    assert (
        "Golden evaluation dataset with representative inputs and expected outputs."
        in record.required_artifacts
    )
    assert (
        "Regression prompt suite covering current and previously fixed behaviors."
        in record.required_artifacts
    )
    assert any("Hallucination checks" in value for value in record.recommended_test_cases)
    assert record.evidence[:2] == (
        "files_or_modules: src/blueprint/prompts/support_reply.md",
        "title: Tune LLM prompt for support replies",
    )


def test_retrieval_summarization_and_embedding_tasks_get_specific_cases():
    result = analyze_task_ai_evaluation_checklist(
        _plan(
            [
                _task(
                    "task-rag",
                    title="Add RAG summary generation",
                    description="Use retrieved context and citations to summarize account history.",
                    files_or_modules=["src/blueprint/retrieval/account_summarizer.py"],
                    metadata={"evaluation": "Track hallucination rate against golden docs."},
                ),
                _task(
                    "task-embeddings",
                    title="Refresh embeddings index",
                    description="Update vector store fixtures for semantic search.",
                    files_or_modules=["src/blueprint/embeddings/index.py"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-rag"].ai_signals == ("retrieval", "summarization", "safety")
    assert any("Retrieval grounding dataset" in value for value in by_id["task-rag"].required_artifacts)
    assert any("Summarization tests" in value for value in by_id["task-rag"].recommended_test_cases)
    assert any("Safety and refusal case set" in value for value in by_id["task-rag"].required_artifacts)
    assert by_id["task-embeddings"].evaluation_risk == "low"
    assert by_id["task-embeddings"].ai_signals == ("embedding",)
    assert any("Embedding similarity fixtures" in value for value in by_id["task-embeddings"].required_artifacts)
    assert result.summary["signal_counts"]["retrieval"] == 1
    assert result.summary["signal_counts"]["embedding"] == 1


def test_model_routing_classifier_and_validation_commands_raise_evidence():
    result = build_task_ai_evaluation_checklist_plan(
        _plan(
            [
                _task(
                    "task-router",
                    title="Route risky messages to fallback model",
                    description="Add model routing for classifier confidence and provider fallback.",
                    files_or_modules=["src/blueprint/model_router.py"],
                    test_command="poetry run pytest tests/evals/test_model_fallback.py",
                    metadata={
                        "validation_commands": {
                            "test": ["poetry run pytest tests/evals/test_classifier_prompts.py"]
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.evaluation_risk == "high"
    assert record.ai_signals == ("model_routing", "classifier")
    assert any("Model fallback matrix" in value for value in record.required_artifacts)
    assert any("Classifier confusion matrix" in value for value in record.required_artifacts)
    assert any("Fallback tests cover model timeout" in value for value in record.recommended_test_cases)
    assert (
        "validation_commands: poetry run pytest tests/evals/test_model_fallback.py"
        in record.evidence
    )
    assert (
        "validation_commands: poetry run pytest tests/evals/test_classifier_prompts.py"
        in record.evidence
    )


def test_non_ai_tasks_are_tracked_without_false_positive_checklists():
    result = build_task_ai_evaluation_checklist_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings page copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/settings_copy.py"],
                ),
                _task(
                    "task-api",
                    title="Add billing API endpoint",
                    description="Expose invoice status for the dashboard.",
                    files_or_modules=["src/blueprint/api/billing.py"],
                ),
            ]
        )
    )

    assert result.ai_task_ids == ()
    assert result.non_ai_task_ids == ("task-api", "task-copy")
    assert all(record.evaluation_risk == "not_ai_related" for record in result.records)
    assert all(record.required_artifacts == () for record in result.records)
    assert all(record.recommended_test_cases == () for record in result.records)
    assert all(record.evidence == () for record in result.records)


def test_model_inputs_malformed_fields_and_serialization_are_stable():
    task_dict = _task(
        "task-malformed",
        title="Add prompt evaluation | support",
        description="Add refusal and hallucination eval cases.",
        files_or_modules={"main": "evals/prompts/support.yaml", "none": None},
        acceptance_criteria={"eval": "Regression prompts pass."},
        metadata={"cases": [{"kind": "safety refusal"}, None, 7]},
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Classify account intent",
            description="Add intent classifier thresholds.",
            files_or_modules=["src/blueprint/classifiers/account_intent.py"],
        )
    )
    raw_result = build_task_ai_evaluation_checklist_plan(_plan([task_dict]))
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")]))

    first = summarize_task_ai_evaluation_checklist(
        _plan([task_dict, task_model.model_dump(mode="python")])
    )
    second = build_task_ai_evaluation_checklist_plan(plan_model)
    payload = task_ai_evaluation_checklist_plan_to_dict(first)
    markdown = task_ai_evaluation_checklist_plan_to_markdown(first)

    assert task_dict == original
    assert raw_result.records[0].task_id == "task-malformed"
    assert task_ai_evaluation_checklist_plan_to_dict(second)["records"][0]["task_id"] == "task-model"
    assert first.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "ai_task_ids", "non_ai_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "evaluation_risk",
        "ai_signals",
        "required_artifacts",
        "recommended_test_cases",
        "evidence",
    ]
    assert markdown.startswith("# Task AI Evaluation Checklist: plan-ai-eval")
    assert "Add prompt evaluation \\| support" not in markdown
    assert "Regression prompt suite" in markdown


def _plan(tasks):
    return {
        "id": "plan-ai-eval",
        "implementation_brief_id": "brief-ai-eval",
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
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
