from blueprint.domain.models import ExecutionPlan
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.generators.plan_generator_staged import StagedPlanGenerator
from blueprint.generators.plan_reviser import PlanReviser


def test_plan_generator_repairs_fenced_json_and_validates_payload():
    provider = FakeLLMProvider(
        [
            {
                "content": """Here is the plan:\n```json\n{
  "target_engine": "codex",
  "target_repo": "example/repo",
  "project_type": "cli_tool",
  "milestones": [
    {
      "name": "Milestone 1: Foundation",
      "description": "Set up the project",
      "tasks": [
        {
          "title": "Create package layout",
          "description": "Set up modules and entry points",
          "owner_type": "agent",
          "suggested_engine": "codex",
          "depends_on": [],
          "files_or_modules": ["src/blueprint/__init__.py"],
          "acceptance_criteria": ["Package imports cleanly"],
          "estimated_complexity": "low"
        },
        {
          "title": "Add configuration defaults",
          "description": "Wire up basic config handling",
          "owner_type": "agent",
          "suggested_engine": "codex",
          "depends_on": ["Milestone 1:0"],
          "files_or_modules": ["src/blueprint/config.py"],
          "acceptance_criteria": ["Defaults load without error"],
          "estimated_complexity": "medium"
        }
      ]
    }
  ],
  "test_strategy": "Run pytest",
  "handoff_prompt": "Implement the plan"
}\n```""",
                "model": "test-model",
                "total_tokens": 111,
            }
        ]
    )
    brief = _implementation_brief()

    plan, tasks = PlanGenerator(provider).generate(brief, model="test-model")

    assert plan["target_engine"] == "codex"
    assert len(plan["milestones"]) == 1
    assert len(tasks) == 2
    assert tasks[1]["depends_on"] == [tasks[0]["id"]]
    assert ExecutionPlan.model_validate({**plan, "tasks": tasks}).id == plan["id"]


def test_staged_plan_generator_repairs_wrapped_json_and_validates_payload():
    provider = FakeLLMProvider(
        [
            {
                "content": """Sure, here are the milestones:\n{\n  "milestones": [\n    {\n      "name": "Milestone 1: Foundation",\n      "description": "Set up the project"\n    },\n    {\n      "name": "Milestone 2: Core Features",\n      "description": "Build the core flow"\n    }\n  ]\n}""",
                "model": "test-model",
                "total_tokens": 10,
            },
            {
                "content": """```json\n{\n  "tasks": [\n    {\n      "title": "Create package layout",\n      "description": "Set up modules and entry points",\n      "files_or_modules": ["src/blueprint/__init__.py"],\n      "acceptance_criteria": ["Package imports cleanly"],\n      "estimated_complexity": "low",\n      "suggested_engine": "codex"\n    }\n  ]\n}\n```""",
                "model": "test-model",
                "total_tokens": 11,
            },
            {
                "content": """{\n  "tasks": [\n    {\n      "title": "Add config defaults",\n      "description": "Wire up basic config handling",\n      "files_or_modules": ["src/blueprint/config.py"],\n      "acceptance_criteria": ["Defaults load without error"],\n      "estimated_complexity": "medium",\n      "suggested_engine": "codex"\n    }\n  ]\n}""",
                "model": "test-model",
                "total_tokens": 12,
            },
            {
                "content": """{\n  "target_engine": "codex",\n  "target_repo": "example/repo",\n  "project_type": "cli_tool",\n  "test_strategy": "Run pytest",\n  "handoff_prompt": "Implement the plan",\n}""",
                "model": "test-model",
                "total_tokens": 13,
            },
        ]
    )
    brief = _implementation_brief()

    plan, tasks = StagedPlanGenerator(provider).generate(brief, model="test-model")

    assert plan["target_engine"] == "codex"
    assert len(plan["milestones"]) == 2
    assert len(tasks) == 2
    assert ExecutionPlan.model_validate({**plan, "tasks": tasks}).id == plan["id"]


def test_plan_reviser_repairs_json_and_preserves_lineage():
    provider = FakeLLMProvider(
        [
            {
                "content": """The revised plan is below:\n```json\n{\n  "target_engine": "codex",\n  "target_repo": "example/repo",\n  "project_type": "cli_tool",\n  "milestones": [\n    {\n      "name": "Milestone 1: Foundation",\n      "description": "Set up the project",\n      "tasks": [\n        {\n          "title": "Create package layout",\n          "description": "Set up modules and entry points",\n          "owner_type": "agent",\n          "suggested_engine": "codex",\n          "depends_on": [],\n          "files_or_modules": ["src/blueprint/__init__.py"],\n          "acceptance_criteria": ["Package imports cleanly"],\n          "estimated_complexity": "low"\n        }\n      ]\n    }\n  ],\n  "test_strategy": "Run pytest",\n  "handoff_prompt": "Implement the plan"\n}\n```""",
                "model": "test-model",
                "total_tokens": 13,
            }
        ]
    )
    brief = _implementation_brief()

    plan, tasks = PlanReviser(provider).generate(
        implementation_brief=brief,
        existing_plan=_existing_plan(),
        feedback="Tighten scope",
        model="test-model",
        feedback_source="inline",
    )

    assert plan["metadata"]["lineage"]["revised_from_plan_id"] == "plan-original"
    assert len(tasks) == 1
    assert ExecutionPlan.model_validate({**plan, "tasks": tasks}).id == plan["id"]


def test_plan_generator_reports_actionable_error_for_unrecoverable_json():
    provider = FakeLLMProvider(
        [
            {
                "content": "This is not JSON and cannot be repaired.",
                "model": "test-model",
                "total_tokens": 1,
            }
        ]
    )

    try:
        PlanGenerator(provider).generate(_implementation_brief(), model="test-model")
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected ValueError")

    assert "execution plan generation" in message
    assert "Last stage:" in message
    assert "Snippet:" in message
    assert "Response saved to:" in message


class FakeLLMProvider:
    def __init__(self, responses: list[dict[str, object]]):
        self.responses = responses
        self.default_model = "test-default"

    def generate(
        self,
        prompt,
        model=None,
        temperature=1.0,
        max_tokens=4096,
        system=None,
    ):
        response = self.responses.pop(0)
        return {
            "content": response["content"],
            "model": response.get("model", model or self.default_model),
            "usage": {
                "input_tokens": 0,
                "output_tokens": response.get("total_tokens", 0),
                "total_tokens": response.get("total_tokens", 0),
            },
        }

    @classmethod
    def resolve_model(cls, model_alias):
        return model_alias


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need plan generation",
        "mvp_goal": "Create an execution plan",
        "product_surface": "CLI",
        "scope": ["Plan generation"],
        "non_goals": ["Plan execution"],
        "assumptions": ["A brief exists"],
        "architecture_notes": "Use generator helpers",
        "data_requirements": "Implementation brief and plan output",
        "integration_points": [],
        "risks": ["Malformed JSON"],
        "validation_plan": "Run plan generator tests",
        "definition_of_done": ["Plans can be generated"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _existing_plan():
    return {
        "id": "plan-original",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Milestone 1: Foundation", "description": "Set up the project"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
