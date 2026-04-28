from click.testing import CliRunner

from blueprint import cli as cli_module
from blueprint import config as blueprint_config
from blueprint.domain.models import ExecutionPlan
from blueprint.generators.heuristic_plan_generator import HeuristicPlanGenerator
from blueprint.store import Store, init_db


def test_heuristic_plan_generator_is_deterministic_for_sample_brief():
    brief = _implementation_brief()
    first_plan, first_tasks = HeuristicPlanGenerator().generate(brief)
    second_plan, second_tasks = HeuristicPlanGenerator().generate(brief)

    assert first_plan == second_plan
    assert first_tasks == second_tasks
    assert first_plan["generation_model"] == "heuristic"
    assert first_plan["generation_tokens"] == 0
    assert [milestone["name"] for milestone in first_plan["milestones"]] == [
        "Milestone 1: Foundation",
        "Milestone 2: Implementation",
        "Milestone 3: Validation",
    ]
    assert ExecutionPlan.model_validate({**first_plan, "tasks": first_tasks})


def test_heuristic_plan_tasks_include_required_execution_fields():
    plan, tasks = HeuristicPlanGenerator().generate(_implementation_brief())

    assert len(plan["milestones"]) == 3
    assert len(tasks) >= 3
    for task in tasks:
        assert task["files_or_modules"]
        assert task["acceptance_criteria"]
        assert task["estimated_complexity"] in {"low", "medium", "high"}
        assert task["suggested_engine"] in {"codex", "smoothie", "manual"}


def test_plan_create_no_llm_persists_plan_without_api_credentials(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(cli_module, "_create_llm_provider", _fail_if_llm_provider_is_created)

    result = CliRunner().invoke(
        cli_module.cli,
        ["plan", "create", "ib-heuristic", "--no-llm"],
    )

    assert result.exit_code == 0, result.output
    assert "Generated execution plan plan-heur-" in result.output
    assert "Generation model: heuristic" in result.output

    plans = Store(str(tmp_path / "blueprint.db")).list_execution_plans(
        brief_id="ib-heuristic",
        limit=10,
    )
    assert len(plans) == 1
    persisted = plans[0]
    assert persisted["generation_model"] == "heuristic"
    assert persisted["generation_tokens"] == 0
    assert [milestone["name"] for milestone in persisted["milestones"]] == [
        "Milestone 1: Foundation",
        "Milestone 2: Implementation",
        "Milestone 3: Validation",
    ]
    assert persisted["tasks"]
    assert all(task["files_or_modules"] for task in persisted["tasks"])


def _fail_if_llm_provider_is_created(config):
    raise AssertionError("--no-llm should not create an LLM provider")


def _setup_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()
    return init_db(str(tmp_path / "blueprint.db"))


def _implementation_brief():
    return {
        "id": "ib-heuristic",
        "source_brief_id": "src-heuristic",
        "title": "Offline Review Queue Planner",
        "domain": "developer_tools",
        "target_user": "Repository maintainers",
        "buyer": "Engineering managers",
        "workflow_context": "Daily pull request triage",
        "problem_statement": "Maintainers need a credential-free way to plan review queue work.",
        "mvp_goal": "Build a CLI planner that summarizes review queue implementation work.",
        "product_surface": "CLI",
        "scope": [
            "Load review queue data from local storage",
            "Render pending review summary in the CLI",
        ],
        "non_goals": ["Hosted dashboard", "Automated reviewer assignment"],
        "assumptions": ["Review data is available locally"],
        "architecture_notes": "Use generator helpers and existing store access patterns.",
        "data_requirements": "Pull request IDs, statuses, reviewers, and blocker notes.",
        "integration_points": ["Local repository metadata"],
        "risks": ["Stale data may mislead users; show timestamps in output"],
        "validation_plan": "Run focused pytest coverage for the CLI planner.",
        "definition_of_done": [
            "CLI creates a persisted plan without API credentials",
            "Generated tasks expose files, criteria, complexity, and engine",
        ],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
