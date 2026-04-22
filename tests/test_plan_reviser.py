from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_store_persists_execution_plan_lineage_metadata(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    plan = store.get_execution_plan("plan-revised")

    assert plan["metadata"]["lineage"]["revised_from_plan_id"] == "plan-original"
    assert plan["metadata"]["lineage"]["revision_feedback"] == "Tighten scope"


def test_plan_revise_cli_creates_separate_plan_with_lineage(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    feedback_path = tmp_path / "feedback.txt"
    feedback_path.write_text("Split migration work into its own milestone")

    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_original_plan(), [_original_task()])

    monkeypatch.setattr("blueprint.cli.LLMClient", FakeLLMClient)
    monkeypatch.setattr("blueprint.cli.PlanReviser", FakePlanReviser)

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "revise",
            "plan-original",
            "--feedback",
            str(feedback_path),
            "--model",
            "sonnet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Generated revised execution plan plan-revised" in result.output
    assert "Revised from: plan-original" in result.output

    persisted_original = Store(str(tmp_path / "blueprint.db")).get_execution_plan(
        "plan-original"
    )
    persisted_revised = Store(str(tmp_path / "blueprint.db")).get_execution_plan(
        "plan-revised"
    )

    assert persisted_original is not None
    assert persisted_revised["id"] == "plan-revised"
    assert persisted_revised["metadata"]["lineage"]["revised_from_plan_id"] == (
        "plan-original"
    )
    assert persisted_revised["metadata"]["lineage"]["revision_feedback"] == (
        "Split migration work into its own milestone"
    )
    assert persisted_revised["metadata"]["lineage"]["revision_feedback_source"] == str(
        feedback_path
    )


def test_plan_inspect_shows_revised_from_lineage(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["plan", "inspect", "plan-revised"])

    assert result.exit_code == 0, result.output
    assert "Revised From:         plan-original" in result.output


class FakeLLMClient:
    def __init__(self, api_key, default_model):
        self.api_key = api_key
        self.default_model = default_model

    @classmethod
    def resolve_model(cls, model_alias):
        return f"resolved-{model_alias}"


class FakePlanReviser:
    def __init__(self, llm_client):
        self.llm_client = llm_client

    def generate(
        self,
        implementation_brief,
        existing_plan,
        feedback,
        model,
        feedback_source,
    ):
        return _execution_plan(feedback, feedback_source, model), _tasks()


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need plan revision",
        "mvp_goal": "Revise plans from feedback",
        "product_surface": "CLI",
        "scope": ["Plan revision"],
        "non_goals": ["Plan execution"],
        "assumptions": ["Original plan exists"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Losing lineage"],
        "validation_plan": "Run plan reviser tests",
        "definition_of_done": ["Revised plans link to originals"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _original_plan():
    return {
        "id": "plan-original",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan(
    feedback="Tighten scope",
    feedback_source="inline",
    model="test-model",
):
    return {
        "id": "plan-revised",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up revised work"},
        ],
        "test_strategy": "Run pytest tests/test_plan_reviser.py",
        "handoff_prompt": "Build the revised plan",
        "status": "draft",
        "generation_model": model,
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {
            "revised_from_plan_id": "plan-original",
            "revision_feedback": feedback,
            "revision_feedback_source": feedback_source,
            "revised_at": "2026-04-23T00:00:00",
            "lineage": {
                "revised_from_plan_id": "plan-original",
                "revision_feedback": feedback,
                "revision_feedback_source": feedback_source,
                "revised_at": "2026-04-23T00:00:00",
            }
        },
    }


def _original_task():
    return {
        "id": "task-original",
        "title": "Setup project",
        "description": "Create the baseline project structure",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["pyproject.toml"],
        "acceptance_criteria": ["Project installs"],
        "estimated_complexity": "low",
        "status": "pending",
    }


def _tasks():
    return [
        {
            "id": "task-revised",
            "title": "Setup revised project",
            "description": "Create the revised baseline project structure",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Project installs"],
            "estimated_complexity": "low",
            "status": "pending",
        }
    ]
