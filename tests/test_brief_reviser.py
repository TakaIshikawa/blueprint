import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.generators.brief_reviser import BriefReviser
from blueprint.store import Store, init_db


def test_brief_reviser_builds_prompt_and_returns_revised_brief():
    llm_client = FakeLLMClient(api_key="test-key", default_model="test-default")
    reviser = BriefReviser(llm_client)

    revised = reviser.generate(
        existing_brief=_implementation_brief(),
        source_brief=_source_brief(),
        feedback="Add an import preview before committing records",
        model="resolved-sonnet",
    )

    assert revised["id"].startswith("ib-")
    assert revised["id"] != "ib-original"
    assert revised["source_brief_id"] == "sb-test"
    assert revised["title"] == "Revised Import Preview Workflow"
    assert revised["scope"] == ["Preview parsed rows", "Persist accepted rows"]
    assert revised["generation_model"] == "resolved-sonnet"
    assert revised["generation_tokens"] == 321
    assert "# Original Source Brief Context" in llm_client.prompt
    assert "# Existing Implementation Brief" in llm_client.prompt
    assert "# Human Feedback" in llm_client.prompt
    assert "Original import workflow" in llm_client.prompt
    assert "Add an import preview before committing records" in llm_client.prompt
    assert "Source context with constraints" in llm_client.prompt


def test_brief_revise_cli_creates_separate_brief_from_feedback_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    feedback_path = tmp_path / "feedback.txt"
    feedback_path.write_text("Add an import preview before committing records")

    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    store.insert_implementation_brief(_implementation_brief())

    monkeypatch.setattr("blueprint.cli.LLMClient", FakeLLMClient)

    result = CliRunner().invoke(
        cli,
        [
            "brief",
            "revise",
            "ib-original",
            "--feedback",
            str(feedback_path),
            "--model",
            "sonnet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Generated revised implementation brief" in result.output
    assert "Revised from: ib-original" in result.output

    persisted_original = Store(str(tmp_path / "blueprint.db")).get_implementation_brief(
        "ib-original"
    )
    persisted_briefs = Store(str(tmp_path / "blueprint.db")).list_implementation_briefs()
    persisted_revised = next(
        brief for brief in persisted_briefs if brief["id"] != "ib-original"
    )

    assert persisted_original["title"] == "Original import workflow"
    assert persisted_revised["source_brief_id"] == "sb-test"
    assert persisted_revised["title"] == "Revised Import Preview Workflow"
    assert persisted_revised["scope"] == ["Preview parsed rows", "Persist accepted rows"]
    assert persisted_revised["status"] == "draft"
    assert persisted_revised["generation_model"] == "resolved-sonnet"
    assert persisted_revised["generation_tokens"] == 321
    assert "Add an import preview before committing records" in (
        persisted_revised["generation_prompt"] or ""
    )
    assert "Existing Implementation Brief" in (persisted_revised["generation_prompt"] or "")


class FakeLLMClient:
    def __init__(self, api_key, default_model):
        self.api_key = api_key
        self.default_model = default_model
        self.prompt = ""

    @classmethod
    def resolve_model(cls, model_alias):
        return f"resolved-{model_alias}"

    def generate(self, prompt, model, temperature, max_tokens, system):
        self.prompt = prompt
        return {
            "content": json.dumps(
                {
                    "title": "Revised Import Preview Workflow",
                    "target_user": "Operations teams importing backlog records",
                    "buyer": "Engineering operations",
                    "workflow_context": "Before committing imported records",
                    "problem_statement": "Operators need confidence before storing imports.",
                    "mvp_goal": "Provide a preview and then persist accepted rows.",
                    "product_surface": "CLI",
                    "scope": ["Preview parsed rows", "Persist accepted rows"],
                    "non_goals": ["Interactive row editing"],
                    "assumptions": ["The source parser remains unchanged"],
                    "architecture_notes": "Add revision-aware brief generation only.",
                    "data_requirements": "Source brief, existing brief, and feedback text.",
                    "integration_points": ["Blueprint store"],
                    "risks": ["Preview output could diverge from persisted rows"],
                    "validation_plan": "Run focused brief reviser tests.",
                    "definition_of_done": ["A revised brief is stored separately"],
                }
            ),
            "model": model,
            "usage": {"total_tokens": 321},
        }


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


def _source_brief():
    return {
        "id": "sb-test",
        "title": "Source import workflow",
        "domain": "operations",
        "summary": "Source context with constraints",
        "source_project": "manual",
        "source_entity_type": "markdown_brief",
        "source_id": "source-import-workflow",
        "source_payload": {
            "constraints": ["Records should not be committed without review"]
        },
        "source_links": {"file": "brief.md"},
    }


def _implementation_brief():
    return {
        "id": "ib-original",
        "source_brief_id": "sb-test",
        "title": "Original import workflow",
        "domain": "operations",
        "target_user": "Operations teams",
        "buyer": "Engineering operations",
        "workflow_context": "Importing backlog records",
        "problem_statement": "Operators need a reliable import path.",
        "mvp_goal": "Import records from a supported source.",
        "product_surface": "CLI",
        "scope": ["Persist parsed rows"],
        "non_goals": ["Preview before commit"],
        "assumptions": ["Source files are well formed"],
        "architecture_notes": "Use existing importer and store methods.",
        "data_requirements": "Source rows and normalized brief records.",
        "integration_points": ["Blueprint store"],
        "risks": ["Invalid rows may be committed"],
        "validation_plan": "Run importer tests.",
        "definition_of_done": ["Rows are stored"],
        "status": "planned",
        "generation_model": "original-model",
        "generation_tokens": 100,
        "generation_prompt": "original prompt",
    }
