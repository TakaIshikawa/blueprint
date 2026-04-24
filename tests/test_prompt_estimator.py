import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.generators.brief_generator import BriefGenerator
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.llm.estimator import estimate_prompt
from blueprint.llm.providers.anthropic import AnthropicLLMProvider
from blueprint.store import init_db


def test_estimate_prompt_counts_words_tokens_and_cost():
    estimate = estimate_prompt("one two three four", model="sonnet")

    assert estimate.model == "sonnet"
    assert estimate.resolved_model == "claude-sonnet-4-5"
    assert estimate.characters == 18
    assert estimate.words == 4
    assert estimate.estimated_tokens == 5
    assert estimate.estimated_input_cost_usd == 0.000015


def test_brief_estimate_json_uses_prompt_builder_without_api_key_or_client(
    tmp_path,
    monkeypatch,
):
    store = _setup_store(tmp_path, monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(AnthropicLLMProvider, "__init__", _fail_if_client_is_created)
    source_brief = _source_brief()
    store.insert_source_brief(source_brief)

    result = CliRunner().invoke(
        cli,
        ["brief", "estimate", source_brief["id"], "--model", "sonnet", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    expected = estimate_prompt(BriefGenerator.build_prompt(source_brief), model="sonnet")
    assert payload == expected.to_dict()


def test_plan_estimate_json_uses_prompt_builder_without_api_key_or_client(
    tmp_path,
    monkeypatch,
):
    store = _setup_store(tmp_path, monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(AnthropicLLMProvider, "__init__", _fail_if_client_is_created)
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)

    result = CliRunner().invoke(
        cli,
        ["plan", "estimate", implementation_brief["id"], "--model", "opus", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    expected = estimate_prompt(PlanGenerator.build_prompt(implementation_brief), model="opus")
    assert payload == expected.to_dict()


def test_estimate_human_output_includes_key_values(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    source_brief = _source_brief()
    store.insert_source_brief(source_brief)
    estimate = estimate_prompt(BriefGenerator.build_prompt(source_brief), model="opus")

    result = CliRunner().invoke(cli, ["brief", "estimate", source_brief["id"]])

    assert result.exit_code == 0, result.output
    assert f"Model: {estimate.model}" in result.output
    assert f"Resolved model: {estimate.resolved_model}" in result.output
    assert f"Characters: {estimate.characters}" in result.output
    assert f"Words: {estimate.words}" in result.output
    assert f"Estimated tokens: {estimate.estimated_tokens}" in result.output
    assert f"Estimated input cost (USD): ${estimate.estimated_input_cost_usd:.6f}" in (
        result.output
    )


def test_estimate_commands_report_missing_ids(tmp_path, monkeypatch):
    _setup_store(tmp_path, monkeypatch)

    brief_result = CliRunner().invoke(cli, ["brief", "estimate", "src-missing"])
    plan_result = CliRunner().invoke(cli, ["plan", "estimate", "ib-missing"])

    assert brief_result.exit_code != 0
    assert "Source brief not found: src-missing" in brief_result.output
    assert plan_result.exit_code != 0
    assert "Implementation brief not found: ib-missing" in plan_result.output


def _fail_if_client_is_created(*args, **kwargs):
    raise AssertionError("estimate command must not instantiate the Anthropic client")


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


def _source_brief():
    return {
        "id": "src-test",
        "title": "Review Queue Assistant",
        "domain": "developer_tools",
        "summary": "Help maintainers triage pull request review queues.",
        "source_project": "manual",
        "source_entity_type": "design_brief",
        "source_id": "manual-1",
        "source_payload": {},
        "source_links": {},
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "src-test",
        "title": "Review Queue Assistant",
        "domain": "developer_tools",
        "target_user": "Repository maintainers",
        "buyer": "Engineering managers",
        "workflow_context": "Daily pull request triage",
        "problem_statement": "Maintainers need a deterministic way to identify blocked reviews.",
        "mvp_goal": "Build a CLI that summarizes pending reviews and blockers.",
        "product_surface": "CLI",
        "scope": ["Load review queue data", "Render pending review summary"],
        "non_goals": ["Automated reviewer assignment", "Hosted dashboard"],
        "assumptions": ["Review data is available locally"],
        "architecture_notes": "Use a small command module backed by store reads.",
        "data_requirements": "Pull request IDs, statuses, reviewers, and blocker notes.",
        "integration_points": ["Local repository metadata"],
        "risks": ["Stale data may mislead users; show timestamps in output"],
        "validation_plan": "Run unit tests and manually inspect CLI output.",
        "definition_of_done": [
            "CLI prints queued reviews",
            "Tests cover empty and non-empty queues",
        ],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
