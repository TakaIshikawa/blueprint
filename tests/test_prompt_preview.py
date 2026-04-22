from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.generators.brief_generator import BriefGenerator
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.store import init_db


def test_brief_prompt_outputs_generator_prompt_to_stdout(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    source_brief = _source_brief()
    store.insert_source_brief(source_brief)

    result = CliRunner().invoke(cli, ["brief", "prompt", source_brief["id"]])

    assert result.exit_code == 0, result.output
    assert result.output == BriefGenerator.build_prompt(source_brief)


def test_plan_prompt_outputs_generator_prompt_to_stdout(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)

    result = CliRunner().invoke(cli, ["plan", "prompt", implementation_brief["id"]])

    assert result.exit_code == 0, result.output
    assert result.output == PlanGenerator.build_prompt(implementation_brief)


def test_brief_prompt_writes_generator_prompt_to_file(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    source_brief = _source_brief()
    store.insert_source_brief(source_brief)
    output_path = tmp_path / "prompts" / "brief.txt"

    result = CliRunner().invoke(
        cli,
        ["brief", "prompt", source_brief["id"], "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Wrote prompt to:" in result.output
    assert output_path.read_text() == BriefGenerator.build_prompt(source_brief)


def test_plan_prompt_writes_generator_prompt_to_file(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)
    output_path = tmp_path / "prompts" / "plan.txt"

    result = CliRunner().invoke(
        cli,
        ["plan", "prompt", implementation_brief["id"], "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Wrote prompt to:" in result.output
    assert output_path.read_text() == PlanGenerator.build_prompt(implementation_brief)


def test_prompt_commands_report_missing_ids(tmp_path, monkeypatch):
    _setup_store(tmp_path, monkeypatch)

    brief_result = CliRunner().invoke(cli, ["brief", "prompt", "src-missing"])
    plan_result = CliRunner().invoke(cli, ["plan", "prompt", "ib-missing"])

    assert brief_result.exit_code != 0
    assert "Source brief not found: src-missing" in brief_result.output
    assert plan_result.exit_code != 0
    assert "Implementation brief not found: ib-missing" in plan_result.output


def test_prompt_builders_keep_private_wrapper_parity():
    source_brief = _source_brief()
    implementation_brief = _implementation_brief()

    assert BriefGenerator.build_prompt(source_brief) == BriefGenerator(None)._build_prompt(
        source_brief
    )
    assert PlanGenerator.build_prompt(implementation_brief) == PlanGenerator(
        None
    )._build_prompt(implementation_brief)


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
        "definition_of_done": ["CLI prints queued reviews", "Tests cover empty and non-empty queues"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
