import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_search_execution_plans_returns_ranked_plan_and_task_matches(tmp_path):
    store = _seed_store(tmp_path)

    results = store.search_execution_plans("payments")

    assert [result["plan_id"] for result in results] == [
        "plan-payments",
        "plan-webhooks",
    ]
    assert results[0]["matched_task_ids"] == []
    assert "target_repo" in results[0]["matched_fields"]
    assert results[1]["matched_task_ids"] == ["task-webhook"]
    assert "tasks.description" in results[1]["matched_fields"]
    assert any(
        match["field"] == "tasks.description" and match["task_id"] == "task-webhook"
        for match in results[1]["matches"]
    )


def test_search_execution_plans_matches_task_acceptance_and_files_once(tmp_path):
    store = _seed_store(tmp_path)

    results = store.search_execution_plans("webhook")

    assert [result["plan_id"] for result in results] == ["plan-webhooks"]
    assert results[0]["matched_task_ids"] == ["task-webhook"]
    assert "tasks.acceptance_criteria" in results[0]["matched_fields"]
    assert "tasks.files_or_modules" in results[0]["matched_fields"]
    assert len(results) == 1


def test_search_execution_plans_filters_status_target_engine_and_limit(tmp_path):
    store = _seed_store(tmp_path)

    status_results = store.search_execution_plans("run", status="queued")
    engine_results = store.search_execution_plans("run", target_engine="relay")
    limited_results = store.search_execution_plans("run", limit=1)

    assert [result["plan_id"] for result in status_results] == ["plan-webhooks"]
    assert [result["plan_id"] for result in engine_results] == ["plan-webhooks"]
    assert len(limited_results) == 1


def test_plan_search_cli_outputs_no_results(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    _seed_store(tmp_path)

    result = CliRunner().invoke(cli, ["plan", "search", "missing-term"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "No execution plans matched"


def test_plan_search_cli_outputs_text_and_json(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    _seed_store(tmp_path)

    text_result = CliRunner().invoke(
        cli,
        ["plan", "search", "webhook", "--status", "queued", "--target-engine", "relay"],
    )
    json_result = CliRunner().invoke(cli, ["plan", "search", "webhook", "--json"])

    assert text_result.exit_code == 0, text_result.output
    assert "plan-webhooks (queued, relay)" in text_result.output
    assert "Tasks:  task-webhook" in text_result.output
    assert "tasks.files_or_modules [task-webhook]" in text_result.output
    assert "Total: 1 plans" in text_result.output

    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert [result["plan_id"] for result in payload] == ["plan-webhooks"]
    assert payload[0]["status"] == "queued"
    assert payload[0]["target_engine"] == "relay"
    assert payload[0]["matched_task_ids"] == ["task-webhook"]
    assert payload[0]["matches"][0]["snippet"]


def _write_config(tmp_path, monkeypatch):
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


def _seed_store(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(
        {
            **_execution_plan("plan-payments"),
            "target_engine": "codex",
            "target_repo": "github.com/example/payments-api",
            "test_strategy": "Run checkout regression tests",
            "handoff_prompt": "Add refund reconciliation support",
            "status": "ready",
        },
        [
            {
                **_task("task-ledger"),
                "title": "Add ledger sync",
                "description": "Persist reconciliation rows",
                "files_or_modules": ["src/ledger.py"],
                "acceptance_criteria": ["Ledger rows are stored"],
            }
        ],
    )
    store.insert_execution_plan(
        {
            **_execution_plan("plan-webhooks"),
            "target_engine": "relay",
            "target_repo": "github.com/example/integrations",
            "test_strategy": "Run integration pytest suite",
            "handoff_prompt": "Run webhook validation after implementation",
            "status": "queued",
        },
        [
            {
                **_task("task-webhook"),
                "title": "Build webhook handler",
                "description": "Process payments provider callbacks",
                "files_or_modules": ["src/webhook_handler.py"],
                "acceptance_criteria": ["Webhook retries are idempotent"],
            },
            {
                **_task("task-worker"),
                "title": "Run background worker",
                "description": "Run queued integration jobs",
                "files_or_modules": ["src/worker.py"],
                "acceptance_criteria": ["Worker drains queued jobs"],
            },
        ],
    )
    return store


def _implementation_brief():
    return {
        "id": "ib-search",
        "source_brief_id": "sb-search",
        "title": "Search Brief",
        "domain": "testing",
        "target_user": "Operators",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need searchable plans",
        "mvp_goal": "Find prior planning work",
        "product_surface": "CLI",
        "scope": ["Plan search"],
        "non_goals": ["External indexing"],
        "assumptions": ["Plans are stored in SQLite"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Poor ranking"],
        "validation_plan": "Run plan search tests",
        "definition_of_done": ["Operators can search plans"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan(plan_id):
    return {
        "id": plan_id,
        "implementation_brief_id": "ib-search",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Build feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _task(task_id):
    return {
        "id": task_id,
        "title": "Build task",
        "description": "Implement the work",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": ["Feature works"],
        "estimated_complexity": "medium",
        "status": "pending",
    }
