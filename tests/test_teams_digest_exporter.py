import json

from blueprint.exporters import TeamsDigestExporter
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.registry import create_exporter, get_exporter_registration


def test_teams_digest_exporter_renders_stable_message_payload(tmp_path):
    output_path = tmp_path / "teams.json"

    TeamsDigestExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    assert output_path.read_text().endswith("\n")
    payload = json.loads(output_path.read_text())
    content = _card_content(payload)

    assert payload["schema_version"] == "blueprint.teams_digest.v1"
    assert payload["type"] == "message"
    assert payload["title"] == "Execution Plan Digest: Test Brief"
    assert payload["summary"] == "Test Brief has 6 tasks across 2 milestones for plan plan-test."
    assert payload["attachments"][0]["contentType"] == "application/vnd.microsoft.card.message"
    assert content["@type"] == "MessageCard"
    assert content["themeColor"] == "D13438"
    assert [section["activityTitle"] for section in content["sections"]] == [
        "Test Brief",
        "Blocked Tasks",
        "High-Risk Tasks",
        "Foundation",
        "Interface",
    ]
    assert content["sections"][0]["facts"] == [
        {"name": "Plan", "value": "plan-test"},
        {"name": "Brief", "value": "ib-test"},
        {
            "name": "Status",
            "value": "pending: 2 | in_progress: 1 | completed: 1 | blocked: 1 | skipped: 1",
        },
        {
            "name": "Summary",
            "value": "Test Brief has 6 tasks across 2 milestones for plan plan-test.",
        },
    ]


def test_teams_digest_highlights_blocked_and_high_risk_tasks():
    payload = TeamsDigestExporter().render_payload(_execution_plan(), _implementation_brief())
    sections = {section["activityTitle"]: section for section in _card_content(payload)["sections"]}

    assert sections["Blocked Tasks"]["facts"] == [
        {
            "name": "task-copy",
            "value": (
                "Write copy | status: blocked | owner: @copy-team | deps: task-api | "
                "blocked: Waiting for product direction"
            ),
        }
    ]
    assert sections["High-Risk Tasks"]["facts"] == [
        {
            "name": "task-api",
            "value": (
                "Build API | status: pending | owner: @api-team | "
                "deps: task-setup, task-schema | risk: P0"
            ),
        }
    ]


def test_teams_digest_empty_plan_includes_empty_callouts():
    plan = _execution_plan()
    plan["tasks"] = []

    payload = TeamsDigestExporter().render_payload(plan, _implementation_brief())
    sections = {section["activityTitle"]: section for section in _card_content(payload)["sections"]}

    assert payload["summary"] == "Test Brief has 0 tasks across 2 milestones for plan plan-test."
    assert sections["Blocked Tasks"]["facts"] == [
        {"name": "Tasks", "value": "No blocked tasks."}
    ]
    assert sections["High-Risk Tasks"]["facts"] == [
        {"name": "Tasks", "value": "No high-risk tasks."}
    ]


def test_teams_digest_markdown_sensitive_content_is_escaped():
    plan = _execution_plan()
    plan["milestones"] = [{"name": "Deploy [prod]*"}]
    plan["tasks"] = [
        {
            "id": "task[*]",
            "title": "Fix *markdown* [link](x) <tag> | pipe",
            "description": "Handle content",
            "milestone": "Deploy [prod]*",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task`dep`"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["Escaped"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting on `owner` & [approval]",
            "metadata": {"teams_owner": "@ops*team"},
        }
    ]

    payload = TeamsDigestExporter().render_payload(plan, _implementation_brief())
    sections = {section["activityTitle"]: section for section in _card_content(payload)["sections"]}

    assert "Deploy \\[prod\\]\\*" in sections
    blocked_fact = sections["Blocked Tasks"]["facts"][0]
    assert blocked_fact["name"] == "task\\[\\*\\]"
    assert "Fix \\*markdown\\* \\[link\\]\\(x\\) \\<tag\\> \\| pipe" in blocked_fact["value"]
    assert "owner: @ops\\*team" in blocked_fact["value"]
    assert "deps: task\\`dep\\`" in blocked_fact["value"]
    assert "blocked: Waiting on \\`owner\\` & \\[approval\\]" in blocked_fact["value"]


def test_teams_digest_is_registered_and_importable():
    registration = get_exporter_registration("teams-digest")

    assert registration.default_format == "json"
    assert registration.extension == ".json"
    assert isinstance(create_exporter("teams_digest"), TeamsDigestExporter)


def test_teams_digest_rendered_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "teams.json"
    TeamsDigestExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    findings = validate_rendered_export(
        target="teams-digest",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def _card_content(payload):
    return payload["attachments"][0]["content"]


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "in_progress",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": _tasks(),
    }


def _tasks():
    return [
        {
            "id": "task-setup",
            "title": "Setup project",
            "description": "Create the baseline project structure",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Project installs"],
            "estimated_complexity": "low",
            "status": "completed",
        },
        {
            "id": "task-schema",
            "title": "Build schema",
            "description": "Create persistence schema",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/schema.py"],
            "acceptance_criteria": ["Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "skipped",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "risk_level": "high",
            "status": "pending",
            "metadata": {"priority": "P0", "teams_owner": "@api-team"},
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting for product direction",
            "metadata": {"teams_owner": "@copy-team"},
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Create the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "in_progress",
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the digest output",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Docs describe usage"],
            "estimated_complexity": "low",
            "status": "pending",
        },
    ]


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a Teams digest export",
        "mvp_goal": "Export Teams digests for execution plans",
        "product_surface": "CLI",
        "scope": ["Teams digest exporter"],
        "non_goals": ["Full status report"],
        "assumptions": ["Tasks already have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Digest contains progress and blockers"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
