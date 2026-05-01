from blueprint.exporters import PagerDutyDigestExporter
from blueprint.exporters.registry import create_exporter, get_exporter_registration


def test_pagerduty_digest_groups_escalation_buckets(tmp_path):
    output_path = tmp_path / "pagerduty.md"

    PagerDutyDigestExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert content.endswith("\n")
    assert "# PagerDuty Escalation Digest: plan-test" in content
    assert "- Escalation buckets: immediate: 2 | scheduled: 1 | watchlist: 1 | informational: 1" in content
    assert _section(content, "Immediate") == [
        "- `task-incident` Patch incident rollback",
        "  - owner_type: human",
        "  - risk_level: critical",
        "  - blocked_reason: Waiting for database access",
        "  - risk_signals: blocked, critical, database, incident, rollback, high-risk",
        "  - suggested_validation: poetry run pytest tests/test_incident.py",
        "  - runbook_or_rollback: https://runbooks.example.com/incident; Revert deploy via release dashboard",
        "- `task-blocked` Unblock vendor webhook",
        "  - owner_type: agent",
        "  - risk_level: medium",
        "  - blocked_reason: Vendor sandbox credentials missing",
        "  - risk_signals: blocked, external, integration, vendor, webhook",
        "  - suggested_validation: curl -f https://vendor.example.com/health",
        "  - runbook_or_rollback: Use webhook retry runbook",
    ]
    assert _section(content, "Scheduled") == [
        "- `task-deploy` Deploy integration worker",
        "  - owner_type: agent",
        "  - risk_level: high",
        "  - blocked_reason: None",
        "  - risk_signals: deploy, integration, high-risk",
        "  - suggested_validation: pytest tests/test_worker.py; npm run test:e2e",
        "  - runbook_or_rollback: Roll back worker image tag",
    ]
    assert _section(content, "Watchlist") == [
        "- `task-api` Harden API queue handling",
        "  - owner_type: either",
        "  - risk_level: medium",
        "  - blocked_reason: None",
        "  - risk_signals: api, queue",
        "  - suggested_validation: Not specified",
        "  - runbook_or_rollback: Not specified",
    ]
    assert _section(content, "Informational") == [
        "- `task-docs` Update docs",
        "  - owner_type: agent",
        "  - risk_level: low",
        "  - blocked_reason: None",
        "  - risk_signals: none",
        "  - suggested_validation: Not specified",
        "  - runbook_or_rollback: Not specified",
    ]


def test_pagerduty_digest_empty_buckets_are_explicit():
    plan = _execution_plan()
    plan["tasks"] = []

    content = PagerDutyDigestExporter().render(plan, _implementation_brief())

    assert "- Escalation buckets: immediate: 0 | scheduled: 0 | watchlist: 0 | informational: 0" in content
    assert _section(content, "Immediate") == ["- None."]
    assert _section(content, "Scheduled") == ["- None."]
    assert _section(content, "Watchlist") == ["- None."]
    assert _section(content, "Informational") == ["- None."]


def test_pagerduty_digest_is_registered_with_underscore_alias():
    registration = get_exporter_registration("pagerduty-digest")

    assert registration.default_format == "markdown"
    assert registration.extension == ".md"
    assert get_exporter_registration("pagerduty_digest") == registration
    assert isinstance(create_exporter("pagerduty_digest"), PagerDutyDigestExporter)


def _section(content: str, heading: str) -> list[str]:
    lines = content.splitlines()
    start = lines.index(f"## {heading}") + 1
    end = next(
        (index for index in range(start, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    return [line for line in lines[start:end] if line]


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Operate"}, {"name": "Polish"}],
        "test_strategy": "Run focused tests",
        "handoff_prompt": "Build the plan",
        "status": "in_progress",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-incident",
                "title": "Patch incident rollback",
                "description": "Rollback a critical incident path touching the database.",
                "milestone": "Operate",
                "owner_type": "human",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/db.py"],
                "acceptance_criteria": ["Incident mitigation is validated"],
                "estimated_complexity": "high",
                "risk_level": "critical",
                "test_command": "poetry run pytest tests/test_incident.py",
                "status": "blocked",
                "blocked_reason": "Waiting for database access",
                "metadata": {
                    "runbook_url": "https://runbooks.example.com/incident",
                    "rollback_plan": "Revert deploy via release dashboard",
                },
            },
            {
                "id": "task-blocked",
                "title": "Unblock vendor webhook",
                "description": "Restore external vendor webhook integration.",
                "milestone": "Operate",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/webhook.py"],
                "acceptance_criteria": ["Webhook retry succeeds"],
                "estimated_complexity": "medium",
                "risk_level": "medium",
                "status": "blocked",
                "metadata": {
                    "blocked_reason": "Vendor sandbox credentials missing",
                    "validation_command": "curl -f https://vendor.example.com/health",
                    "runbook": "Use webhook retry runbook",
                },
            },
            {
                "id": "task-deploy",
                "title": "Deploy integration worker",
                "description": "Deploy worker for integration traffic.",
                "milestone": "Operate",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["deploy/worker.yaml"],
                "acceptance_criteria": ["Integration worker processes events"],
                "estimated_complexity": "medium",
                "risk_level": "high",
                "status": "pending",
                "metadata": {
                    "validation_commands": {
                        "test": ["pytest tests/test_worker.py"],
                        "build": ["npm run test:e2e"],
                    },
                    "rollback_hint": "Roll back worker image tag",
                },
            },
            {
                "id": "task-api",
                "title": "Harden API queue handling",
                "description": "Make API queue behavior more resilient.",
                "milestone": "Operate",
                "owner_type": "either",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/api.py"],
                "acceptance_criteria": ["Queue errors are handled"],
                "estimated_complexity": "medium",
                "risk_level": "medium",
                "status": "pending",
            },
            {
                "id": "task-docs",
                "title": "Update docs",
                "description": "Document the operations flow.",
                "milestone": "Polish",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["README.md"],
                "acceptance_criteria": ["Docs are clear"],
                "estimated_complexity": "low",
                "risk_level": "low",
                "status": "completed",
            },
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "PagerDuty Brief",
        "domain": "operations",
        "target_user": "SREs",
        "buyer": "Engineering",
        "workflow_context": "Incident response",
        "problem_statement": "Need escalation visibility",
        "mvp_goal": "Export PagerDuty digest",
        "product_surface": "CLI",
        "scope": ["PagerDuty digest exporter"],
        "non_goals": ["PagerDuty API integration"],
        "assumptions": ["Tasks have metadata"],
        "architecture_notes": "Use exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["PagerDuty"],
        "risks": ["Missing operational context"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Digest groups escalation tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
