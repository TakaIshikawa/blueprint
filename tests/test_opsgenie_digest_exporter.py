import json

from blueprint.exporters import OpsgenieDigestExporter
from blueprint.exporters.registry import create_exporter, get_exporter_registration


def test_opsgenie_digest_renders_escalation_sections(tmp_path):
    output_path = tmp_path / "opsgenie.json"

    OpsgenieDigestExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    assert output_path.read_text().endswith("\n")
    payload = json.loads(output_path.read_text())
    sections = {section["key"]: section for section in payload["sections"]}

    assert payload["schema_version"] == "blueprint.opsgenie_digest.v1"
    assert payload["plan_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Opsgenie Brief",
        "milestones": ["Operate", "Polish"],
        "plan_id": "plan-test",
        "plan_status": "in_progress",
        "project_type": "service",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "task_count": 6,
    }
    assert payload["routing_hints"] == {
        "alias": "plan-test:opsgenie-digest",
        "default_priority": "P3",
        "message": "Opsgenie Brief escalation digest for plan-test",
        "source": "blueprint",
        "tags": ["blueprint", "execution-plan", "codex", "service", "Opsgenie"],
    }
    assert payload["summary"] == {
        "escalation_task_count": 4,
        "section_counts": {
            "blocked": 2,
            "dependency_sensitive": 4,
            "high_risk": 2,
            "production_touching": 2,
            "rollback_sensitive": 3,
        },
        "task_count": 6,
    }
    assert [section["key"] for section in payload["sections"]] == [
        "blocked",
        "high_risk",
        "production_touching",
        "rollback_sensitive",
        "dependency_sensitive",
    ]
    assert [task["id"] for task in sections["blocked"]["tasks"]] == [
        "task-incident",
        "task-blocked",
    ]
    assert [task["id"] for task in sections["high_risk"]["tasks"]] == [
        "task-incident",
        "task-deploy",
    ]
    assert [task["id"] for task in sections["production_touching"]["tasks"]] == [
        "task-incident",
        "task-deploy",
    ]
    assert [task["id"] for task in sections["rollback_sensitive"]["tasks"]] == [
        "task-incident",
        "task-blocked",
        "task-deploy",
    ]
    assert [task["id"] for task in sections["dependency_sensitive"]["tasks"]] == [
        "task-incident",
        "task-blocked",
        "task-deploy",
        "task-api",
    ]


def test_opsgenie_digest_task_summaries_include_routing_priority_and_evidence():
    payload = OpsgenieDigestExporter().render_payload(_execution_plan(), _implementation_brief())
    sections = {section["key"]: section for section in payload["sections"]}
    task = sections["blocked"]["tasks"][0]

    assert task == {
        "dependencies": [],
        "evidence": {
            "acceptance_criteria": ["Incident mitigation is validated"],
            "blocked_reason": "Waiting for database access",
            "files_or_modules": ["src/db.py"],
            "runbook_or_rollback": [
                "https://runbooks.example.com/incident",
                "Revert deploy via release dashboard",
            ],
            "validation_commands": ["poetry run pytest tests/test_incident.py"],
        },
        "id": "task-incident",
        "milestone": "Operate",
        "owner_type": "human",
        "priority": "P1",
        "responders": [{"name": "Database Escalation Policy", "type": "escalation"}],
        "risk_level": "critical",
        "routing_hint": {
            "alias": "task-incident",
            "entity": "Operate",
            "message": "P1 task-incident: Patch production incident rollback",
            "tags": [
                "blueprint-task",
                "blocked",
                "critical",
                "database",
                "deploy",
                "production",
                "rollback",
                "high-risk",
                "payments",
            ],
            "team": "platform-oncall",
        },
        "signals": [
            "database",
            "deploy",
            "production",
            "rollback",
            "high-risk",
            "blocked",
        ],
        "status": "blocked",
        "title": "Patch production incident rollback",
    }

    dependency_task = sections["dependency_sensitive"]["tasks"][-1]
    assert dependency_task["id"] == "task-api"
    assert dependency_task["dependencies"] == ["task-deploy"]
    assert dependency_task["priority"] == "P3"
    assert dependency_task["responders"] == [{"name": "api-team", "type": "team"}]


def test_opsgenie_digest_empty_sections_are_deterministic():
    plan = _execution_plan()
    plan["tasks"] = []

    payload = OpsgenieDigestExporter().render_payload(plan, _implementation_brief())

    assert payload["summary"]["escalation_task_count"] == 0
    assert payload["summary"]["section_counts"] == {
        "blocked": 0,
        "dependency_sensitive": 0,
        "high_risk": 0,
        "production_touching": 0,
        "rollback_sensitive": 0,
    }
    assert all(section["tasks"] == [] for section in payload["sections"])


def test_opsgenie_digest_is_registered_with_underscore_alias():
    registration = get_exporter_registration("opsgenie-digest")

    assert registration.default_format == "json"
    assert registration.extension == ".json"
    assert get_exporter_registration("opsgenie_digest") == registration
    assert isinstance(create_exporter("opsgenie_digest"), OpsgenieDigestExporter)


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
                "title": "Patch production incident rollback",
                "description": "Rollback a critical production deploy path touching the database.",
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
                    "opsgenie_responders": ["Database Escalation Policy"],
                    "opsgenie_tags": ["payments"],
                    "opsgenie_team": "platform-oncall",
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
                },
            },
            {
                "id": "task-deploy",
                "title": "Deploy integration worker",
                "description": "Deploy worker for production integration traffic.",
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
                "depends_on": ["task-deploy"],
                "files_or_modules": ["src/api.py"],
                "acceptance_criteria": ["Queue errors are handled"],
                "estimated_complexity": "medium",
                "risk_level": "medium",
                "status": "pending",
                "metadata": {"team": "api-team"},
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
            {
                "id": "task-cleanup",
                "title": "Clean up copy",
                "description": "Small wording pass.",
                "milestone": "Polish",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["README.md"],
                "acceptance_criteria": ["Copy is clear"],
                "estimated_complexity": "low",
                "risk_level": "low",
                "status": "pending",
            },
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Opsgenie Brief",
        "domain": "operations",
        "target_user": "SREs",
        "buyer": "Engineering",
        "workflow_context": "Incident response",
        "problem_statement": "Need escalation visibility",
        "mvp_goal": "Export Opsgenie digest",
        "product_surface": "CLI",
        "scope": ["Opsgenie digest exporter"],
        "non_goals": ["Opsgenie API integration"],
        "assumptions": ["Tasks have metadata"],
        "architecture_notes": "Use exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["Opsgenie"],
        "risks": ["Missing operational context"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Digest groups escalation tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
