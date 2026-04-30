import json

from blueprint.rollout_checklist import (
    SECTION_ORDER,
    RolloutChecklist,
    build_rollout_checklist,
    rollout_checklist_to_dict,
)


def test_low_risk_plan_has_stable_sections_and_deterministic_markdown():
    checklist = build_rollout_checklist(_brief(), _plan())

    assert isinstance(checklist, RolloutChecklist)
    assert [section.name for section in checklist.sections] == list(SECTION_ORDER)
    assert checklist.sections[0].name == "preflight"
    assert checklist.sections[-1].name == "rollback"
    assert "Use the normal release path after validation passes." in _items(checklist, "rollout")
    assert (
        "Use version-control rollback for task-scoped code changes if release validation fails."
        in _items(checklist, "rollback")
    )

    markdown = checklist.to_markdown()

    assert markdown == checklist.to_markdown()
    assert markdown.startswith("# Rollout Checklist for plan-rollout\n\n## Preflight\n")
    assert "\n## Implementation\n" in markdown
    assert "\n## Validation\n" in markdown
    assert "\n## Rollout\n" in markdown
    assert "\n## Rollback\n" in markdown
    assert "- [ ] Run validation command: `poetry run pytest tests/test_ui.py`." in markdown


def test_high_risk_plan_adds_rollout_and_rollback_checklist_items():
    plan = _plan()
    plan["tasks"][0]["risk_level"] = "high"
    plan["tasks"][0]["metadata"] = {
        "rollback_hint": "Disable the route flag and redeploy the previous handler."
    }
    brief = _brief()
    brief["risks"] = ["API response shape changes production traffic"]

    checklist = build_rollout_checklist(brief, plan)

    assert any("Review known release risks" in item for item in _items(checklist, "preflight"))
    assert any("Roll out high-risk tasks" in item for item in _items(checklist, "rollout"))
    assert any("`task-api`" in item for item in _items(checklist, "rollout"))
    assert any(
        "Prepare rollback steps for high-risk tasks" in item
        for item in _items(checklist, "rollback")
    )
    assert any("Disable the route flag" in item for item in _items(checklist, "rollback"))


def test_database_or_migration_files_add_rollout_and_rollback_items():
    plan = _plan()
    plan["tasks"].append(
        _task(
            "task-migration",
            "Add account audit migration",
            files=["migrations/20260501_add_account_audit.sql"],
            acceptance=["Migration applies cleanly"],
            test_command="poetry run pytest tests/test_migrations.py",
        )
    )

    checklist = build_rollout_checklist(_brief(), plan)

    assert any("Apply migration or data tasks" in item for item in _items(checklist, "rollout"))
    assert any("`task-migration`" in item for item in _items(checklist, "rollout"))
    assert any("Verify migration rollback" in item for item in _items(checklist, "rollback"))
    assert any(
        "Missing rollback evidence for high-risk or migration work" in item
        for item in _items(checklist, "rollback")
    )


def test_validation_commands_from_tasks_and_plan_metadata_are_deduped():
    plan = _plan()
    plan["metadata"] = {
        "validation_commands": {
            "test": [
                "poetry run pytest tests/test_ui.py",
                "poetry run pytest tests/test_api.py",
            ],
            "lint": ["poetry run ruff check"],
        }
    }
    plan["tasks"][0]["metadata"] = {
        "test_commands": ["poetry run pytest tests/test_api.py"]
    }

    checklist = build_rollout_checklist(_brief(), plan)
    validation = "\n".join(_items(checklist, "validation"))

    assert validation.count("poetry run pytest tests/test_api.py") == 1
    assert validation.count("poetry run pytest tests/test_ui.py") == 1
    assert validation.count("poetry run ruff check") == 1
    assert validation.index("poetry run pytest tests/test_api.py") < validation.index(
        "poetry run pytest tests/test_ui.py"
    )


def test_serialization_is_json_compatible_and_stable():
    checklist = build_rollout_checklist(_brief(), _plan())

    payload = rollout_checklist_to_dict(checklist)

    assert payload == checklist.to_dict()
    assert list(payload) == ["brief_id", "plan_id", "sections"]
    assert list(payload["sections"][0]) == ["name", "items"]
    assert json.loads(json.dumps(payload)) == payload


def _items(checklist, section_name):
    return next(section.items for section in checklist.sections if section.name == section_name)


def _brief():
    return {
        "id": "brief-rollout",
        "title": "Rollout brief",
        "scope": ["Expose release checklist builder"],
        "integration_points": ["GitHub Actions"],
        "risks": [],
        "validation_plan": "Run focused rollout checklist tests",
    }


def _plan():
    return {
        "id": "plan-rollout",
        "implementation_brief_id": "brief-rollout",
        "target_repo": "example/repo",
        "test_strategy": "Run focused pytest",
        "metadata": {},
        "tasks": [
            _task(
                "task-api",
                "Add API support",
                files=["src/blueprint/api.py"],
                acceptance=["API returns checklist data"],
                test_command="poetry run pytest tests/test_api.py",
            ),
            _task(
                "task-ui",
                "Render checklist",
                files=["src/blueprint/ui.py"],
                acceptance=["Checklist sections render in order"],
                test_command="poetry run pytest tests/test_ui.py",
            ),
        ],
    }


def _task(task_id, title, *, files, acceptance, test_command):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "files_or_modules": files,
        "acceptance_criteria": acceptance,
        "risk_level": "low",
        "test_command": test_command,
        "status": "pending",
    }
