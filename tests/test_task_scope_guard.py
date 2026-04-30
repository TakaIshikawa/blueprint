from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.task_scope_guard import (
    TaskScopeGuard,
    build_task_scope_guards,
    task_scope_guards_to_dicts,
)


def test_each_task_receives_allowed_paths_from_files_or_modules_in_plan_order():
    guards = build_task_scope_guards(
        _plan(
            tasks=[
                _task(
                    "task-api",
                    "Build API",
                    files_or_modules=[" src/api.py ", "src/api.py", "tests/test_api.py"],
                ),
                _task(
                    "task-ui",
                    "Build UI",
                    files_or_modules=["src\\ui\\panel.tsx"],
                ),
            ]
        )
    )

    assert all(isinstance(guard, TaskScopeGuard) for guard in guards)
    assert [(guard.task_id, guard.title, guard.allowed_paths) for guard in guards] == [
        ("task-api", "Build API", ("src/api.py", "tests/test_api.py")),
        ("task-ui", "Build UI", ("src/ui/panel.tsx",)),
    ]
    assert all(guard.review_only_paths == () for guard in guards)
    assert all(guard.blocked_path_patterns == () for guard in guards)
    assert all(guard.escalation_reasons == () for guard in guards)


def test_missing_file_scope_produces_escalation_reason():
    guards = build_task_scope_guards(
        _plan(
            tasks=[
                _task("task-missing", "Investigate exports", files_or_modules=[]),
                _task("task-none", "Write handoff", files_or_modules=None),
            ]
        )
    )

    assert [guard.allowed_paths for guard in guards] == [(), ()]
    assert [guard.escalation_reasons for guard in guards] == [
        ("missing files_or_modules scope",),
        ("missing files_or_modules scope",),
    ]


def test_non_goals_become_blocked_scope_hints_when_they_mention_files_modules_or_surfaces():
    guards = build_task_scope_guards(
        _plan(tasks=[_task("task-billing", "Build billing export")]),
        _brief(
            non_goals=[
                "Do not edit src/legacy/reporting.py or mobile/onboarding/",
                "Avoid checkout module and admin surface work.",
                "Do not add analytics.",
            ]
        ),
    )

    assert guards[0].blocked_path_patterns == (
        "**/admin/**",
        "**/admin.*",
        "**/checkout/**",
        "**/checkout.py",
        "mobile/onboarding/**",
        "src/legacy/reporting.py",
    )


def test_risk_sensitive_and_shared_paths_are_review_only_with_escalations():
    guards = build_task_scope_guards(
        _plan(
            tasks=[
                _task(
                    "task-risk",
                    "Update auth config",
                    files_or_modules=[
                        "src/auth/tokens.py",
                        "migrations/202602010101_add_user_tokens.py",
                        "config/app.yml",
                        "src/shared/cache.py",
                    ],
                )
            ]
        )
    )

    assert guards[0].review_only_paths == (
        "src/auth/tokens.py",
        "migrations/202602010101_add_user_tokens.py",
        "config/app.yml",
    )
    assert guards[0].escalation_reasons == (
        "review required for risk-sensitive path: src/auth/tokens.py",
        "review required for risk-sensitive path: migrations/202602010101_add_user_tokens.py",
        "review required for risk-sensitive path: config/app.yml",
        "review required for shared package path: src/shared/cache.py",
    )


def test_model_inputs_and_dict_serialization_are_stable():
    guards = build_task_scope_guards(
        ExecutionPlan.model_validate(
            _plan(
                tasks=[
                    _task(
                        "task-config",
                        "Tune package settings",
                        files_or_modules=["pyproject.toml"],
                    )
                ]
            )
        ),
        ImplementationBrief.model_validate(
            _brief(non_goals=["Avoid dashboard surface changes."])
        ),
    )
    payload = task_scope_guards_to_dicts(guards)

    assert payload == [guard.to_dict() for guard in guards]
    assert payload == [
        {
            "task_id": "task-config",
            "title": "Tune package settings",
            "allowed_paths": ["pyproject.toml"],
            "review_only_paths": ["pyproject.toml"],
            "blocked_path_patterns": ["**/dashboard/**", "**/dashboard.*"],
            "escalation_reasons": [
                "review required for risk-sensitive path: pyproject.toml"
            ],
        }
    ]
    assert list(payload[0]) == [
        "task_id",
        "title",
        "allowed_paths",
        "review_only_paths",
        "blocked_path_patterns",
        "escalation_reasons",
    ]


def _plan(*, tasks):
    return {
        "id": "plan-scope-guard",
        "implementation_brief_id": "brief-scope-guard",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _brief(*, non_goals):
    return {
        "id": "brief-scope-guard",
        "source_brief_id": "source-scope-guard",
        "title": "Scope Guard",
        "problem_statement": "Autonomous agents need explicit scope guardrails.",
        "mvp_goal": "Build per-task guardrails.",
        "scope": ["Guard execution tasks"],
        "non_goals": non_goals,
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run focused scope guard tests.",
        "definition_of_done": ["Scope guardrails are serializable"],
    }


_DEFAULT_FILES = object()


def _task(
    task_id,
    title,
    *,
    files_or_modules=_DEFAULT_FILES,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-scope-guard",
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": (
            ["src/app.py"] if files_or_modules is _DEFAULT_FILES else files_or_modules
        ),
        "acceptance_criteria": ["Behavior is covered"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": {},
    }
