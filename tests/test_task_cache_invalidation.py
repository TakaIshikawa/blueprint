import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_cache_invalidation import (
    TaskCacheInvalidationGuidance,
    TaskCacheInvalidationPlan,
    build_task_cache_invalidation_plan,
    derive_task_cache_invalidation_plan,
    task_cache_invalidation_to_dict,
    task_cache_invalidation_to_markdown,
)


def test_explicit_cache_layers_are_classified_with_actionable_strategy():
    result = build_task_cache_invalidation_plan(
        _plan(
            [
                _task(
                    "task-cache",
                    title="Invalidate Redis and memcached response cache",
                    description=(
                        "Purge CloudFront CDN responses, Redis keys, memcached keys, "
                        "and ORM query cache entries for the changed API endpoint."
                    ),
                    files_or_modules=["src/api/orders.py", "src/cache/redis_keys.py"],
                    risk_level="high",
                    test_command="poetry run pytest tests/test_orders.py",
                )
            ]
        )
    )

    guidance = result.tasks[0]
    assert guidance.cache_layers == (
        "cdn",
        "redis",
        "memcached",
        "orm_query_cache",
        "api_response_cache",
    )
    assert guidance.stale_data_risk == "high"
    assert guidance.risk_reasons == (
        "user-visible stale assets or edge responses",
        "cached API responses can outlive contract or payload changes",
        "shared in-memory cache may serve stale computed data",
        "database-backed reads may retain stale query results",
        "high task risk",
    )
    assert "purge affected CDN keys" in guidance.invalidation_strategy
    assert "delete or rotate affected Redis keys" in guidance.invalidation_strategy
    assert "poetry run pytest tests/test_orders.py" in guidance.validation_hints
    assert "Prepare an emergency purge command before rollout." in guidance.rollback_considerations


def test_api_assets_flags_and_database_backed_reads_get_stale_data_guidance():
    result = build_task_cache_invalidation_plan(
        _plan(
            [
                _task(
                    "task-assets",
                    title="Update checkout assets",
                    files_or_modules=["public/assets/checkout.css", "src/build/manifest.json"],
                    acceptance_criteria=["New CSS and JS bundle are served after deploy"],
                ),
                _task(
                    "task-flags",
                    title="Add LaunchDarkly feature flag rollout",
                    metadata={"flag": "checkout_v2"},
                ),
                _task(
                    "task-db-api",
                    title="Change account API database-backed reads",
                    description="Update REST endpoint read model and query filters.",
                    files_or_modules=["src/api/accounts.py", "src/repositories/accounts.py"],
                ),
            ]
        )
    )

    by_id = {task.task_id: task for task in result.tasks}
    assert by_id["task-assets"].cache_layers == (
        "cdn",
        "browser_cache",
        "build_artifact_cache",
    )
    assert by_id["task-assets"].stale_data_risk == "medium"
    assert by_id["task-flags"].cache_layers == ("feature_flag_cache",)
    assert by_id["task-flags"].validation_hints == (
        "Confirm flag value refresh across rollout and rollback cohorts.",
    )
    assert by_id["task-db-api"].cache_layers == ("orm_query_cache", "api_response_cache")
    assert by_id["task-db-api"].stale_data_risk == "medium"
    assert "Run stale-read regression checks" in "; ".join(by_id["task-db-api"].validation_hints)


def test_non_cache_tasks_do_not_generate_noisy_high_risk_recommendations():
    result = build_task_cache_invalidation_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update onboarding copy",
                    description="Adjust Markdown documentation and CLI help text.",
                    files_or_modules=["docs/onboarding.md", "src/cli/help.py"],
                    acceptance_criteria=["Help text is accurate"],
                )
            ]
        )
    )

    guidance = result.tasks[0]
    assert guidance.cache_layers == ()
    assert guidance.stale_data_risk == "none"
    assert guidance.risk_reasons == ()
    assert guidance.invalidation_strategy == (
        "No cache invalidation required; keep normal validation evidence."
    )
    assert guidance.rollback_considerations == ("No cache-specific rollback step expected.",)
    assert guidance.evidence_hints == (
        "Normal task validation is sufficient; no cache evidence needed.",
    )


def test_serialization_markdown_aliases_and_model_input_are_stable_without_mutation():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Update API response cache for account route",
                files_or_modules=["src/routes/account.py"],
                test_command="make smoke-account",
            )
        ],
        plan_id="plan-cache",
    )
    original = copy.deepcopy(plan)

    result = build_task_cache_invalidation_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_cache_invalidation_plan(plan)
    payload = task_cache_invalidation_to_dict(result)

    assert plan == original
    assert isinstance(result, TaskCacheInvalidationPlan)
    assert isinstance(result.tasks[0], TaskCacheInvalidationGuidance)
    assert payload == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert alias_result.to_dict() == result.to_dict()
    assert list(payload) == ["plan_id", "tasks"]
    assert list(payload["tasks"][0]) == [
        "task_id",
        "title",
        "cache_layers",
        "invalidation_strategy",
        "stale_data_risk",
        "risk_reasons",
        "rollback_considerations",
        "validation_hints",
        "evidence_hints",
    ]
    assert task_cache_invalidation_to_markdown(result) == "\n".join(
        [
            "# Task Cache Invalidation: plan-cache",
            "",
            "## Guidance",
            "",
            "| Task | Layers | Risk | Strategy | Validation |",
            "| --- | --- | --- | --- | --- |",
            (
                "| task-api | api_response_cache | medium | "
                "expire API response keys for changed routes, payloads, and vary headers. | "
                "make smoke-account; Compare cached and uncached API responses for changed routes. |"
            ),
        ]
    )


def test_empty_and_iterable_inputs_are_supported():
    empty = build_task_cache_invalidation_plan({"id": "plan-empty", "tasks": []})
    iterable = build_task_cache_invalidation_plan(
        [
            _task("task-flag", title="Refresh feature flag cache"),
            _task("task-static", title="Publish static assets"),
        ]
    )

    assert empty.to_markdown() == "\n".join(
        [
            "# Task Cache Invalidation: plan-empty",
            "",
            "No tasks were found.",
        ]
    )
    assert iterable.plan_id is None
    assert [task.task_id for task in iterable.tasks] == ["task-flag", "task-static"]
    assert iterable.tasks[0].cache_layers == ("feature_flag_cache",)


def _plan(tasks, *, plan_id="plan-cache"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cache",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone="Foundation",
    files_or_modules=None,
    acceptance_criteria=None,
    risk_level="medium",
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
