from blueprint.task_retry_policy import TaskRetryPolicy, generate_task_retry_policies


def test_generate_task_retry_policies_returns_records_in_task_order():
    policies = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task("task-backend", "Backend API", files=["src/api.py"]),
                _task("task-docs", "Docs", files=["README.md"]),
            ],
        }
    )

    assert [policy.task_id for policy in policies] == ["task-backend", "task-docs"]
    assert all(isinstance(policy, TaskRetryPolicy) for policy in policies)


def test_normal_implementation_task_retries_validation_failures():
    policy = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task(
                    "task-api",
                    "Add backend endpoint",
                    files=["src/blueprint/api.py", "tests/test_api.py"],
                    test_command="poetry run pytest tests/test_api.py",
                )
            ],
        }
    )[0]

    assert policy.max_attempts == 2
    assert "test failure after implementation changes" in policy.retryable_failure_patterns
    assert "validation command output" in policy.required_context_on_retry
    assert not any("destructive" in item for item in policy.escalation_reasons)


def test_test_only_validation_failures_are_retryable_without_strict_stop_conditions():
    policy = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "metadata": {
                "validation_commands": {
                    "test": ["poetry run pytest tests/test_task_retry_policy.py"],
                    "lint": ["poetry run ruff check src tests"],
                }
            },
            "tasks": [
                _task(
                    "task-tests",
                    "Add retry policy tests",
                    files=["tests/test_task_retry_policy.py"],
                    acceptance=["Retry policy tests cover validation failures"],
                )
            ],
        }
    )[0]

    assert policy.max_attempts == 2
    assert "lint, format, typecheck, or build validation failure" in (
        policy.retryable_failure_patterns
    )
    assert "validation command output" in policy.required_context_on_retry
    assert not any("Migration" in item or "Security" in item for item in policy.escalation_reasons)


def test_migration_tasks_have_strict_stop_conditions_and_logs():
    policy = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task(
                    "task-migration",
                    "Add user audit migration",
                    files=["migrations/20260501_add_user_audit.sql"],
                    acceptance=["Migration creates the audit table"],
                    test_command="poetry run pytest tests/test_db_migrations.py",
                )
            ],
        },
        default_max_attempts=3,
    )[0]

    assert policy.max_attempts == 1
    assert any("rerunning migration" in item for item in policy.stop_conditions)
    assert any("Migration or data operation failed" in item for item in policy.escalation_reasons)
    assert "migration, backfill, import, or data verification logs" in (
        policy.required_context_on_retry
    )


def test_security_sensitive_file_paths_have_strict_stop_conditions():
    policy = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task(
                    "task-auth",
                    "Update token verification",
                    files=["src/blueprint/security/token_verifier.py"],
                    acceptance=["Expired tokens are rejected"],
                    test_command="poetry run pytest tests/test_auth.py",
                )
            ],
        },
        default_max_attempts=4,
    )[0]

    assert policy.max_attempts == 1
    assert any("authentication" in item for item in policy.stop_conditions)
    assert any("Security-sensitive" in item for item in policy.escalation_reasons)
    assert "security-sensitive files changed and reviewer concern summary" in (
        policy.required_context_on_retry
    )


def test_destructive_command_failures_are_not_treated_as_retryable_validation_failures():
    policy = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task(
                    "task-cleanup",
                    "Clean generated data",
                    files=["scripts/cleanup.sh"],
                    metadata={"validation_command": "rm -rf tmp/generated"},
                )
            ],
        }
    )[0]

    assert policy.max_attempts == 1
    assert "destructive command failure" not in policy.retryable_failure_patterns
    assert any("destructive command fails" in item for item in policy.stop_conditions)
    assert any("Destructive command failed" in item for item in policy.escalation_reasons)
    assert "destructive command output and rollback or recovery status" in (
        policy.required_context_on_retry
    )


def test_unresolved_dependencies_block_retries_and_escalate():
    policies = generate_task_retry_policies(
        {
            "id": "plan-retry",
            "tasks": [
                _task(
                    "task-schema",
                    "Add schema",
                    files=["src/schema.py"],
                    status="pending",
                ),
                _task(
                    "task-api",
                    "Use schema",
                    files=["src/api.py"],
                    depends_on=["task-schema", "task-missing"],
                    status="pending",
                    test_command="poetry run pytest tests/test_api.py",
                ),
            ],
        }
    )

    policy = policies[1]

    assert policy.max_attempts == 0
    assert policy.retryable_failure_patterns == []
    assert any("task-schema, task-missing" in item for item in policy.stop_conditions)
    assert any("Dependencies are missing or not completed" in item for item in policy.escalation_reasons)


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    depends_on=None,
    test_command=None,
    risk_level=None,
    status=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if depends_on is not None:
        task["depends_on"] = depends_on
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if status is not None:
        task["status"] = status
    if metadata is not None:
        task["metadata"] = metadata
    return task
