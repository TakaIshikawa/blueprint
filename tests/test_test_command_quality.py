from blueprint.audits.test_command_quality import audit_test_command_quality


def test_test_command_quality_accepts_focused_pytest_command():
    plan = _plan(
        [
            _task(
                files_or_modules=["tests/test_test_command_quality.py"],
                test_command=(
                    "poetry run pytest tests/test_test_command_quality.py -o addopts=''"
                ),
            )
        ]
    )

    result = audit_test_command_quality(plan)

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 0
    assert result.findings == []
    assert result.to_dict()["summary"] == {
        "blocking": 0,
        "warning": 0,
        "tasks": 1,
    }


def test_test_command_quality_flags_missing_command_as_blocking():
    result = audit_test_command_quality(_plan([_task(test_command=None)]))

    assert result.passed is False
    assert result.blocking_count == 1
    assert result.warning_count == 0
    assert _finding_codes(result) == ["missing_test_command"]
    assert result.findings[0].severity == "blocking"
    assert result.findings[0].field == "test_command"


def test_test_command_quality_flags_destructive_command_as_blocking():
    plan = _plan(
        [
            _task(test_command="rm -rf .pytest_cache && poetry run pytest"),
            _task(task_id="task-reset", test_command="git reset --hard && pytest"),
            _task(task_id="task-prune", test_command="docker system prune -f && pytest"),
        ]
    )

    result = audit_test_command_quality(plan)

    assert result.passed is False
    assert result.blocking_count == 3
    assert result.warning_count == 0
    assert _finding_codes(result) == [
        "destructive_test_command",
        "destructive_test_command",
        "destructive_test_command",
    ]


def test_test_command_quality_flags_non_validation_command_as_blocking():
    result = audit_test_command_quality(
        _plan([_task(test_command="python scripts/generate_report.py")])
    )

    assert result.passed is False
    assert result.blocking_count == 1
    assert result.warning_count == 0
    assert _finding_codes(result) == ["non_validation_test_command"]


def test_test_command_quality_warns_on_repo_wide_command_for_single_test_file():
    result = audit_test_command_quality(
        _plan(
            [
                _task(
                    files_or_modules=["tests/test_test_command_quality.py"],
                    risk_level="low",
                    test_command="poetry run pytest",
                )
            ]
        )
    )

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 1
    assert _finding_codes(result) == ["overbroad_test_command"]
    assert result.findings_by_severity()["blocking"] == []
    assert result.findings_by_severity()["warning"] == result.findings


def _finding_codes(result):
    return [finding.code for finding in result.findings]


def _plan(tasks):
    return {
        "id": "plan-test-command-quality",
        "tasks": tasks,
    }


def _task(
    *,
    task_id="task-tests",
    title="Update focused tests",
    files_or_modules=None,
    risk_level="low",
    test_command="poetry run pytest tests/test_example.py -o addopts=''",
):
    return {
        "id": task_id,
        "title": title,
        "description": "Update implementation and tests.",
        "files_or_modules": files_or_modules or ["tests/test_example.py"],
        "risk_level": risk_level,
        "test_command": test_command,
    }
