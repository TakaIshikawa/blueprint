from blueprint.audits.file_contention import audit_file_contention


def test_file_contention_groups_exact_overlapping_paths_with_task_context():
    result = audit_file_contention(
        _plan(
            [
                _task(
                    "task-api",
                    title="Build API",
                    milestone="Foundation",
                    status="pending",
                    files_or_modules=["src/app.py"],
                ),
                _task(
                    "task-tests",
                    title="Cover API",
                    milestone="Validation",
                    status="in_progress",
                    files_or_modules=["src/app.py"],
                ),
            ]
        ),
        exact_path_matches_only=True,
    )

    assert result.passed is False
    assert result.finding_count == 1
    finding = result.findings[0]
    assert finding.path == "src/app.py"
    assert finding.severity == "low"
    assert finding.task_ids == ["task-api", "task-tests"]
    assert finding.task_titles == ["Build API", "Cover API"]
    assert finding.milestones == ["Foundation", "Validation"]
    assert finding.statuses == ["in_progress", "pending"]
    assert [task.matched_paths for task in finding.tasks] == [
        ["src/app.py"],
        ["src/app.py"],
    ]


def test_file_contention_normalizes_relative_paths_before_grouping():
    result = audit_file_contention(
        _plan(
            [
                _task(
                    "task-one",
                    files_or_modules=["./src/blueprint/./cli.py"],
                ),
                _task(
                    "task-two",
                    files_or_modules=["src/blueprint/cli.py"],
                ),
            ]
        )
    )

    assert result.finding_count == 1
    assert result.findings[0].path == "src/blueprint/cli.py"
    assert result.findings[0].task_ids == ["task-one", "task-two"]


def test_file_contention_excludes_completed_and_skipped_tasks_by_default():
    result = audit_file_contention(
        _plan(
            [
                _task("task-active", status="pending", files_or_modules=["src/app.py"]),
                _task(
                    "task-done",
                    status="completed",
                    files_or_modules=["src/app.py"],
                ),
                _task(
                    "task-skipped",
                    status="skipped",
                    files_or_modules=["src/app.py"],
                ),
            ]
        )
    )

    assert result.analyzed_task_count == 1
    assert result.passed is True

    included = audit_file_contention(
        _plan(
            [
                _task("task-active", status="pending", files_or_modules=["src/app.py"]),
                _task(
                    "task-done",
                    status="completed",
                    files_or_modules=["src/app.py"],
                ),
                _task(
                    "task-skipped",
                    status="skipped",
                    files_or_modules=["src/app.py"],
                ),
            ]
        ),
        ignore_completed_skipped_tasks=False,
    )

    assert included.analyzed_task_count == 3
    assert included.findings[0].task_ids == [
        "task-active",
        "task-done",
        "task-skipped",
    ]


def test_file_contention_severity_increases_for_many_or_high_risk_tasks():
    high_risk_result = audit_file_contention(
        _plan(
            [
                _task(
                    "task-api",
                    files_or_modules=["src/app.py"],
                    risk_level="high",
                ),
                _task(
                    "task-tests",
                    files_or_modules=["src/app.py"],
                    risk_level="low",
                ),
            ]
        )
    )

    many_medium_result = audit_file_contention(
        _plan(
            [
                _task("task-a", files_or_modules=["src/app.py"], risk_level="medium"),
                _task("task-b", files_or_modules=["src/app.py"], risk_level="low"),
                _task("task-c", files_or_modules=["src/app.py"], risk_level="low"),
            ]
        )
    )

    assert high_risk_result.findings[0].severity == "high"
    assert many_medium_result.findings[0].severity == "high"
    assert high_risk_result.high_count == 1


def test_file_contention_respects_exact_path_matches_only_option():
    plan = _plan(
        [
            _task("task-package", files_or_modules=["src/blueprint"]),
            _task("task-file", files_or_modules=["src/blueprint/cli.py"]),
        ]
    )

    overlapping = audit_file_contention(plan)
    exact_only = audit_file_contention(plan, exact_path_matches_only=True)

    assert overlapping.finding_count == 1
    assert overlapping.findings[0].path == "src/blueprint"
    assert overlapping.findings[0].task_ids == ["task-file", "task-package"]
    assert exact_only.passed is True


def test_file_contention_can_ignore_empty_or_directory_like_placeholders():
    result = audit_file_contention(
        _plan(
            [
                _task("task-a", files_or_modules=["", ".", "src/blueprint/"]),
                _task("task-b", files_or_modules=["src/blueprint/"]),
            ]
        )
    )

    assert result.passed is True
    assert result.findings == []


def test_file_contention_accepts_no_overlap_plans():
    result = audit_file_contention(
        _plan(
            [
                _task("task-api", files_or_modules=["src/app.py"]),
                _task("task-tests", files_or_modules=["tests/test_app.py"]),
                _task("task-docs", files_or_modules=["docs/app.md"]),
            ]
        )
    )

    assert result.to_dict() == {
        "plan_id": "plan-contention",
        "passed": True,
        "summary": {
            "findings": 0,
            "high": 0,
            "medium": 0,
            "low": 0,
            "tasks": 3,
            "analyzed_tasks": 3,
        },
        "findings": [],
    }


def _plan(tasks):
    return {
        "id": "plan-contention",
        "title": "File contention",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    milestone="Foundation",
    status="pending",
    files_or_modules=None,
    risk_level="low",
):
    return {
        "id": task_id,
        "title": title or task_id.replace("-", " ").title(),
        "description": "Implement scoped work.",
        "milestone": milestone,
        "status": status,
        "risk_level": risk_level,
        "files_or_modules": (
            ["src/blueprint/audits/file_contention.py"]
            if files_or_modules is None
            else files_or_modules
        ),
        "acceptance_criteria": ["Tests pass"],
    }
