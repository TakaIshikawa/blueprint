from blueprint.audits.change_budget import audit_change_budget


def test_change_budget_accepts_tasks_under_file_and_directory_thresholds():
    result = audit_change_budget(
        _plan(
            [
                _task(
                    "task-api",
                    files_or_modules=[
                        "src/blueprint/audits/change_budget.py",
                        "tests/test_change_budget.py",
                    ],
                )
            ]
        ),
        max_files_per_task=2,
        max_directories_per_task=2,
    )

    assert result.passed is True
    assert result.findings == []


def test_change_budget_flags_tasks_over_file_threshold_with_file_count():
    result = audit_change_budget(
        _plan(
            [
                _task(
                    "task-wide",
                    title="Touch many files",
                    files_or_modules=[
                        "src/blueprint/audits/a.py",
                        "src/blueprint/audits/b.py",
                        "src/blueprint/audits/c.py",
                    ],
                )
            ]
        ),
        max_files_per_task=2,
        max_directories_per_task=1,
    )

    assert result.passed is False
    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.task_id == "task-wide"
    assert finding.title == "Touch many files"
    assert finding.file_count == 3
    assert finding.directory_count == 1
    assert "Split or narrow" in finding.recommendation


def test_change_budget_flags_tasks_over_directory_threshold_with_directory_count():
    result = audit_change_budget(
        _plan(
            [
                _task(
                    "task-spread",
                    files_or_modules=[
                        "src/blueprint/audits/change_budget.py",
                        "src/blueprint/exporters/csv.py",
                        "tests/test_change_budget.py",
                    ],
                )
            ]
        ),
        max_files_per_task=3,
        max_directories_per_task=2,
    )

    finding = result.findings[0]
    assert finding.task_id == "task-spread"
    assert finding.file_count == 3
    assert finding.directory_count == 3
    assert "3 source directories" in finding.recommendation


def test_change_budget_counts_duplicate_file_paths_once():
    result = audit_change_budget(
        _plan(
            [
                _task(
                    "task-duplicates",
                    files_or_modules=[
                        "src/blueprint/audits/change_budget.py",
                        "src/blueprint/audits/./change_budget.py",
                        "tests/test_change_budget.py",
                    ],
                )
            ]
        ),
        max_files_per_task=2,
        max_directories_per_task=1,
    )

    finding = result.findings[0]
    assert finding.file_count == 2
    assert finding.directory_count == 2


def test_change_budget_ignores_empty_or_missing_files_or_modules():
    result = audit_change_budget(
        _plan(
            [
                _task("task-empty", files_or_modules=[]),
                _task("task-blank", files_or_modules=["", "   "]),
                {"id": "task-missing", "title": "Missing files"},
            ]
        ),
        max_files_per_task=0,
        max_directories_per_task=0,
    )

    assert result.passed is True
    assert result.findings == []


def test_change_budget_to_dict_is_deterministic():
    result = audit_change_budget(
        _plan(
            [
                _task(
                    "task-serialize",
                    title="Serialize budget",
                    files_or_modules=[
                        "src/blueprint/audits/change_budget.py",
                        "src/blueprint/exporters/report.py",
                    ],
                )
            ]
        ),
        max_files_per_task=5,
        max_directories_per_task=1,
    )

    assert result.to_dict() == {
        "plan_id": "plan-budget",
        "passed": False,
        "summary": {
            "findings": 1,
            "tasks": 1,
            "max_files_per_task": 5,
            "max_directories_per_task": 1,
        },
        "findings": [
            {
                "task_id": "task-serialize",
                "title": "Serialize budget",
                "file_count": 2,
                "directory_count": 2,
                "recommendation": (
                    "Split or narrow the task scope before execution: it touches "
                    "2 files across 2 source directories (budget: 5 files, "
                    "1 directory)."
                ),
            }
        ],
    }


def _plan(tasks):
    return {
        "id": "plan-budget",
        "title": "Change budget",
        "tasks": tasks,
    }


def _task(task_id, *, title=None, files_or_modules=None):
    return {
        "id": task_id,
        "title": title or task_id.replace("-", " ").title(),
        "description": "Implement scoped work.",
        "files_or_modules": (
            ["src/blueprint/audits/change_budget.py"]
            if files_or_modules is None
            else files_or_modules
        ),
        "acceptance_criteria": ["Tests pass"],
    }
