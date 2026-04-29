from blueprint.audits.file_path_hygiene import audit_file_path_hygiene


def test_file_path_hygiene_accepts_clean_repo_relative_paths():
    result = audit_file_path_hygiene(
        _plan(
            tasks=[
                _task(
                    "task-api",
                    files_or_modules=[
                        "src/blueprint/audits/file_path_hygiene.py",
                        "tests/test_file_path_hygiene.py",
                    ],
                ),
                _task("task-docs", files_or_modules=["docs/release-notes.md"]),
            ]
        )
    )

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 0
    assert result.findings == []


def test_file_path_hygiene_flags_each_finding_code():
    result = audit_file_path_hygiene(
        _plan(
            tasks=[
                _task(
                    "task-paths",
                    files_or_modules=[
                        "/Users/taka/project/src/app.py",
                        "../outside.py",
                        "",
                        "src/app.py",
                        "src/./app.py",
                        "**/*",
                    ],
                )
            ]
        )
    )

    assert {finding.code for finding in result.findings} == {
        "absolute_path",
        "parent_directory_traversal",
        "empty_path_entry",
        "duplicate_path",
        "broad_root_glob",
    }
    assert result.blocking_count == 3
    assert result.warning_count == 2
    assert result.passed is False


def test_file_path_hygiene_flags_windows_absolute_paths_and_root_globs():
    result = audit_file_path_hygiene(
        _plan(
            tasks=[
                _task("task-windows", files_or_modules=["C:\\repo\\src\\app.py"]),
                _task("task-root", files_or_modules=["*", "."]),
            ]
        )
    )

    assert [finding.code for finding in result.findings] == [
        "broad_root_glob",
        "broad_root_glob",
        "absolute_path",
    ]
    assert result.findings[2].normalized_path == "C:/repo/src/app.py"


def test_file_path_hygiene_order_is_stable_by_task_id_and_path():
    result = audit_file_path_hygiene(
        _plan(
            tasks=[
                _task("task-z", files_or_modules=["z/../escape.py", "/z.py"]),
                _task("task-a", files_or_modules=["src/b.py", "src/./b.py", "."]),
            ]
        )
    )

    assert [
        (finding.task_id, finding.normalized_path, finding.code)
        for finding in result.findings
    ] == [
        ("task-a", ".", "broad_root_glob"),
        ("task-a", "src/b.py", "duplicate_path"),
        ("task-z", "/z.py", "absolute_path"),
        ("task-z", "escape.py", "parent_directory_traversal"),
    ]


def test_file_path_hygiene_to_dict_is_deterministic():
    result = audit_file_path_hygiene(
        _plan(
            tasks=[
                _task(
                    "task-serialize",
                    title="Serialize audit",
                    files_or_modules=["src/app.py", "src/./app.py"],
                )
            ]
        )
    )

    assert result.to_dict() == {
        "plan_id": "plan-paths",
        "passed": False,
        "summary": {
            "blocking": 0,
            "warning": 1,
            "findings": 1,
            "tasks": 1,
        },
        "findings": [
            {
                "code": "duplicate_path",
                "severity": "warning",
                "task_id": "task-serialize",
                "task_title": "Serialize audit",
                "path": "src/./app.py",
                "normalized_path": "src/app.py",
                "message": (
                    "Task task-serialize lists the same normalized path more "
                    "than once: src/app.py"
                ),
                "remediation": (
                    "Remove duplicate files_or_modules entries from the task."
                ),
            }
        ],
    }


def _plan(tasks):
    return {
        "id": "plan-paths",
        "title": "Path hygiene",
        "tasks": tasks,
    }


def _task(task_id, *, title=None, files_or_modules=None):
    return {
        "id": task_id,
        "title": title or task_id.replace("-", " ").title(),
        "description": "Implement scoped work.",
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": ["Tests pass"],
    }
