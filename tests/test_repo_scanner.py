from pathlib import Path

from blueprint.repo_scanner import format_repository_context, scan_repository


def test_scan_repository_detects_python_node_and_test_commands(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[tool.poetry]\nname = 'demo'\n")
    (repo / "poetry.lock").write_text("")
    (repo / "package.json").write_text('{"scripts": {"test": "vitest run"}}')
    (repo / "pnpm-lock.yaml").write_text("")
    (repo / "README.md").write_text("# Demo\n")
    (repo / "Makefile").write_text("test:\n\tpytest\n")
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_text("print('hello')\n")
    (repo / "src" / "ui.ts").write_text("export {}\n")

    summary = scan_repository(repo)

    assert summary["languages"] == ["Python", "Node/JavaScript", "Make"]
    assert summary["package_managers"] == ["poetry", "pnpm"]
    assert "pyproject.toml" in summary["important_files"]
    assert "package.json" in summary["important_files"]
    assert summary["test_commands"] == ["poetry run pytest", "pnpm test", "make test"]
    assert "src/" in summary["top_level_structure"]


def test_scan_repository_ignores_generated_and_dependency_directories(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "package.json").write_text("{}")
    for ignored in [".git", "node_modules", "venv"]:
        ignored_dir = repo / ignored
        ignored_dir.mkdir()
        (ignored_dir / "package.json").write_text('{"scripts": {"test": "bad"}}')
        (ignored_dir / "generated.py").write_text("bad = True\n")

    summary = scan_repository(repo)
    context = format_repository_context(summary)

    assert summary["ignored_directories"] == [".git", "node_modules", "venv"]
    assert ".git/" not in summary["top_level_structure"]
    assert "node_modules/" not in summary["top_level_structure"]
    assert "venv/" not in summary["top_level_structure"]
    assert "generated.py" not in context


def test_scan_repository_ignores_python_generated_metadata_directories(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text("# Notes\n")

    egg_info = repo / "demo.egg-info"
    eggs = repo / ".eggs"
    pycache = repo / "__pycache__"
    for generated_dir in [egg_info, eggs, pycache]:
        generated_dir.mkdir()
        (generated_dir / "generated.py").write_text("bad = True\n")
        (generated_dir / "package.json").write_text('{"scripts": {"test": "bad"}}')
        (generated_dir / "pytest.ini").write_text("[pytest]\n")

    summary = scan_repository(repo)
    context = format_repository_context(summary)

    assert summary["languages"] == ["Generic"]
    assert summary["important_files"] == ["README.md"]
    assert ".eggs" in summary["ignored_directories"]
    assert "__pycache__" in summary["ignored_directories"]
    assert "demo.egg-info/" not in summary["top_level_structure"]
    assert ".eggs/" not in summary["top_level_structure"]
    assert "__pycache__/" not in summary["top_level_structure"]
    assert "generated.py" not in context
    assert "pytest.ini" not in context
    assert "package.json" not in context
    assert "demo.egg-info" not in context


def test_scan_repository_reports_generic_project_without_language_signals(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text("# Notes\n")

    summary = scan_repository(repo)

    assert summary["languages"] == ["Generic"]
    assert summary["package_managers"] == []
    assert summary["test_commands"] == []


def test_scan_repository_rejects_invalid_paths(tmp_path):
    missing = tmp_path / "missing"
    file_path = tmp_path / "file.txt"
    file_path.write_text("not a directory")

    for invalid_path in [missing, file_path]:
        try:
            scan_repository(invalid_path)
        except ValueError as exc:
            assert "Repository path" in str(exc)
        else:
            raise AssertionError("scan_repository should reject invalid paths")
