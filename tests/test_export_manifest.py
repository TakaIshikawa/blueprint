import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.manifest import ExportManifestExporter
from blueprint.store import Store, init_db


def test_export_manifest_includes_existing_and_missing_files(tmp_path):
    store = _setup_store(tmp_path)
    file_path = tmp_path / "exports" / "plan-test-codex.md"
    file_path.parent.mkdir()
    file_path.write_bytes(b"export content\n")
    missing_path = tmp_path / "exports" / "missing.md"

    _insert_export_record(store, "exp-codex", "codex", "markdown", file_path)
    _insert_export_record(store, "exp-missing", "release-notes", "markdown", missing_path)

    manifest = ExportManifestExporter().build(
        store,
        "plan-test",
        generated_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )

    assert manifest["plan_id"] == "plan-test"
    assert manifest["generated_at"] == "2026-01-02T03:04:05Z"

    entries = {entry["export_record_id"]: entry for entry in manifest["exports"]}
    assert entries["exp-codex"] == {
        "checksum": hashlib.sha256(b"export content\n").hexdigest(),
        "exists": True,
        "export_record_id": "exp-codex",
        "exported_at": "2026-01-01T00:00:00",
        "format": "markdown",
        "path": str(file_path.resolve()),
        "size_bytes": len(b"export content\n"),
        "target_engine": "codex",
    }
    assert entries["exp-missing"] == {
        "checksum": None,
        "exists": False,
        "export_record_id": "exp-missing",
        "exported_at": "2026-01-01T00:00:00",
        "format": "markdown",
        "path": str(missing_path.resolve()),
        "size_bytes": None,
        "target_engine": "release-notes",
    }


def test_export_manifest_includes_all_export_records(tmp_path):
    store = _setup_store(tmp_path)
    export_dir = tmp_path / "exports"
    export_dir.mkdir()

    for index in range(51):
        path = export_dir / f"artifact-{index}.json"
        path.write_text(f"{index}\n")
        _insert_export_record(store, f"exp-{index:02d}", "codex", "json", path)

    manifest = ExportManifestExporter().build(store, "plan-test")

    assert len(manifest["exports"]) == 51


def test_export_manifest_cli_writes_sorted_json_output(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    file_path = tmp_path / "exports" / "plan-test.csv"
    file_path.parent.mkdir()
    file_path.write_text("task_id\n")
    output_path = tmp_path / "manifest.json"
    _insert_export_record(store, "exp-csv", "csv-tasks", "csv", file_path)

    result = CliRunner().invoke(
        cli,
        ["export-manifest", "plan-test", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""

    manifest_text = output_path.read_text()
    assert manifest_text.startswith('{\n  "exports": [\n')
    assert '"generated_at":' in manifest_text
    assert manifest_text.endswith("}\n")

    payload = json.loads(manifest_text)
    assert payload["plan_id"] == "plan-test"
    assert payload["exports"][0]["checksum"] == hashlib.sha256(b"task_id\n").hexdigest()


def test_export_manifest_cli_prints_json_to_stdout(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    file_path = tmp_path / "artifact.xml"
    file_path.write_text("<testsuite />\n")
    _insert_export_record(store, "exp-junit", "junit-tasks", "xml", file_path)

    result = CliRunner().invoke(cli, ["export-manifest", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["exports"][0]["target_engine"] == "junit-tasks"
    assert payload["exports"][0]["size_bytes"] == len("<testsuite />\n")


def _setup_store(tmp_path, monkeypatch=None):
    if monkeypatch is not None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".blueprint.yaml").write_text(
            f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path / "exports"}
"""
        )
        blueprint_config.reload_config()

    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [])
    return store


def _insert_export_record(
    store: Store,
    record_id: str,
    target: str,
    export_format: str,
    path: Path,
):
    store.insert_export_record(
        {
            "id": record_id,
            "execution_plan_id": "plan-test",
            "target_engine": target,
            "export_format": export_format,
            "output_path": str(path),
            "exported_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
    )


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "src-test",
        "title": "Test Brief",
        "problem_statement": "Need reliable exports",
        "mvp_goal": "Generate a manifest",
        "scope": ["Manifest"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run tests",
        "definition_of_done": ["Manifest exists"],
        "status": "draft",
    }
