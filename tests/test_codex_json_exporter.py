from blueprint.exporters.codex import CodexExporter

from tests.test_task_planning_metadata import _execution_plan, _implementation_brief, _task


def test_codex_exporter_serializes_task_planning_metadata(tmp_path):
    output_path = tmp_path / "codex.md"
    plan = {**_execution_plan(), "tasks": [_task()]}

    CodexExporter().export(plan, _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert "estimated hours: 2.5" in content
    assert "risk: high" in content
    assert "test: `poetry run pytest tests/test_api.py`" in content
