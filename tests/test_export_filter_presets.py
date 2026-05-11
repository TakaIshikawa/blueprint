import pytest

from blueprint.export import ExportFilterPreset
from blueprint.export.data_exporter import DataExporter, DataExportFormat, ExportFilters, InMemoryDataStore


def _exporter() -> DataExporter:
    store = InMemoryDataStore()
    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Launch",
        "status": "active",
        "tags": ["release"],
        "tasks": [{"id": "task-1"}],
    }
    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Backlog",
        "status": "draft",
        "tags": ["internal"],
        "tasks": [{"id": "task-2"}],
    }
    store.tasks["task-1"] = {"id": "task-1", "execution_plan_id": "plan-1"}
    store.tasks["task-2"] = {"id": "task-2", "execution_plan_id": "plan-2"}
    return DataExporter(store=store)


def test_register_and_get_filter_preset():
    exporter = _exporter()
    filters = ExportFilters(status=["active"])

    preset = exporter.register_filter_preset("active", filters, "Active plans")

    assert isinstance(preset, ExportFilterPreset)
    assert exporter.get_filter_preset("active") == preset


def test_register_filter_preset_overwrites_by_name():
    exporter = _exporter()
    exporter.register_filter_preset("plans", ExportFilters(status=["draft"]))

    updated = exporter.register_filter_preset("plans", ExportFilters(tags=["release"]))

    assert exporter.get_filter_preset("plans") == updated
    assert exporter.get_filter_preset("plans").filters.tags == ["release"]  # type: ignore[union-attr]


def test_list_filter_presets_is_stable_and_returns_copies():
    exporter = _exporter()
    exporter.register_filter_preset("zeta", ExportFilters(status=["draft"]))
    exporter.register_filter_preset("alpha", ExportFilters(status=["active"]))

    presets = exporter.list_filter_presets()
    presets[0] = presets[0].model_copy(update={"description": "changed"})

    assert [preset.name for preset in presets] == ["alpha", "zeta"]
    assert exporter.get_filter_preset("alpha").description == ""  # type: ignore[union-attr]


def test_missing_filter_preset_returns_none_and_export_raises():
    exporter = _exporter()

    assert exporter.get_filter_preset("missing") is None
    with pytest.raises(ValueError, match="not registered"):
        exporter.export_filter_preset("missing")


def test_export_filter_preset_delegates_to_filtered_export():
    exporter = _exporter()
    exporter.register_filter_preset("active", ExportFilters(status=["active"]))

    result = exporter.export_filter_preset("active", fmt=DataExportFormat.JSONL)

    lines = result.data.decode("utf-8").splitlines()
    assert result.scope == "filtered"
    assert result.manifest.record_counts == {"plans": 1, "tasks": 1}
    assert len(lines) == 2
