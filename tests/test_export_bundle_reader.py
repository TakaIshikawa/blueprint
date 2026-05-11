import base64

import pytest

from blueprint.export.data_exporter import (
    DataExporter,
    DataExportFormat,
    InMemoryDataStore,
    parse_export_bundle,
)


def _exporter() -> DataExporter:
    store = InMemoryDataStore()
    store.plans["plan-1"] = {"id": "plan-1", "title": "Launch"}
    return DataExporter(store=store)


def test_export_bundle_round_trips_result():
    exporter = _exporter()
    result = exporter.export_all_data(fmt=DataExportFormat.JSON)

    bundle = exporter.export_to_bundle(result, metadata={"source": "test"})
    parsed = parse_export_bundle(bundle)

    assert bundle["manifest"] == result.manifest.model_dump(mode="json")
    assert base64.b64decode(bundle["data"]) == result.data
    assert bundle["metadata"]["source"] == "test"
    assert parsed == result


def test_parse_export_bundle_reports_missing_field():
    with pytest.raises(ValueError, match="missing required field: data"):
        parse_export_bundle({"manifest": {}, "metadata": {}})


def test_parse_export_bundle_reports_malformed_base64():
    exporter = _exporter()
    result = exporter.export_all_data(fmt=DataExportFormat.JSON)
    bundle = exporter.export_to_bundle(result)
    bundle["data"] = "not valid base64!"

    with pytest.raises(ValueError, match="not valid base64"):
        parse_export_bundle(bundle)
