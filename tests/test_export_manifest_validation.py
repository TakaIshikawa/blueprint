from blueprint.export import ExportManifestValidationResult
from blueprint.export.data_exporter import DataExporter, DataExportFormat, InMemoryDataStore


def _exporter() -> DataExporter:
    store = InMemoryDataStore()
    store.plans["plan-1"] = {"id": "plan-1", "title": "Launch"}
    store.tasks["task-1"] = {"id": "task-1", "title": "Ship"}
    return DataExporter(store=store)


def test_valid_export_manifest_passes_validation():
    exporter = _exporter()
    result = exporter.export_all_data(fmt=DataExportFormat.JSON)

    validation = exporter.validate_export_result(result)

    assert isinstance(validation, ExportManifestValidationResult)
    assert validation.is_valid is True
    assert validation.errors == []


def test_checksum_mismatch_is_reported_without_raising():
    exporter = _exporter()
    result = exporter.export_all_data(fmt=DataExportFormat.JSON)
    tampered = result.model_copy(update={"data": result.data + b"\n"})

    validation = exporter.validate_export_result(tampered)

    assert validation.is_valid is False
    assert {error["code"] for error in validation.errors} == {"checksum_mismatch"}


def test_record_count_mismatch_is_reported_independently():
    exporter = _exporter()
    result = exporter.export_all_data(fmt=DataExportFormat.JSON)
    manifest = result.manifest.model_copy(
        update={"record_counts": {**result.manifest.record_counts, "plans": 99}}
    )
    tampered = result.model_copy(update={"manifest": manifest})

    validation = exporter.validate_export_result(tampered)

    assert validation.is_valid is False
    assert {error["code"] for error in validation.errors} == {"record_counts_mismatch"}

