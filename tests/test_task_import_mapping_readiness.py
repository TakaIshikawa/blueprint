import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_import_mapping_readiness import (
    analyze_task_import_mapping_readiness,
    build_task_import_mapping_readiness_plan,
    recommend_task_import_mapping_readiness,
    task_import_mapping_readiness_plan_to_dict,
    task_import_mapping_readiness_plan_to_dicts,
    task_import_mapping_readiness_plan_to_markdown,
)


def test_complete_import_mapping_task_is_ready():
    plan = build_task_import_mapping_readiness_plan(
        {
            "id": "plan-import",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "CSV import mapping for account ingestion",
                    "description": "Import job maps CSV source to target fields.",
                    "acceptance_criteria": [
                        "Field mapping table lists every source-to-target column mapping.",
                        "Transform rules normalize phone numbers and trim whitespace.",
                        "Required fields define mandatory columns, defaults, and missing required errors.",
                        "Type coercion covers date format, number format, boolean conversion, and enum conversion.",
                        "Identifier mapping uses external ID lookup key and foreign key matching.",
                        "Validation errors produce row errors, reject file, and failure reason report.",
                        "Sample fixtures include example CSV and golden file sample data.",
                    ],
                    "files_or_modules": ["fixtures/imports/account_mapping.csv"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "field_mapping",
        "transform_rules",
        "required_fields",
        "type_coercion",
        "identifier_mapping",
        "validation_errors",
        "sample_fixtures",
    )


def test_partial_import_mapping_reports_missing_safeguards_and_path_evidence():
    plan = analyze_task_import_mapping_readiness(
        ExecutionPlan.model_validate(
            {
                "id": "plan-partial",
                "implementation_brief_id": "brief-import",
                "milestones": [],
                "tasks": [
                    {
                        "id": "task-partial",
                        "title": "Bulk import mapping",
                        "description": "CSV import has source-to-target field mapping and validation errors.",
                        "acceptance_criteria": [],
                        "files_or_modules": ["src/imports/customer_field_map.py", "tests/fixtures/sample-customers.csv"],
                    },
                    {"id": "task-docs", "title": "Docs", "description": "No import mapping changes are planned.", "acceptance_criteria": []},
                ],
            }
        )
    )

    record = plan.records[0]
    assert plan.ignored_task_ids == ("task-docs",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("field_mapping", "validation_errors")
    assert record.missing_criteria == (
        "transform_rules",
        "required_fields",
        "type_coercion",
        "identifier_mapping",
        "sample_fixtures",
    )
    assert any("customer_field_map.py" in item for item in record.evidence)


def test_serialization_summary_and_malformed_inputs_are_stable():
    result = build_task_import_mapping_readiness_plan("CSV ingestion mapping")
    payload = task_import_mapping_readiness_plan_to_dict(result)

    assert result.summary["impacted_task_count"] == 1
    assert result.summary["missing_criterion_count"] == 7
    assert recommend_task_import_mapping_readiness(result) == result.records
    assert task_import_mapping_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["records"][0]["task_id"] == "task-1"
    assert task_import_mapping_readiness_plan_to_markdown(result).startswith("# Task Import Mapping Readiness")
    assert build_task_import_mapping_readiness_plan(42).records == ()
