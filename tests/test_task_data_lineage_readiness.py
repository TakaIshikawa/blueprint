import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_lineage_readiness import (
    analyze_task_data_lineage_readiness,
    build_task_data_lineage_readiness_plan,
    extract_task_data_lineage_readiness,
    task_data_lineage_readiness_plan_to_dict,
    task_data_lineage_readiness_plan_to_markdown,
)


def test_complete_data_lineage_task_is_ready():
    result = build_task_data_lineage_readiness_plan(
        _plan(
            [
                _task(
                    "lineage-ready",
                    "Add data lineage for customer pipeline",
                    (
                        "Pipeline transformation provenance links upstream dataset to downstream dataset. "
                        "Lineage metadata captures source and target metadata. Owner mapping assigns dataset owner. "
                        "Validation checks validate lineage. Schema version tracking records schema version. "
                        "Observability adds metrics and alerts. Backfill handling covers historical reload. "
                        "Run ID tracking stores job id and run id for each workflow run."
                    ),
                    ["src/data/lineage/customer_pipeline.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert {"data_lineage", "provenance", "pipeline", "transformation", "dataset_dependency"} <= set(record.detected_signals)
    assert record.missing_safeguards == ()


def test_partial_lineage_task_reports_missing_required_safeguards():
    result = analyze_task_data_lineage_readiness(
        _plan(
            [
                _task(
                    "lineage-partial",
                    "Track lineage for ETL transformations",
                    "Capture upstream dataset and downstream dataset dependency with schema version.",
                    ["src/pipelines/lineage_edges.py"],
                ),
                _task("copy", "Docs", "Update unrelated docs.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("lineage-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert record.present_safeguards == ("schema_version_tracking",)
    assert {"lineage_metadata", "owner_mapping", "validation_checks", "observability"} <= set(record.missing_safeguards)
    assert result.summary["lineage_task_count"] == 1
    assert result.summary["signal_counts"]["dataset_dependency"] == 1
    assert result.summary["missing_safeguard_counts"]["owner_mapping"] == 1


def test_object_dict_execution_task_and_plan_inputs_do_not_mutate_payloads():
    payload = _plan([_task("dict-lineage", "Backfill lineage", "Backfill provenance with run id tracking.", [])])
    original = copy.deepcopy(payload)
    dict_result = extract_task_data_lineage_readiness(payload)
    task = ExecutionTask(
        id="model-lineage",
        title="Dataset dependency lineage",
        description="Data lineage records upstream dataset dependency.",
        files_or_modules=["src/data/lineage.py"],
        acceptance_criteria=["Lineage metadata is captured."],
    )
    plan_result = build_task_data_lineage_readiness_plan(
        ExecutionPlan(id="model-plan", implementation_brief_id="brief", milestones=[], tasks=[task])
    )

    class TaskLike:
        id = "object-lineage"
        title = "Pipeline provenance"
        description = "Pipeline provenance captures job id."
        acceptance_criteria = ["Run id tracking is present."]

    object_result = build_task_data_lineage_readiness_plan(TaskLike())

    assert payload == original
    assert dict_result.records[0].task_id == "dict-lineage"
    assert plan_result.plan_id == "model-plan"
    assert plan_result.records[0].task_id == "model-lineage"
    assert object_result.records[0].task_id == "object-lineage"


def test_serialization_and_markdown_are_stable():
    result = build_task_data_lineage_readiness_plan(
        _plan([_task("lineage-weak", "Add lineage graph", "Track downstream dataset lineage.", [])])
    )
    payload = task_data_lineage_readiness_plan_to_dict(result)

    assert result.records[0].readiness == "missing"
    assert json.loads(json.dumps(payload)) == payload
    assert "| `lineage-weak` | Add lineage graph |" in task_data_lineage_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-lineage", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
