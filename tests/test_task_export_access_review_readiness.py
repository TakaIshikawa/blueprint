import json

from blueprint.task_export_access_review_readiness import (
    build_task_export_access_review_readiness_plan,
    derive_task_export_access_review_readiness,
    task_export_access_review_readiness_plan_to_dict,
    task_export_access_review_readiness_plan_to_dicts,
    task_export_access_review_readiness_plan_to_markdown,
)


def test_complete_export_access_review_task_is_ready():
    result = build_task_export_access_review_readiness_plan(
        _plan(
            [
                _task(
                    "export-ready",
                    "Customer CSV export access review",
                    (
                        "Build CSV export with data owner as reviewer owner. Allowed audience is finance managers. "
                        "Field-level access and column permission checks run before export. Sensitive columns are masked. "
                        "Audit evidence writes export log and download log. Link expiry is 7 days with revocation. "
                        "Approval workflow requires manager approval."
                    ),
                    ["src/exports/customer_csv.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "reviewer_ownership",
        "allowed_audience",
        "field_level_access",
        "sensitive_column_handling",
        "audit_evidence",
        "expiry_revocation",
        "approval_workflow",
    )
    assert record.missing_criteria == ()


def test_detects_export_report_download_and_data_share_with_ignored_ids():
    result = build_task_export_access_review_readiness_plan(
        _plan(
            [
                _task("report", "Analytics report", "Create report for customer usage data.", ["reports/usage.py"]),
                _task("download", "Bulk download", "Add downloadable account extract.", ["downloads/account_extract.py"]),
                _task("share", "Dataset share", "External data share for partners.", ["datasets/share.py"]),
                _task("docs", "Docs", "No export or report access review changes are required.", []),
            ]
        )
    )

    assert result.impacted_task_ids == ("download", "report", "share")
    assert result.ignored_task_ids == ("docs",)
    assert result.summary["impacted_task_count"] == 3
    assert result.summary["readiness_counts"]["needs_planning"] == 3
    assert result.summary["missing_criterion_counts"]["allowed_audience"] == 3


def test_aliases_serialization_and_markdown_are_stable():
    plan = _plan([_task("alias", "Export", "Export with allowed audience and audit evidence.", ["exports/a.py"])])
    result = build_task_export_access_review_readiness_plan(plan)
    alias = derive_task_export_access_review_readiness(plan)
    payload = task_export_access_review_readiness_plan_to_dict(result)

    assert alias.to_dict() == result.to_dict()
    assert task_export_access_review_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Export Access Review Readiness: plan-export-access" in task_export_access_review_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-export-access", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
