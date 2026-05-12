import json

from blueprint.task_access_review_readiness import (
    build_task_access_review_readiness_plan,
    derive_task_access_review_readiness,
    task_access_review_readiness_plan_to_dict,
    task_access_review_readiness_plan_to_dicts,
    task_access_review_readiness_plan_to_markdown,
)


def test_complete_access_review_task_is_ready():
    result = build_task_access_review_readiness_plan(
        _plan(
            [
                _task(
                    "access-ready",
                    "Quarterly privileged access review",
                    (
                        "Run privileged access review and reviewer attestation. App owner is reviewer owner. "
                        "Population scope covers all admins and entitlement groups. Evidence source is IAM export. "
                        "Revocation workflow opens removal tickets. Review cadence is quarterly."
                    ),
                    ["src/access_reviews/privileged_access.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "reviewer_ownership",
        "population_scope",
        "evidence_source",
        "revocation_workflow",
        "review_cadence",
    )
    assert record.missing_criteria == ()


def test_detects_review_recertification_and_stale_access_with_no_impact_suppression():
    result = build_task_access_review_readiness_plan(
        _plan(
            [
                _task("entitlements", "Entitlement review", "Review role entitlements for support users.", ["src/iam/entitlements.py"]),
                _task("stale", "Stale access cleanup", "Find stale access and dormant accounts.", ["src/iam/stale_access.py"]),
                _task("docs", "Docs", "No access review changes are required.", []),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("entitlements", "stale")
    assert result.ignored_task_ids == ("docs",)
    assert "entitlement_review" in by_id["entitlements"].detected_signals
    assert "stale_access" in by_id["stale"].detected_signals
    assert result.summary["readiness_counts"]["needs_planning"] == 2


def test_aliases_serialization_and_markdown_are_stable():
    plan = _plan([_task("alias", "Access review", "Access review with quarterly cadence.", ["access/reviews.py"])])
    result = build_task_access_review_readiness_plan(plan)
    alias = derive_task_access_review_readiness(plan)
    payload = task_access_review_readiness_plan_to_dict(result)

    assert alias.to_dict() == result.to_dict()
    assert task_access_review_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Access Review Readiness: plan-access-review" in task_access_review_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-access-review", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
