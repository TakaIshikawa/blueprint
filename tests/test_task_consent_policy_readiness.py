import copy

from blueprint.domain.models import ExecutionPlan
from blueprint.task_consent_policy_readiness import (
    build_task_consent_policy_readiness_plan,
    task_consent_policy_readiness_plan_to_dict,
    task_consent_policy_readiness_plan_to_dicts,
    task_consent_policy_readiness_plan_to_markdown,
)


def test_complete_consent_policy_task_is_ready():
    result = build_task_consent_policy_readiness_plan(_plan([_task("ready", title="Consent policy enforcement", description="Implement consent policy for processing purposes.", acceptance_criteria=[
        "Consent scope covers processing purposes, purpose limitation, and data categories.",
        "Policy version source is the versioned policy source of truth with consent versioning.",
        "Capture or revocation path supports opt-in, opt-out, and withdraw consent.",
        "Enforcement points gate processing and deny processing without consent.",
        "Audit trail stores consent history and audit logs.",
        "User communication includes policy copy and in-app notice copy.",
        "Validation coverage includes acceptance tests and consent tests.",
    ])]))
    assert result.records[0].readiness == "ready"
    assert result.records[0].missing_criteria == ()


def test_partial_consent_policy_task_reports_ordered_gaps():
    record = build_task_consent_policy_readiness_plan([_task("partial", title="Consent policy opt-out", description="Add opt-out for user consent purposes.")]).records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("consent_scope", "capture_or_revocation_path")
    assert record.missing_criteria == ("policy_version_source", "enforcement_points", "audit_trail", "user_communication", "validation_coverage")
    assert record.recommended_follow_up_actions[0].startswith("Identify the policy version source")


def test_consent_policy_path_hints_nested_metadata_no_mutation_and_conversion():
    source = _plan([_task("paths", title="Lawful basis consent enforcement", description="Add consent policy.", files_or_modules=["privacy/consent_policy/lawful_basis.py", "preferences/opt_out/purpose_limitations.py"], metadata={"audit": "Audit trail keeps consent history."}), _task("noop", title="Privacy docs", description="No consent policy changes are required.")], plan_id="plan-consent")
    original = copy.deepcopy(source)
    result = build_task_consent_policy_readiness_plan(ExecutionPlan.model_validate(source))
    payload = task_consent_policy_readiness_plan_to_dict(result)
    assert source == original
    assert result.impacted_task_ids == ("paths",)
    assert result.ignored_task_ids == ("noop",)
    assert result.records[0].detected_signals == ("consent_policy", "lawful_basis", "preference_choice")
    assert any("metadata.audit" in item for item in result.records[0].evidence)
    assert task_consent_policy_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_consent_policy_readiness_plan_to_markdown(result).startswith("# Task Consent Policy Readiness: plan-consent")


def _plan(tasks, *, plan_id="plan-consent"):
    return {"id": plan_id, "implementation_brief_id": "brief-consent", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, files_or_modules=None, metadata=None):
    task = {"id": task_id, "title": title or task_id, "description": description or "", "acceptance_criteria": acceptance_criteria or []}
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
