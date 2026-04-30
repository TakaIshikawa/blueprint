import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.risk_mitigation_plan import (
    RiskMitigationRecord,
    build_risk_mitigation_plan,
    risk_mitigation_plan_to_dict,
)


def test_brief_only_risks_receive_deterministic_mitigation_records():
    records = build_risk_mitigation_plan(
        _brief(
            risks=[
                "Security permissions could leak patient notes",
                "Reviewer workflow is ambiguous",
            ]
        )
    )

    assert [record.risk for record in records] == [
        "Security permissions could leak patient notes",
        "Reviewer workflow is ambiguous",
    ]
    assert all(isinstance(record, RiskMitigationRecord) for record in records)
    assert records[0].to_dict() == {
        "risk": "Security permissions could leak patient notes",
        "mitigation": (
            "Require pre-dispatch review and rollback notes; cover permission, credential, "
            "and audit behavior for risk: Security permissions could leak patient notes."
        ),
        "validation_signal": "Run the focused pytest suite.",
        "related_task_ids": [],
        "severity": "high",
        "owner_hint": "security_reviewer",
    }
    assert records[1].severity == "low"
    assert records[1].owner_hint == "technical_lead"


def test_risks_are_mapped_to_related_tasks_by_keyword_overlap():
    records = build_risk_mitigation_plan(
        _brief(risks=["CRM API timeout hides escalation updates"]),
        _plan(
            [
                _task(
                    "task-crm",
                    "Implement CRM escalation API sync",
                    description="Handle external CRM service timeout retries.",
                    files=["src/integrations/crm.py"],
                    acceptance=["CRM escalation updates retry on timeout"],
                    test_command="poetry run pytest tests/test_crm_sync.py",
                    risk_level="medium",
                    owner_type="integration_engineer",
                    metadata={"risk": "CRM timeout"},
                ),
                _task("task-copy", "Update empty-state copy"),
            ]
        ),
    )

    assert len(records) == 1
    assert records[0].related_task_ids == ("task-crm",)
    assert records[0].severity == "medium"
    assert records[0].owner_hint == "integration_engineer"
    assert records[0].validation_signal == (
        "poetry run pytest tests/test_crm_sync.py | "
        "CRM escalation updates retry on timeout | Run the focused pytest suite."
    )


def test_duplicate_and_near_empty_risks_are_normalized_without_losing_evidence():
    records = build_risk_mitigation_plan(
        _brief(
            risks=[
                "  data migration may corrupt audit history. ",
                "Data migration may corrupt audit history",
                "risk",
                "",
                "Docs",
            ]
        ),
        _plan(
            [
                _task(
                    "task-migration",
                    "Add audit history migration",
                    files=["migrations/20260501_audit_history.sql"],
                    acceptance=["Audit history remains intact"],
                    risk_level="high",
                )
            ]
        ),
    )

    assert [record.risk for record in records] == [
        "data migration may corrupt audit history"
    ]
    assert records[0].related_task_ids == ("task-migration",)
    assert records[0].severity == "high"
    assert records[0].owner_hint == "data_reviewer"
    assert "Audit history remains intact" in records[0].validation_signal


def test_validation_signals_fall_back_to_risk_specific_checks():
    brief = _brief(risks=["External webhook contract can drift"], validation_plan="")
    records = build_risk_mitigation_plan(brief)

    assert records[0].validation_signal == "Run integration contract or smoke validation."
    assert records[0].owner_hint == "integration_owner"
    assert records[0].severity == "medium"


def test_model_inputs_and_serialization_are_stable():
    brief_model = ImplementationBrief.model_validate(
        _brief(risks=["Rollout config could disable intake queues"])
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-config",
                    "Update intake queue rollout config",
                    files=["config/intake.yaml"],
                    risk_level="medium",
                )
            ]
        )
    )

    records = build_risk_mitigation_plan(brief_model, plan_model)
    payload = risk_mitigation_plan_to_dict(records)

    assert payload == [record.to_dict() for record in records]
    assert list(payload[0]) == [
        "risk",
        "mitigation",
        "validation_signal",
        "related_task_ids",
        "severity",
        "owner_hint",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _brief(*, risks, validation_plan="Run the focused pytest suite."):
    return {
        "id": "risk-brief",
        "source_brief_id": "source-risk",
        "title": "Risk Brief",
        "problem_statement": "Care teams need better intake review.",
        "mvp_goal": "Ship intake review support.",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks,
        "validation_plan": validation_plan,
        "definition_of_done": [],
    }


def _plan(tasks):
    return {
        "id": "risk-plan",
        "implementation_brief_id": "risk-brief",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    test_command=None,
    risk_level=None,
    owner_type=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if owner_type is not None:
        task["owner_type"] = owner_type
    if metadata is not None:
        task["metadata"] = metadata
    return task
