from blueprint.audits.acceptance_traceability import audit_acceptance_traceability


def test_acceptance_traceability_accepts_covered_commitments():
    result = audit_acceptance_traceability(
        _brief(),
        _plan(
            tasks=[
                _task(
                    "task-report",
                    [
                        "Verify project status report shows open blockers and review owners",
                        "Assert CSV export includes status report rows for reviewers",
                    ],
                ),
                _task(
                    "task-alerts",
                    [
                        "Verify blocker alerts are displayed before release",
                    ],
                ),
            ]
        ),
    )

    assert result.passed is True
    assert result.findings == []
    assert [item.to_dict() for item in result.coverage] == [
        {
            "brief_item_code": "goal:mvp_goal",
            "brief_item_type": "goal",
            "brief_item_text": "Show project status reports with open blockers",
            "covered": True,
            "task_ids": ["task-report"],
        },
        {
            "brief_item_code": "scope:001",
            "brief_item_type": "scope",
            "brief_item_text": "Render status report rows for reviewers",
            "covered": True,
            "task_ids": ["task-report"],
        },
        {
            "brief_item_code": "scope:002",
            "brief_item_type": "scope",
            "brief_item_text": "Surface blocker alerts before release",
            "covered": True,
            "task_ids": ["task-alerts"],
        },
        {
            "brief_item_code": "requirement:001",
            "brief_item_type": "requirement",
            "brief_item_text": "CSV export includes review owner status",
            "covered": True,
            "task_ids": ["task-report"],
        },
    ]
    assert result.to_dict()["summary"] == {
        "high": 0,
        "medium": 0,
        "brief_items": 4,
        "covered_brief_items": 4,
    }


def test_acceptance_traceability_reports_uncovered_brief_items():
    result = audit_acceptance_traceability(
        _brief(),
        _plan(
            tasks=[
                _task(
                    "task-report",
                    ["Verify project status report shows open blockers"],
                )
            ]
        ),
    )

    assert result.passed is False
    assert [finding.to_dict() for finding in result.findings] == [
        {
            "code": "uncovered_scope",
            "severity": "high",
            "message": "No task acceptance criteria appear to cover this brief commitment.",
            "task_id": None,
            "criterion_text": None,
            "brief_item_code": "scope:001",
            "brief_item_type": "scope",
            "brief_item_text": "Render status report rows for reviewers",
        },
        {
            "code": "uncovered_scope",
            "severity": "high",
            "message": "No task acceptance criteria appear to cover this brief commitment.",
            "task_id": None,
            "criterion_text": None,
            "brief_item_code": "scope:002",
            "brief_item_type": "scope",
            "brief_item_text": "Surface blocker alerts before release",
        },
        {
            "code": "uncovered_requirement",
            "severity": "high",
            "message": "No task acceptance criteria appear to cover this brief commitment.",
            "task_id": None,
            "criterion_text": None,
            "brief_item_code": "requirement:001",
            "brief_item_type": "requirement",
            "brief_item_text": "CSV export includes review owner status",
        },
    ]


def test_acceptance_traceability_flags_missing_and_generic_acceptance_criteria():
    result = audit_acceptance_traceability(
        _brief(),
        _plan(
            tasks=[
                _task("task-missing", []),
                _task("task-generic", ["Works", "Implemented as expected"]),
            ]
        ),
    )

    assert [finding.code for finding in result.findings[:3]] == [
        "missing_acceptance_criteria",
        "generic_acceptance_criterion",
        "generic_acceptance_criterion",
    ]
    assert result.findings[0].severity == "high"
    assert result.findings[0].task_id == "task-missing"
    assert result.findings[1].severity == "medium"
    assert result.findings[1].criterion_text == "Works"
    assert result.high_count == 5
    assert result.medium_count == 2


def test_acceptance_traceability_handles_empty_plans_deterministically():
    result = audit_acceptance_traceability(_brief(), _plan(tasks=[]))

    assert result.to_dict() == {
        "brief_id": "brief-trace",
        "plan_id": "plan-trace",
        "passed": False,
        "summary": {
            "high": 5,
            "medium": 0,
            "brief_items": 4,
            "covered_brief_items": 0,
        },
        "findings": [
            {
                "code": "empty_plan",
                "severity": "high",
                "message": "Plan has no tasks with acceptance criteria to trace.",
                "task_id": None,
                "criterion_text": None,
                "brief_item_code": None,
                "brief_item_type": None,
                "brief_item_text": None,
            },
            {
                "code": "uncovered_goal",
                "severity": "high",
                "message": "No task acceptance criteria appear to cover this brief commitment.",
                "task_id": None,
                "criterion_text": None,
                "brief_item_code": "goal:mvp_goal",
                "brief_item_type": "goal",
                "brief_item_text": "Show project status reports with open blockers",
            },
            {
                "code": "uncovered_scope",
                "severity": "high",
                "message": "No task acceptance criteria appear to cover this brief commitment.",
                "task_id": None,
                "criterion_text": None,
                "brief_item_code": "scope:001",
                "brief_item_type": "scope",
                "brief_item_text": "Render status report rows for reviewers",
            },
            {
                "code": "uncovered_scope",
                "severity": "high",
                "message": "No task acceptance criteria appear to cover this brief commitment.",
                "task_id": None,
                "criterion_text": None,
                "brief_item_code": "scope:002",
                "brief_item_type": "scope",
                "brief_item_text": "Surface blocker alerts before release",
            },
            {
                "code": "uncovered_requirement",
                "severity": "high",
                "message": "No task acceptance criteria appear to cover this brief commitment.",
                "task_id": None,
                "criterion_text": None,
                "brief_item_code": "requirement:001",
                "brief_item_type": "requirement",
                "brief_item_text": "CSV export includes review owner status",
            },
        ],
        "coverage": [
            {
                "brief_item_code": "goal:mvp_goal",
                "brief_item_type": "goal",
                "brief_item_text": "Show project status reports with open blockers",
                "covered": False,
                "task_ids": [],
            },
            {
                "brief_item_code": "scope:001",
                "brief_item_type": "scope",
                "brief_item_text": "Render status report rows for reviewers",
                "covered": False,
                "task_ids": [],
            },
            {
                "brief_item_code": "scope:002",
                "brief_item_type": "scope",
                "brief_item_text": "Surface blocker alerts before release",
                "covered": False,
                "task_ids": [],
            },
            {
                "brief_item_code": "requirement:001",
                "brief_item_type": "requirement",
                "brief_item_text": "CSV export includes review owner status",
                "covered": False,
                "task_ids": [],
            },
        ],
    }


def _brief() -> dict:
    return {
        "id": "brief-trace",
        "mvp_goal": "Show project status reports with open blockers",
        "scope": [
            "Render status report rows for reviewers",
            "Surface blocker alerts before release",
        ],
        "requirements": ["CSV export includes review owner status"],
    }


def _plan(*, tasks: list[dict]) -> dict:
    return {
        "id": "plan-trace",
        "tasks": tasks,
    }


def _task(task_id: str, acceptance_criteria: list[str]) -> dict:
    return {
        "id": task_id,
        "title": task_id,
        "description": task_id,
        "acceptance_criteria": acceptance_criteria,
    }
