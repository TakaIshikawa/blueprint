from blueprint.task_support_escalation_readiness import (
    analyze_task_support_escalation_readiness,
    task_support_escalation_readiness_plan_to_dict,
    task_support_escalation_readiness_plan_to_markdown,
)


def test_ready_support_escalation_task_covers_expected_criteria():
    plan = analyze_task_support_escalation_readiness(
        [{
            "id": "task-ready",
            "title": "Implement support escalation routing for SLA breach",
            "description": (
                "Escalation trigger is SLA breach or severity change. Target tier is tier 2 owner and on-call DRI. "
                "Routing rules use queue mapping and skill based assignment. Customer context includes account plan, "
                "entitlement, history, and case summary. SLA timing tracks response time and business hours deadline. "
                "Audit trail records actor timestamp reason code and handoff record. Notification template sends agent "
                "notification and customer notice. Integration tests cover routing and SLA edge cases."
            ),
            "files_or_modules": ["src/support/escalation_routing.py"],
        }]
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert "escalation" in record.detected_signals


def test_partial_support_escalation_task_returns_followups():
    plan = analyze_task_support_escalation_readiness(
        [{"id": "task-partial", "title": "Support escalation on SLA breach", "description": "Escalation trigger is priority change and target tier is tier 2."}]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("escalation_trigger", "target_tier_owner", "sla_timing")
    assert "Document routing" in record.recommended_follow_up_actions[0]


def test_sparse_support_escalation_task_needs_planning():
    plan = analyze_task_support_escalation_readiness("Support escalation workflow")

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_customer_support_without_escalation_or_routing_is_ignored():
    plan = analyze_task_support_escalation_readiness(
        [{"id": "task-support", "title": "Customer support dashboard", "description": "Show tickets, customers, and case counts."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-support",)


def test_support_escalation_serialization_and_markdown_are_deterministic():
    plan = analyze_task_support_escalation_readiness(
        [{"id": "task-esc", "title": "Support escalation", "description": "Escalation trigger is SLA breach."}]
    )

    payload = task_support_escalation_readiness_plan_to_dict(plan)
    assert payload["summary"]["missing_criterion_count"] == 6
    assert task_support_escalation_readiness_plan_to_markdown(plan) == plan.to_markdown()
