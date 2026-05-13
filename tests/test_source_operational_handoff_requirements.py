from blueprint.source_operational_handoff_requirements import build_source_operational_handoff_requirements


def test_operational_handoff_brief_extracts_ordered_records():
    report = build_source_operational_handoff_requirements(
        _source(
            [
                "Support handoff must name customer support owner, Slack channel, and before launch timing.",
                "On-call ownership requires primary on-call owner, PagerDuty schedule, and backup.",
                "Runbook location must link Confluence runbook with owner and quarterly update cadence.",
                "Escalation path must define tier levels, pager contacts, and severity rules.",
                "Launch checklist must include smoke test, rollback verification, approver, and ticket evidence.",
                "Training materials must include support audience, KB article, deck, and training date.",
                "Post-launch review must be scheduled after launch with support, SRE, product, and ticket metrics.",
            ]
        )
    )

    assert [record.requirement_type for record in report.records] == [
        "support_handoff",
        "on_call_ownership",
        "runbook_location",
        "escalation_path",
        "launch_checklist",
        "training_materials",
        "post_launch_review",
    ]
    assert all(record.evidence for record in report.records)


def test_partial_handoff_brief_reports_missing_details():
    report = build_source_operational_handoff_requirements(_source(["Support handoff is required.", "Runbook is needed."]))
    by_type = {record.requirement_type: record for record in report.records}

    assert by_type["support_handoff"].missing_details == ("support_owner", "handoff_channel", "handoff_timing")
    assert by_type["runbook_location"].missing_details == ("runbook_url", "owner", "update_cadence")
    assert by_type["runbook_location"].missing_detail_guidance == "runbook_url; owner; update_cadence"


def test_negated_handoff_scope_is_ignored_and_serializes():
    empty = build_source_operational_handoff_requirements(_source(["No operational handoff, support handoff, runbook, on-call, or training work is required."]))
    report = build_source_operational_handoff_requirements("Operational handoff requires launch checklist and escalation path.")

    assert empty.records == ()
    assert report.to_dicts() == report.to_dict()["requirements"]
    assert "Operational Handoff" in report.to_markdown()


def _source(requirements):
    return {
        "id": "sb-handoff",
        "title": "Operational readiness",
        "domain": "operations",
        "summary": "Operations planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-handoff",
        "source_payload": {"requirements": requirements},
        "source_links": {},
    }
