"""Tests for the audit trail and compliance reporting module."""

import json

from blueprint.audit.audit_trail import (
    AuditEntry,
    AuditTrailManager,
    ComplianceFramework,
    ComplianceReport,
    EventType,
    FRAMEWORK_CONTROLS,
    IntegrityReport,
    IntegrityStatus,
    RetentionPolicy,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_manager(signing_key: bytes | None = None) -> AuditTrailManager:
    return AuditTrailManager(signing_key=signing_key or b"test-signing-key-32bytes-pad!!")


def _record_plan_lifecycle(mgr: AuditTrailManager, plan_id: str = "plan-001") -> None:
    """Record a typical set of lifecycle events for a plan."""
    mgr.record_event(
        EventType.PLAN_CREATED, actor="alice", target=plan_id,
        changes={"after": {"status": "draft"}},
        ip_address="10.0.0.1",
    )
    mgr.record_event(
        EventType.TASK_ADDED, actor="alice", target=plan_id,
        changes={"after": {"task_id": "task-1", "title": "Implement auth"}},
        ip_address="10.0.0.1",
    )
    mgr.record_event(
        EventType.PLAN_UPDATED, actor="bob", target=plan_id,
        changes={
            "before": {"status": "draft"},
            "after": {"status": "in_progress"},
        },
        change_reason="Ready for development",
        ip_address="10.0.0.2",
    )
    mgr.record_event(
        EventType.STATUS_TRANSITION, actor="bob", target=plan_id,
        changes={
            "before": {"task_status": "pending"},
            "after": {"task_status": "in_progress"},
        },
        ip_address="10.0.0.2",
    )
    mgr.record_event(
        EventType.EXPORT_OPERATION, actor="alice", target=plan_id,
        metadata={"format": "markdown", "engine": "relay"},
        ip_address="10.0.0.1",
    )


# ---------------------------------------------------------------------------
# Event recording tests
# ---------------------------------------------------------------------------


class TestEventRecording:

    def test_record_basic_event(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.PLAN_CREATED,
            actor="alice",
            target="plan-001",
        )

        assert isinstance(entry, AuditEntry)
        assert entry.event_type == EventType.PLAN_CREATED
        assert entry.actor == "alice"
        assert entry.target == "plan-001"
        assert entry.timestamp
        assert entry.entry_id.startswith("audit-")
        assert entry.signature  # cryptographically signed

    def test_record_event_with_changes(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.PLAN_UPDATED,
            actor="bob",
            target="plan-001",
            changes={
                "before": {"status": "draft"},
                "after": {"status": "in_progress"},
            },
        )
        assert entry.before_state == {"status": "draft"}
        assert entry.after_state == {"status": "in_progress"}

    def test_record_event_with_metadata(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.API_ACCESS,
            actor="service-bot",
            target="plan-001",
            metadata={"endpoint": "/api/plans/001", "method": "GET"},
        )
        assert entry.metadata["endpoint"] == "/api/plans/001"

    def test_record_event_with_ip_and_reason(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.PERMISSION_CHANGED,
            actor="admin",
            target="plan-001",
            ip_address="192.168.1.1",
            change_reason="Granting editor access",
        )
        assert entry.ip_address == "192.168.1.1"
        assert entry.change_reason == "Granting editor access"

    def test_entry_to_dict(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.PLAN_CREATED, actor="alice", target="plan-001"
        )
        data = entry.to_dict()
        assert data["event_type"] == "plan_created"
        assert data["actor"] == "alice"
        assert "signature" in data

    def test_entry_to_json(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.PLAN_CREATED, actor="alice", target="plan-001"
        )
        parsed = json.loads(entry.to_json())
        assert parsed["entry_id"] == entry.entry_id

    def test_event_types_coverage(self):
        mgr = _make_manager()
        for et in EventType:
            entry = mgr.record_event(et, actor="system", target="plan-test")
            assert entry.event_type == et
        assert mgr.entry_count == len(EventType)

    def test_action_detail_generated(self):
        mgr = _make_manager()
        entry = mgr.record_event(
            EventType.TASK_MODIFIED, actor="charlie", target="plan-002"
        )
        assert "task_modified" in entry.action_detail
        assert "charlie" in entry.action_detail

    def test_sequential_entry_ids(self):
        mgr = _make_manager()
        e1 = mgr.record_event(EventType.PLAN_CREATED, actor="a", target="p")
        e2 = mgr.record_event(EventType.PLAN_UPDATED, actor="a", target="p")
        assert e1.entry_id < e2.entry_id


# ---------------------------------------------------------------------------
# Query tests
# ---------------------------------------------------------------------------


class TestQueryAuditTrail:

    def test_query_all(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        entries = mgr.query_audit_trail()
        assert len(entries) == 5

    def test_query_by_plan_id(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr, "plan-001")
        _record_plan_lifecycle(mgr, "plan-002")

        results = mgr.query_audit_trail({"plan_id": "plan-001"})
        assert len(results) == 5
        assert all(e.target == "plan-001" for e in results)

    def test_query_by_actor(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({"actor": "alice"})
        assert all(e.actor == "alice" for e in results)
        assert len(results) == 3  # alice did create, task_added, export

    def test_query_by_event_type_string(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({"event_type": "plan_updated"})
        assert len(results) == 1
        assert results[0].event_type == EventType.PLAN_UPDATED

    def test_query_by_event_type_enum(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({"event_type": EventType.EXPORT_OPERATION})
        assert len(results) == 1

    def test_query_by_action_type_alias(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({"action_type": "task_added"})
        assert len(results) == 1

    def test_query_by_date_range(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({
            "start_date": "2000-01-01",
            "end_date": "2099-12-31",
        })
        assert len(results) == 5  # all within range

    def test_query_combined_filters(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({
            "actor": "bob",
            "plan_id": "plan-001",
        })
        assert len(results) == 2  # bob did plan_updated and status_transition

    def test_query_empty_result(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        results = mgr.query_audit_trail({"plan_id": "nonexistent"})
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Compliance report tests
# ---------------------------------------------------------------------------


class TestComplianceReports:

    def test_soc2_report(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1", ComplianceFramework.SOC2)

        assert isinstance(report, ComplianceReport)
        assert report.plan_id == "plan-001"
        assert report.framework == ComplianceFramework.SOC2
        assert report.period == "2026-Q1"
        assert report.total_events == 5
        assert report.controls_assessed > 0
        assert 0.0 <= report.compliance_score <= 1.0
        assert report.generated_at

    def test_gdpr_report(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1", ComplianceFramework.GDPR)
        assert report.framework == ComplianceFramework.GDPR
        assert report.controls_assessed == len(FRAMEWORK_CONTROLS[ComplianceFramework.GDPR])

    def test_hipaa_report(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1", ComplianceFramework.HIPAA)
        assert report.framework == ComplianceFramework.HIPAA

    def test_iso27001_report(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1", ComplianceFramework.ISO27001)
        assert report.framework == ComplianceFramework.ISO27001

    def test_compliance_report_to_dict(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1")
        data = report.to_dict()
        assert data["plan_id"] == "plan-001"
        assert "compliance_score" in data
        assert "findings" in data
        assert "recommendations" in data

    def test_compliance_report_to_json(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.generate_compliance_report("plan-001", "2026-Q1")
        parsed = json.loads(report.to_json())
        assert parsed["framework"] == "soc2"

    def test_report_for_plan_with_no_events(self):
        mgr = _make_manager()
        report = mgr.generate_compliance_report("empty-plan", "2026-Q1")
        assert report.total_events == 0
        findings_text = " ".join(report.findings)
        assert "No audit events recorded" in findings_text

    def test_full_compliance_score_with_all_event_types(self):
        mgr = _make_manager()
        # Record at least one event of each SOC2-relevant type
        for control_events in FRAMEWORK_CONTROLS[ComplianceFramework.SOC2].values():
            for et in control_events:
                mgr.record_event(et, actor="system", target="plan-full")
        report = mgr.generate_compliance_report("plan-full", "2026-Q1", ComplianceFramework.SOC2)
        assert report.compliance_score == 1.0
        assert report.controls_passed == report.controls_assessed


# ---------------------------------------------------------------------------
# Integrity verification tests
# ---------------------------------------------------------------------------


class TestIntegrityVerification:

    def test_verify_valid_entries(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.verify_data_integrity("plan-001")

        assert isinstance(report, IntegrityReport)
        assert report.status == IntegrityStatus.VALID
        assert report.entries_verified == 5
        assert report.entries_valid == 5
        assert report.entries_tampered == 0

    def test_verify_empty_plan(self):
        mgr = _make_manager()
        report = mgr.verify_data_integrity("nonexistent")
        assert report.status == IntegrityStatus.VALID
        assert report.entries_verified == 0

    def test_detect_tampered_entry(self):
        mgr = _make_manager()
        mgr.record_event(EventType.PLAN_CREATED, actor="alice", target="plan-001")

        # Manually tamper with the entry's signature
        entry = mgr.entries[0]
        tampered = AuditEntry(
            entry_id=entry.entry_id,
            event_type=entry.event_type,
            actor="malicious-actor",  # changed!
            target=entry.target,
            timestamp=entry.timestamp,
            before_state=entry.before_state,
            after_state=entry.after_state,
            signature=entry.signature,  # original signature
        )
        mgr._entries[0] = tampered

        report = mgr.verify_data_integrity("plan-001")
        assert report.status == IntegrityStatus.TAMPERED
        assert report.entries_tampered == 1
        assert entry.entry_id in report.tampered_entry_ids

    def test_integrity_report_to_dict(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        report = mgr.verify_data_integrity("plan-001")
        data = report.to_dict()
        assert data["status"] == "valid"
        assert data["entries_verified"] == 5


# ---------------------------------------------------------------------------
# Export tests
# ---------------------------------------------------------------------------


class TestAuditExport:

    def test_export_jsonl(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        output = mgr.export_audit_log("jsonl")

        lines = output.strip().split("\n")
        assert len(lines) == 5
        parsed = json.loads(lines[0])
        assert "entry_id" in parsed

    def test_export_json(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        output = mgr.export_audit_log("json")

        parsed = json.loads(output)
        assert isinstance(parsed, list)
        assert len(parsed) == 5

    def test_export_csv(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        output = mgr.export_audit_log("csv")

        lines = output.strip().split("\n")
        assert lines[0].startswith("entry_id,event_type")
        assert len(lines) == 6  # header + 5 events

    def test_export_csv_empty(self):
        mgr = _make_manager()
        output = mgr.export_audit_log("csv")
        assert "entry_id" in output

    def test_export_with_filters(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        output = mgr.export_audit_log("json", filters={"actor": "alice"})
        parsed = json.loads(output)
        assert len(parsed) == 3


# ---------------------------------------------------------------------------
# SIEM streaming tests
# ---------------------------------------------------------------------------


class TestSIEMStreaming:

    def test_register_siem_callback(self):
        mgr = _make_manager()
        received: list[AuditEntry] = []
        mgr.register_siem_callback(lambda e: received.append(e))

        mgr.record_event(EventType.PLAN_CREATED, actor="alice", target="plan-001")
        assert len(received) == 1
        assert received[0].event_type == EventType.PLAN_CREATED

    def test_multiple_siem_callbacks(self):
        mgr = _make_manager()
        log1: list[AuditEntry] = []
        log2: list[AuditEntry] = []
        mgr.register_siem_callback(lambda e: log1.append(e))
        mgr.register_siem_callback(lambda e: log2.append(e))

        mgr.record_event(EventType.LOGIN, actor="user", target="system")
        assert len(log1) == 1
        assert len(log2) == 1


# ---------------------------------------------------------------------------
# Retention policy tests
# ---------------------------------------------------------------------------


class TestRetentionPolicy:

    def test_retention_policy_to_dict(self):
        policy = RetentionPolicy(max_age_days=365, archive_after_days=90)
        data = policy.to_dict()
        assert data["max_age_days"] == 365
        assert data["archive_after_days"] == 90

    def test_manager_with_retention_policy(self):
        policy = RetentionPolicy(max_age_days=90, archive_format="json")
        mgr = AuditTrailManager(retention_policy=policy)
        assert mgr.retention_policy is not None
        assert mgr.retention_policy.max_age_days == 90

    def test_apply_retention_policy_noop_without_policy(self):
        mgr = _make_manager()
        _record_plan_lifecycle(mgr)
        removed = mgr.apply_retention_policy()
        assert removed == 0


# ---------------------------------------------------------------------------
# Framework controls tests
# ---------------------------------------------------------------------------


class TestFrameworkControls:

    def test_all_frameworks_have_controls(self):
        for framework in ComplianceFramework:
            assert framework in FRAMEWORK_CONTROLS
            assert len(FRAMEWORK_CONTROLS[framework]) > 0

    def test_soc2_has_access_control(self):
        assert "access_control" in FRAMEWORK_CONTROLS[ComplianceFramework.SOC2]

    def test_gdpr_has_data_changes(self):
        assert "data_changes" in FRAMEWORK_CONTROLS[ComplianceFramework.GDPR]

    def test_hipaa_has_phi_access(self):
        assert "phi_access" in FRAMEWORK_CONTROLS[ComplianceFramework.HIPAA]

    def test_iso27001_has_security_events(self):
        assert "security_events" in FRAMEWORK_CONTROLS[ComplianceFramework.ISO27001]
