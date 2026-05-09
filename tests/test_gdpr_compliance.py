"""Tests for GDPR compliance framework."""

import pytest

from blueprint.compliance.gdpr_compliance import (
    AnonymizationReport,
    AuditLogEntry,
    ConsentRecord,
    ConsentType,
    DataExport,
    DataSubjectRight,
    DeletionReport,
    GDPRManager,
    PrivacyReport,
    RetentionAction,
    RetentionPolicy,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def manager() -> GDPRManager:
    return GDPRManager()


@pytest.fixture
def manager_with_data(manager: GDPRManager) -> GDPRManager:
    manager.store_user_data("user-1", {
        "name": "Alice Smith",
        "email": "alice@example.com",
        "phone": "+1234567890",
        "address": "123 Main St",
        "preferences": {"theme": "dark"},
        "activity_log": [{"action": "login", "ts": "2025-01-01"}],
    })
    manager.store_user_data("user-2", {
        "name": "Bob Jones",
        "email": "bob@example.com",
        "ip_address": "192.168.1.1",
    })
    return manager


# ---------------------------------------------------------------------------
# Data export (right to access / portability)
# ---------------------------------------------------------------------------

class TestExportUserData:
    def test_export_json_format(self, manager_with_data: GDPRManager) -> None:
        export = manager_with_data.export_user_data("user-1", "json")
        assert isinstance(export, DataExport)
        assert export.user_id == "user-1"
        assert export.export_format == "json"
        assert "name" in export.data
        assert "email" in export.data
        assert export.generated_at

    def test_export_csv_format(self, manager_with_data: GDPRManager) -> None:
        export = manager_with_data.export_user_data("user-1", "csv")
        assert export.export_format == "csv"
        rows = export.to_csv_rows()
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_export_includes_categories(self, manager_with_data: GDPRManager) -> None:
        export = manager_with_data.export_user_data("user-1")
        assert len(export.categories) > 0
        assert "name" in export.categories

    def test_export_nonexistent_user(self, manager: GDPRManager) -> None:
        export = manager.export_user_data("nonexistent")
        assert export.data == {}
        assert len(export.categories) == 0

    def test_export_to_json_string(self, manager_with_data: GDPRManager) -> None:
        export = manager_with_data.export_user_data("user-1")
        json_str = export.to_json()
        assert isinstance(json_str, str)
        assert "alice@example.com" in json_str

    def test_export_includes_consent_history(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.process_consent("user-1", ConsentType.ANALYTICS, True)
        export = manager_with_data.export_user_data("user-1")
        assert "consent_history" in export.data

    def test_export_to_dict(self, manager_with_data: GDPRManager) -> None:
        export = manager_with_data.export_user_data("user-1")
        d = export.to_dict()
        assert d["user_id"] == "user-1"
        assert isinstance(d["categories"], list)

    def test_export_creates_audit_log(self, manager_with_data: GDPRManager) -> None:
        manager_with_data.export_user_data("user-1")
        logs = manager_with_data.audit_log
        assert len(logs) > 0
        assert logs[-1].right == DataSubjectRight.ACCESS


# ---------------------------------------------------------------------------
# Data deletion (right to erasure)
# ---------------------------------------------------------------------------

class TestDeleteUserData:
    def test_deletes_all_data(self, manager_with_data: GDPRManager) -> None:
        report = manager_with_data.delete_user_data("user-1")
        assert isinstance(report, DeletionReport)
        assert report.user_id == "user-1"
        assert len(report.deleted_categories) > 0
        assert len(report.retained_categories) == 0
        assert report.completed_at

    def test_respects_retention_policy(self, manager_with_data: GDPRManager) -> None:
        retention = {"activity_log": "legal_obligation"}
        report = manager_with_data.delete_user_data("user-1", retention)
        assert "activity_log" in report.retained_categories
        assert "activity_log" not in report.deleted_categories
        assert report.retention_reasons["activity_log"] == "legal_obligation"

    def test_delete_nonexistent_user(self, manager: GDPRManager) -> None:
        report = manager.delete_user_data("nonexistent")
        assert len(report.deleted_categories) == 0

    def test_delete_removes_data(self, manager_with_data: GDPRManager) -> None:
        manager_with_data.delete_user_data("user-1")
        export = manager_with_data.export_user_data("user-1")
        assert export.data == {}

    def test_delete_creates_audit_log(self, manager_with_data: GDPRManager) -> None:
        manager_with_data.delete_user_data("user-1")
        logs = manager_with_data.audit_log
        assert any(e.right == DataSubjectRight.ERASURE for e in logs)

    def test_deletion_report_to_dict(self, manager_with_data: GDPRManager) -> None:
        report = manager_with_data.delete_user_data("user-1")
        d = report.to_dict()
        assert "deleted_categories" in d
        assert "retained_categories" in d


# ---------------------------------------------------------------------------
# Anonymization
# ---------------------------------------------------------------------------

class TestAnonymizeUserData:
    def test_anonymizes_pii_fields(self, manager_with_data: GDPRManager) -> None:
        report = manager_with_data.anonymize_user_data("user-1")
        assert isinstance(report, AnonymizationReport)
        assert report.user_id == "user-1"
        assert "name" in report.anonymized_fields
        assert "email" in report.anonymized_fields
        assert report.technique == "pseudonymization"

    def test_anonymized_values_are_pseudonyms(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.anonymize_user_data("user-1")
        export = manager_with_data.export_user_data("user-1")
        assert export.data["name"].startswith("anon_")
        assert export.data["email"].startswith("anon_")

    def test_anonymize_preserves_non_pii(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.anonymize_user_data("user-1")
        export = manager_with_data.export_user_data("user-1")
        assert "preferences" in export.data

    def test_anonymize_ip_address(self, manager_with_data: GDPRManager) -> None:
        report = manager_with_data.anonymize_user_data("user-2")
        assert "ip_address" in report.anonymized_fields

    def test_anonymize_nonexistent_user(self, manager: GDPRManager) -> None:
        report = manager.anonymize_user_data("nonexistent")
        assert len(report.anonymized_fields) == 0

    def test_anonymization_report_to_dict(
        self, manager_with_data: GDPRManager
    ) -> None:
        report = manager_with_data.anonymize_user_data("user-1")
        d = report.to_dict()
        assert d["technique"] == "pseudonymization"
        assert isinstance(d["anonymized_fields"], list)


# ---------------------------------------------------------------------------
# Consent management
# ---------------------------------------------------------------------------

class TestConsent:
    def test_grant_consent(self, manager: GDPRManager) -> None:
        record = manager.process_consent("user-1", ConsentType.ANALYTICS, True)
        assert isinstance(record, ConsentRecord)
        assert record.user_id == "user-1"
        assert record.consent_type == ConsentType.ANALYTICS
        assert record.granted is True
        assert record.timestamp

    def test_revoke_consent(self, manager: GDPRManager) -> None:
        manager.process_consent("user-1", ConsentType.MARKETING, True)
        manager.process_consent("user-1", ConsentType.MARKETING, False)
        assert not manager.has_consent("user-1", ConsentType.MARKETING)

    def test_granular_consent_types(self, manager: GDPRManager) -> None:
        for ct in ConsentType:
            manager.process_consent("user-1", ct, True)
        assert manager.has_consent("user-1", ConsentType.ANALYTICS)
        assert manager.has_consent("user-1", ConsentType.COOKIES)

    def test_consent_with_version(self, manager: GDPRManager) -> None:
        record = manager.process_consent(
            "user-1", ConsentType.DATA_PROCESSING, True, version="2.0",
        )
        assert record.version == "2.0"

    def test_get_user_consents(self, manager: GDPRManager) -> None:
        manager.process_consent("user-1", ConsentType.ANALYTICS, True)
        manager.process_consent("user-1", ConsentType.MARKETING, False)
        manager.process_consent("user-2", ConsentType.ANALYTICS, True)

        user1_consents = manager.get_user_consents("user-1")
        assert len(user1_consents) == 2

    def test_has_consent_no_records(self, manager: GDPRManager) -> None:
        assert not manager.has_consent("unknown", ConsentType.ANALYTICS)

    def test_consent_record_to_dict(self, manager: GDPRManager) -> None:
        record = manager.process_consent("user-1", ConsentType.PROFILING, True)
        d = record.to_dict()
        assert d["consent_type"] == "profiling"
        assert d["granted"] is True

    def test_consent_timestamp_recorded(self, manager: GDPRManager) -> None:
        record = manager.process_consent("user-1", ConsentType.COOKIES, True)
        assert record.timestamp
        assert "T" in record.timestamp  # ISO format


# ---------------------------------------------------------------------------
# Retention policies
# ---------------------------------------------------------------------------

class TestRetentionPolicies:
    def test_add_retention_policy(self, manager: GDPRManager) -> None:
        policy = RetentionPolicy(
            category="activity_log",
            retention_days=365,
            action=RetentionAction.DELETE,
            legal_basis="legitimate_interest",
        )
        manager.add_retention_policy(policy)
        assert len(manager.retention_policies) == 1

    def test_multiple_retention_policies(self, manager: GDPRManager) -> None:
        manager.add_retention_policy(RetentionPolicy(
            category="logs", retention_days=90, action=RetentionAction.DELETE,
        ))
        manager.add_retention_policy(RetentionPolicy(
            category="backups", retention_days=180, action=RetentionAction.ARCHIVE,
        ))
        assert len(manager.retention_policies) == 2

    def test_get_expired_data(self, manager: GDPRManager) -> None:
        manager.add_retention_policy(RetentionPolicy(
            category="temp_data", retention_days=30, action=RetentionAction.DELETE,
        ))
        expired = manager.get_expired_data()
        assert len(expired) == 1
        assert expired[0]["category"] == "temp_data"
        assert expired[0]["action"] == "delete"

    def test_retention_policy_to_dict(self) -> None:
        policy = RetentionPolicy(
            category="audit", retention_days=730,
            action=RetentionAction.ANONYMIZE, legal_basis="regulatory",
        )
        d = policy.to_dict()
        assert d["retention_days"] == 730
        assert d["action"] == "anonymize"

    def test_automatic_expiration(self, manager: GDPRManager) -> None:
        manager.add_retention_policy(RetentionPolicy(
            category="session_data", retention_days=1,
            action=RetentionAction.DELETE,
        ))
        expired = manager.get_expired_data()
        assert len(expired) == 1


# ---------------------------------------------------------------------------
# Privacy reporting
# ---------------------------------------------------------------------------

class TestPrivacyReport:
    def test_generate_report_structure(self, manager: GDPRManager) -> None:
        report = manager.generate_privacy_report("2025-Q1")
        assert isinstance(report, PrivacyReport)
        assert report.period == "2025-Q1"

    def test_report_counts_requests(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.export_user_data("user-1")
        manager_with_data.delete_user_data("user-2")
        report = manager_with_data.generate_privacy_report("2025-Q1")
        assert report.total_requests == 2

    def test_report_includes_consent_rates(self, manager: GDPRManager) -> None:
        manager.process_consent("user-1", ConsentType.ANALYTICS, True)
        manager.process_consent("user-2", ConsentType.ANALYTICS, True)
        manager.process_consent("user-3", ConsentType.ANALYTICS, False)
        report = manager.generate_privacy_report("2025-Q1")
        assert "analytics" in report.consent_rates
        assert abs(report.consent_rates["analytics"] - 0.6667) < 0.01

    def test_report_includes_recommendations(self, manager: GDPRManager) -> None:
        report = manager.generate_privacy_report("2025-Q1")
        assert len(report.recommendations) > 0

    def test_report_retention_compliance(self, manager: GDPRManager) -> None:
        for i in range(6):
            manager.add_retention_policy(RetentionPolicy(
                category=f"cat-{i}", retention_days=90,
                action=RetentionAction.DELETE,
            ))
        report = manager.generate_privacy_report("2025-Q1")
        assert report.retention_compliance == 1.0

    def test_report_to_dict(self, manager: GDPRManager) -> None:
        report = manager.generate_privacy_report("2025-Q1")
        d = report.to_dict()
        assert d["period"] == "2025-Q1"
        assert "recommendations" in d


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    def test_audit_log_created_on_export(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.export_user_data("user-1")
        logs = manager_with_data.audit_log
        assert len(logs) == 1
        assert logs[0].user_id == "user-1"
        assert logs[0].right == DataSubjectRight.ACCESS

    def test_audit_log_created_on_delete(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.delete_user_data("user-1")
        logs = manager_with_data.audit_log
        assert any(e.action == "data_deleted" for e in logs)

    def test_audit_log_entry_to_dict(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.export_user_data("user-1")
        entry = manager_with_data.audit_log[0]
        d = entry.to_dict()
        assert "request_id" in d
        assert "timestamp" in d
        assert d["right"] == "access"

    def test_audit_log_sequential_ids(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.export_user_data("user-1")
        manager_with_data.delete_user_data("user-1")
        logs = manager_with_data.audit_log
        assert logs[0].request_id == "req-0001"
        assert logs[1].request_id == "req-0002"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_export_after_delete(self, manager_with_data: GDPRManager) -> None:
        manager_with_data.delete_user_data("user-1")
        export = manager_with_data.export_user_data("user-1")
        assert export.data == {}

    def test_anonymize_after_anonymize(
        self, manager_with_data: GDPRManager
    ) -> None:
        manager_with_data.anonymize_user_data("user-1")
        report = manager_with_data.anonymize_user_data("user-1")
        # Second anonymization should still work (values already pseudonymized)
        assert isinstance(report, AnonymizationReport)

    def test_consent_for_multiple_users(self, manager: GDPRManager) -> None:
        manager.process_consent("user-1", ConsentType.ANALYTICS, True)
        manager.process_consent("user-2", ConsentType.ANALYTICS, False)
        assert manager.has_consent("user-1", ConsentType.ANALYTICS)
        assert not manager.has_consent("user-2", ConsentType.ANALYTICS)

    def test_delete_with_full_retention(
        self, manager_with_data: GDPRManager
    ) -> None:
        """All categories retained = nothing deleted."""
        export = manager_with_data.export_user_data("user-1")
        retention = {cat: "legal" for cat in export.categories}
        report = manager_with_data.delete_user_data("user-1", retention)
        assert len(report.deleted_categories) == 0
        assert len(report.retained_categories) > 0
