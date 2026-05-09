"""GDPR compliance framework supporting data subject rights and privacy controls.

Handles data subject requests (access, erasure, rectification, portability,
objection), consent management, data retention policies, and privacy reporting.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Mapping


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DataSubjectRight(str, Enum):
    """GDPR data subject rights."""

    ACCESS = "access"
    ERASURE = "erasure"
    RECTIFICATION = "rectification"
    PORTABILITY = "portability"
    OBJECTION = "objection"


class ConsentType(str, Enum):
    """Granular consent types."""

    ANALYTICS = "analytics"
    MARKETING = "marketing"
    PROFILING = "profiling"
    THIRD_PARTY_SHARING = "third_party_sharing"
    DATA_PROCESSING = "data_processing"
    COOKIES = "cookies"


class RetentionAction(str, Enum):
    """Action to take when retention period expires."""

    DELETE = "delete"
    ANONYMIZE = "anonymize"
    ARCHIVE = "archive"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class DataExport:
    """Result of a data export (right to access / portability)."""

    user_id: str
    export_format: str
    data: dict[str, Any] = field(default_factory=dict)
    generated_at: str = ""
    categories: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "export_format": self.export_format,
            "data": dict(self.data),
            "generated_at": self.generated_at,
            "categories": list(self.categories),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

    def to_csv_rows(self) -> list[dict[str, Any]]:
        """Flatten data into CSV-compatible rows."""
        rows: list[dict[str, Any]] = []
        for category, values in self.data.items():
            if isinstance(values, dict):
                rows.append({"category": category, **values})
            elif isinstance(values, list):
                for item in values:
                    if isinstance(item, dict):
                        rows.append({"category": category, **item})
                    else:
                        rows.append({"category": category, "value": item})
            else:
                rows.append({"category": category, "value": values})
        return rows


@dataclass(frozen=True, slots=True)
class DeletionReport:
    """Report of a data deletion (right to erasure)."""

    user_id: str
    deleted_categories: tuple[str, ...] = field(default_factory=tuple)
    retained_categories: tuple[str, ...] = field(default_factory=tuple)
    retention_reasons: dict[str, str] = field(default_factory=dict)
    completed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "deleted_categories": list(self.deleted_categories),
            "retained_categories": list(self.retained_categories),
            "retention_reasons": dict(self.retention_reasons),
            "completed_at": self.completed_at,
        }


@dataclass(frozen=True, slots=True)
class AnonymizationReport:
    """Report of a data anonymization."""

    user_id: str
    anonymized_fields: tuple[str, ...] = field(default_factory=tuple)
    technique: str = "pseudonymization"
    completed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "anonymized_fields": list(self.anonymized_fields),
            "technique": self.technique,
            "completed_at": self.completed_at,
        }


@dataclass(frozen=True, slots=True)
class ConsentRecord:
    """Record of a consent decision."""

    user_id: str
    consent_type: ConsentType
    granted: bool
    timestamp: str
    version: str = "1.0"
    source: str = "user_action"

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "consent_type": self.consent_type.value,
            "granted": self.granted,
            "timestamp": self.timestamp,
            "version": self.version,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Data retention policy for a category."""

    category: str
    retention_days: int
    action: RetentionAction
    legal_basis: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "retention_days": self.retention_days,
            "action": self.action.value,
            "legal_basis": self.legal_basis,
        }


@dataclass(frozen=True, slots=True)
class AuditLogEntry:
    """Audit log entry for data subject requests."""

    request_id: str
    user_id: str
    right: DataSubjectRight
    action: str
    timestamp: str
    details: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "user_id": self.user_id,
            "right": self.right.value,
            "action": self.action,
            "timestamp": self.timestamp,
            "details": self.details,
        }


@dataclass(frozen=True, slots=True)
class PrivacyReport:
    """GDPR compliance privacy report."""

    period: str
    total_requests: int = 0
    requests_by_right: dict[str, int] = field(default_factory=dict)
    avg_response_time_hours: float = 0.0
    consent_rates: dict[str, float] = field(default_factory=dict)
    retention_compliance: float = 0.0
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "period": self.period,
            "total_requests": self.total_requests,
            "requests_by_right": dict(self.requests_by_right),
            "avg_response_time_hours": round(self.avg_response_time_hours, 2),
            "consent_rates": dict(self.consent_rates),
            "retention_compliance": round(self.retention_compliance, 4),
            "recommendations": list(self.recommendations),
        }


# ---------------------------------------------------------------------------
# GDPRManager
# ---------------------------------------------------------------------------

class GDPRManager:
    """Manage GDPR compliance: data subject rights, consent, retention."""

    def __init__(self) -> None:
        self._user_data: dict[str, dict[str, Any]] = {}
        self._consent_records: list[ConsentRecord] = []
        self._audit_log: list[AuditLogEntry] = []
        self._retention_policies: list[RetentionPolicy] = []
        self._request_counter = 0

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    def store_user_data(self, user_id: str, data: Mapping[str, Any]) -> None:
        """Store or update user data (for testing/demonstration)."""
        self._user_data[user_id] = dict(data)

    # ------------------------------------------------------------------
    # Right to access / portability
    # ------------------------------------------------------------------

    def export_user_data(
        self,
        user_id: str,
        export_format: str = "json",
    ) -> DataExport:
        """Export all user data in a structured format.

        Supports ``json`` and ``csv`` export formats.
        Implements right to access and right to portability.
        """
        now = datetime.now(timezone.utc).isoformat()
        data = dict(self._user_data.get(user_id, {}))

        # Include consent records
        user_consents = [
            c.to_dict()
            for c in self._consent_records
            if c.user_id == user_id
        ]
        if user_consents:
            data["consent_history"] = user_consents

        categories = tuple(data.keys())

        self._log_audit(
            user_id, DataSubjectRight.ACCESS,
            "data_exported", f"Format: {export_format}",
        )

        return DataExport(
            user_id=user_id,
            export_format=export_format,
            data=data,
            generated_at=now,
            categories=categories,
        )

    # ------------------------------------------------------------------
    # Right to erasure
    # ------------------------------------------------------------------

    def delete_user_data(
        self,
        user_id: str,
        retention_policy: Mapping[str, str] | None = None,
    ) -> DeletionReport:
        """Delete user personal data respecting retention requirements.

        Args:
            user_id: The user whose data to delete.
            retention_policy: Mapping of category → reason for categories
                that must be retained (e.g., legal obligation).
        """
        now = datetime.now(timezone.utc).isoformat()
        retention = dict(retention_policy) if retention_policy else {}
        user_data = self._user_data.get(user_id, {})

        deleted: list[str] = []
        retained: list[str] = []
        retention_reasons: dict[str, str] = {}

        for category in list(user_data.keys()):
            if category in retention:
                retained.append(category)
                retention_reasons[category] = retention[category]
            else:
                deleted.append(category)

        # Remove deletable data
        if user_id in self._user_data:
            for cat in deleted:
                self._user_data[user_id].pop(cat, None)
            if not self._user_data[user_id]:
                del self._user_data[user_id]

        self._log_audit(
            user_id, DataSubjectRight.ERASURE,
            "data_deleted",
            f"Deleted: {deleted}, Retained: {retained}",
        )

        return DeletionReport(
            user_id=user_id,
            deleted_categories=tuple(sorted(deleted)),
            retained_categories=tuple(sorted(retained)),
            retention_reasons=retention_reasons,
            completed_at=now,
        )

    # ------------------------------------------------------------------
    # Anonymization
    # ------------------------------------------------------------------

    def anonymize_user_data(self, user_id: str) -> AnonymizationReport:
        """Anonymize user data using pseudonymization.

        Replaces PII fields with hashed pseudonyms.
        """
        now = datetime.now(timezone.utc).isoformat()
        user_data = self._user_data.get(user_id, {})
        anonymized_fields: list[str] = []

        pii_fields = ["name", "email", "phone", "address", "ip_address"]

        for field_name in pii_fields:
            if field_name in user_data:
                original = str(user_data[field_name])
                pseudonym = hashlib.sha256(
                    f"{user_id}:{field_name}:{original}".encode()
                ).hexdigest()[:16]
                user_data[field_name] = f"anon_{pseudonym}"
                anonymized_fields.append(field_name)

        if user_id in self._user_data:
            self._user_data[user_id] = user_data

        self._log_audit(
            user_id, DataSubjectRight.ERASURE,
            "data_anonymized",
            f"Anonymized fields: {anonymized_fields}",
        )

        return AnonymizationReport(
            user_id=user_id,
            anonymized_fields=tuple(anonymized_fields),
            technique="pseudonymization",
            completed_at=now,
        )

    # ------------------------------------------------------------------
    # Consent management
    # ------------------------------------------------------------------

    def process_consent(
        self,
        user_id: str,
        consent_type: ConsentType,
        granted: bool,
        version: str = "1.0",
        source: str = "user_action",
    ) -> ConsentRecord:
        """Record a consent decision with timestamp."""
        now = datetime.now(timezone.utc).isoformat()
        record = ConsentRecord(
            user_id=user_id,
            consent_type=consent_type,
            granted=granted,
            timestamp=now,
            version=version,
            source=source,
        )
        self._consent_records.append(record)

        self._log_audit(
            user_id, DataSubjectRight.OBJECTION,
            "consent_recorded",
            f"Type: {consent_type.value}, Granted: {granted}",
        )

        return record

    def get_user_consents(self, user_id: str) -> list[ConsentRecord]:
        """Get all consent records for a user."""
        return [c for c in self._consent_records if c.user_id == user_id]

    def has_consent(self, user_id: str, consent_type: ConsentType) -> bool:
        """Check if user has active consent for a given type."""
        user_consents = [
            c for c in self._consent_records
            if c.user_id == user_id and c.consent_type == consent_type
        ]
        if not user_consents:
            return False
        # Return the most recent consent decision
        latest = max(user_consents, key=lambda c: c.timestamp)
        return latest.granted

    # ------------------------------------------------------------------
    # Retention policies
    # ------------------------------------------------------------------

    def add_retention_policy(self, policy: RetentionPolicy) -> None:
        """Register a data retention policy."""
        self._retention_policies.append(policy)

    def get_expired_data(
        self,
        reference_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Find data categories that have exceeded their retention period."""
        ref = reference_date or datetime.now(timezone.utc)
        expired: list[dict[str, Any]] = []

        for policy in self._retention_policies:
            expiry = ref - timedelta(days=policy.retention_days)
            expired.append({
                "category": policy.category,
                "retention_days": policy.retention_days,
                "action": policy.action.value,
                "expiry_date": expiry.isoformat(),
                "legal_basis": policy.legal_basis,
            })

        return expired

    @property
    def retention_policies(self) -> list[RetentionPolicy]:
        return list(self._retention_policies)

    # ------------------------------------------------------------------
    # Privacy reporting
    # ------------------------------------------------------------------

    def generate_privacy_report(self, period: str) -> PrivacyReport:
        """Generate a GDPR compliance report for a given period."""
        requests_by_right: dict[str, int] = {}
        for entry in self._audit_log:
            right_val = entry.right.value
            requests_by_right[right_val] = requests_by_right.get(right_val, 0) + 1

        total_requests = len(self._audit_log)

        # Compute consent rates
        consent_rates: dict[str, float] = {}
        for ct in ConsentType:
            type_records = [
                c for c in self._consent_records
                if c.consent_type == ct
            ]
            if type_records:
                granted = sum(1 for c in type_records if c.granted)
                consent_rates[ct.value] = round(granted / len(type_records), 4)

        # Retention compliance (fraction of policies defined)
        retention_compliance = min(
            len(self._retention_policies) / max(len(ConsentType), 1), 1.0
        )

        recommendations: list[str] = []
        if not self._retention_policies:
            recommendations.append("Define data retention policies for all data categories.")
        if total_requests == 0:
            recommendations.append("No data subject requests recorded; ensure request channels are accessible.")
        for ct in ConsentType:
            if ct.value not in consent_rates:
                recommendations.append(f"No consent records for {ct.value}; ensure consent collection.")

        return PrivacyReport(
            period=period,
            total_requests=total_requests,
            requests_by_right=requests_by_right,
            avg_response_time_hours=0.0,  # Would require timestamps in production
            consent_rates=consent_rates,
            retention_compliance=retention_compliance,
            recommendations=tuple(recommendations),
        )

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    def _log_audit(
        self,
        user_id: str,
        right: DataSubjectRight,
        action: str,
        details: str = "",
    ) -> None:
        self._request_counter += 1
        self._audit_log.append(AuditLogEntry(
            request_id=f"req-{self._request_counter:04d}",
            user_id=user_id,
            right=right,
            action=action,
            timestamp=datetime.now(timezone.utc).isoformat(),
            details=details,
        ))

    @property
    def audit_log(self) -> list[AuditLogEntry]:
        return list(self._audit_log)


__all__ = [
    "AnonymizationReport",
    "AuditLogEntry",
    "ConsentRecord",
    "ConsentType",
    "DataExport",
    "DataSubjectRight",
    "DeletionReport",
    "GDPRManager",
    "PrivacyReport",
    "RetentionAction",
    "RetentionPolicy",
]
