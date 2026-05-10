"""Issue and blocker tracker with resolution workflow.

Provides issue lifecycle management, assignment, escalation,
SLA tracking, and dashboard views.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from blueprint.issues.issue_model import (
    DEFAULT_SLA_HOURS,
    Issue,
    IssueSeverity,
    IssueStatus,
    IssueTransition,
    IssueType,
    SLAConfig,
    _gen_id,
    _now_iso,
)

# Valid status transitions
_VALID_TRANSITIONS: dict[IssueStatus, set[IssueStatus]] = {
    IssueStatus.OPEN: {IssueStatus.INVESTIGATING, IssueStatus.RESOLVED, IssueStatus.CLOSED},
    IssueStatus.INVESTIGATING: {IssueStatus.RESOLVED, IssueStatus.OPEN, IssueStatus.CLOSED},
    IssueStatus.RESOLVED: {IssueStatus.CLOSED, IssueStatus.OPEN},
    IssueStatus.CLOSED: {IssueStatus.OPEN},
}


class IssueTracker:
    """Manages issues and blockers with lifecycle workflow."""

    def __init__(self, sla_overrides: dict[IssueSeverity, int] | None = None) -> None:
        self._issues: dict[str, Issue] = {}
        self._transitions: list[IssueTransition] = []
        self._sla_hours: dict[IssueSeverity, int] = {**DEFAULT_SLA_HOURS}
        if sla_overrides:
            self._sla_hours.update(sla_overrides)

    # ------------------------------------------------------------------
    # Issue CRUD
    # ------------------------------------------------------------------

    def create_issue(
        self,
        title: str,
        *,
        description: str = "",
        issue_type: IssueType = IssueType.BLOCKER,
        severity: IssueSeverity = IssueSeverity.MEDIUM,
        owner: str = "",
        assignee: str = "",
        related_tasks: list[str] | None = None,
        plan_id: str = "",
    ) -> Issue:
        issue = Issue(
            issue_id=_gen_id("iss"),
            title=title,
            description=description,
            issue_type=issue_type,
            severity=severity,
            owner=owner,
            assignee=assignee,
            related_tasks=related_tasks or [],
            plan_id=plan_id,
        )
        self._issues[issue.issue_id] = issue
        return issue

    def get_issue(self, issue_id: str) -> Issue | None:
        return self._issues.get(issue_id)

    def list_issues(
        self,
        *,
        status: IssueStatus | None = None,
        severity: IssueSeverity | None = None,
        plan_id: str | None = None,
    ) -> list[Issue]:
        results = list(self._issues.values())
        if status is not None:
            results = [i for i in results if i.status == status]
        if severity is not None:
            results = [i for i in results if i.severity == severity]
        if plan_id is not None:
            results = [i for i in results if i.plan_id == plan_id]
        return results

    # ------------------------------------------------------------------
    # Lifecycle workflow
    # ------------------------------------------------------------------

    def transition_issue(
        self,
        issue_id: str,
        to_status: IssueStatus,
        *,
        transitioned_by: str = "",
        comment: str = "",
        root_cause: str = "",
        resolution: str = "",
    ) -> Issue | None:
        issue = self._issues.get(issue_id)
        if issue is None:
            return None

        valid = _VALID_TRANSITIONS.get(issue.status, set())
        if to_status not in valid:
            return None

        resolved_at = issue.resolved_at
        if to_status == IssueStatus.RESOLVED:
            resolved_at = _now_iso()

        updated = replace(
            issue,
            status=to_status,
            updated_at=_now_iso(),
            resolved_at=resolved_at,
            root_cause=root_cause or issue.root_cause,
            resolution=resolution or issue.resolution,
        )
        self._issues[issue_id] = updated

        self._transitions.append(
            IssueTransition(
                transition_id=_gen_id("trans"),
                issue_id=issue_id,
                from_status=issue.status,
                to_status=to_status,
                transitioned_by=transitioned_by,
                comment=comment,
            )
        )
        return updated

    # ------------------------------------------------------------------
    # Assignment & escalation
    # ------------------------------------------------------------------

    def assign_issue(self, issue_id: str, assignee: str) -> Issue | None:
        issue = self._issues.get(issue_id)
        if issue is None:
            return None
        updated = replace(issue, assignee=assignee, updated_at=_now_iso())
        self._issues[issue_id] = updated
        return updated

    def escalate_issue(self, issue_id: str) -> Issue | None:
        issue = self._issues.get(issue_id)
        if issue is None:
            return None
        updated = replace(
            issue,
            escalated=True,
            escalation_level=issue.escalation_level + 1,
            updated_at=_now_iso(),
        )
        self._issues[issue_id] = updated
        return updated

    # ------------------------------------------------------------------
    # Issue-to-task linking
    # ------------------------------------------------------------------

    def link_task(self, issue_id: str, task_id: str) -> Issue | None:
        issue = self._issues.get(issue_id)
        if issue is None:
            return None
        if task_id in issue.related_tasks:
            return issue
        updated = replace(
            issue,
            related_tasks=[*issue.related_tasks, task_id],
            updated_at=_now_iso(),
        )
        self._issues[issue_id] = updated
        return updated

    # ------------------------------------------------------------------
    # SLA & age detection
    # ------------------------------------------------------------------

    def get_sla_hours(self, severity: IssueSeverity) -> int:
        return self._sla_hours[severity]

    def configure_sla(self, severity: IssueSeverity, hours: int) -> None:
        self._sla_hours[severity] = hours

    def calculate_issue_age_hours(self, issue_id: str) -> float:
        issue = self._issues.get(issue_id)
        if issue is None:
            return 0.0
        created = datetime.fromisoformat(issue.created_at)
        now = datetime.now(timezone.utc)
        return (now - created).total_seconds() / 3600

    def find_overdue_issues(self) -> list[Issue]:
        overdue: list[Issue] = []
        now = datetime.now(timezone.utc)
        for issue in self._issues.values():
            if issue.status in (IssueStatus.RESOLVED, IssueStatus.CLOSED):
                continue
            sla = self._sla_hours[issue.severity]
            created = datetime.fromisoformat(issue.created_at)
            age_hours = (now - created).total_seconds() / 3600
            if age_hours > sla:
                overdue.append(issue)
        return overdue

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    def generate_dashboard(self) -> dict[str, Any]:
        issues = list(self._issues.values())
        open_issues = [i for i in issues if i.status in (IssueStatus.OPEN, IssueStatus.INVESTIGATING)]

        by_severity: dict[str, int] = {}
        for sev in IssueSeverity:
            by_severity[sev.value] = sum(1 for i in open_issues if i.severity == sev)

        by_type: dict[str, int] = {}
        for it in IssueType:
            by_type[it.value] = sum(1 for i in open_issues if i.issue_type == it)

        return {
            "total_issues": len(issues),
            "open_issues": len(open_issues),
            "resolved_issues": sum(1 for i in issues if i.status == IssueStatus.RESOLVED),
            "closed_issues": sum(1 for i in issues if i.status == IssueStatus.CLOSED),
            "escalated_issues": sum(1 for i in open_issues if i.escalated),
            "by_severity": by_severity,
            "by_type": by_type,
        }

    def get_transitions(self, issue_id: str) -> list[IssueTransition]:
        return [t for t in self._transitions if t.issue_id == issue_id]


__all__ = ["IssueTracker"]
