"""Tests for issue tracker covering workflow and escalation."""

import pytest

from blueprint.issues.issue_tracker import IssueTracker
from blueprint.issues.issue_model import (
    IssueSeverity,
    IssueStatus,
    IssueType,
)


@pytest.fixture
def tracker() -> IssueTracker:
    return IssueTracker()


class TestIssueCRUD:
    def test_create_issue(self, tracker: IssueTracker):
        issue = tracker.create_issue(
            "Build failing",
            issue_type=IssueType.BLOCKER,
            severity=IssueSeverity.CRITICAL,
            owner="alice",
        )
        assert issue.title == "Build failing"
        assert issue.status == IssueStatus.OPEN
        assert issue.severity == IssueSeverity.CRITICAL

    def test_get_issue(self, tracker: IssueTracker):
        issue = tracker.create_issue("Test")
        assert tracker.get_issue(issue.issue_id) is not None
        assert tracker.get_issue("missing") is None

    def test_list_issues_filter_status(self, tracker: IssueTracker):
        tracker.create_issue("Open 1")
        i2 = tracker.create_issue("Open 2")
        tracker.transition_issue(i2.issue_id, IssueStatus.INVESTIGATING)
        open_issues = tracker.list_issues(status=IssueStatus.OPEN)
        assert len(open_issues) == 1

    def test_list_issues_filter_severity(self, tracker: IssueTracker):
        tracker.create_issue("Critical", severity=IssueSeverity.CRITICAL)
        tracker.create_issue("Low", severity=IssueSeverity.LOW)
        critical = tracker.list_issues(severity=IssueSeverity.CRITICAL)
        assert len(critical) == 1


class TestWorkflow:
    def test_valid_transition(self, tracker: IssueTracker):
        issue = tracker.create_issue("Bug")
        result = tracker.transition_issue(issue.issue_id, IssueStatus.INVESTIGATING)
        assert result is not None
        assert result.status == IssueStatus.INVESTIGATING

    def test_invalid_transition(self, tracker: IssueTracker):
        issue = tracker.create_issue("Bug")
        # Can't go directly from OPEN to CLOSED? Actually it can per _VALID_TRANSITIONS
        # Let's test an actually invalid transition: OPEN -> OPEN (not in valid set)
        result = tracker.transition_issue(issue.issue_id, IssueStatus.OPEN)
        assert result is None

    def test_resolve_with_details(self, tracker: IssueTracker):
        issue = tracker.create_issue("Bug")
        result = tracker.transition_issue(
            issue.issue_id,
            IssueStatus.RESOLVED,
            root_cause="Null pointer",
            resolution="Added null check",
        )
        assert result is not None
        assert result.root_cause == "Null pointer"
        assert result.resolution == "Added null check"
        assert result.resolved_at is not None

    def test_full_lifecycle(self, tracker: IssueTracker):
        issue = tracker.create_issue("Lifecycle")
        tracker.transition_issue(issue.issue_id, IssueStatus.INVESTIGATING)
        tracker.transition_issue(issue.issue_id, IssueStatus.RESOLVED)
        result = tracker.transition_issue(issue.issue_id, IssueStatus.CLOSED)
        assert result is not None
        assert result.status == IssueStatus.CLOSED

    def test_reopen_resolved(self, tracker: IssueTracker):
        issue = tracker.create_issue("Reopen")
        tracker.transition_issue(issue.issue_id, IssueStatus.RESOLVED)
        result = tracker.transition_issue(issue.issue_id, IssueStatus.OPEN)
        assert result is not None
        assert result.status == IssueStatus.OPEN

    def test_transitions_recorded(self, tracker: IssueTracker):
        issue = tracker.create_issue("Track")
        tracker.transition_issue(issue.issue_id, IssueStatus.INVESTIGATING, transitioned_by="bob")
        transitions = tracker.get_transitions(issue.issue_id)
        assert len(transitions) == 1
        assert transitions[0].transitioned_by == "bob"


class TestAssignmentAndEscalation:
    def test_assign_issue(self, tracker: IssueTracker):
        issue = tracker.create_issue("Assign")
        result = tracker.assign_issue(issue.issue_id, "bob")
        assert result is not None
        assert result.assignee == "bob"

    def test_escalate_issue(self, tracker: IssueTracker):
        issue = tracker.create_issue("Escalate")
        result = tracker.escalate_issue(issue.issue_id)
        assert result is not None
        assert result.escalated is True
        assert result.escalation_level == 1
        result2 = tracker.escalate_issue(issue.issue_id)
        assert result2.escalation_level == 2

    def test_assign_missing(self, tracker: IssueTracker):
        assert tracker.assign_issue("missing", "bob") is None


class TestTaskLinking:
    def test_link_task(self, tracker: IssueTracker):
        issue = tracker.create_issue("Link", related_tasks=["task-1"])
        result = tracker.link_task(issue.issue_id, "task-2")
        assert result is not None
        assert "task-2" in result.related_tasks
        assert "task-1" in result.related_tasks

    def test_link_duplicate_task(self, tracker: IssueTracker):
        issue = tracker.create_issue("Dup", related_tasks=["task-1"])
        result = tracker.link_task(issue.issue_id, "task-1")
        assert len(result.related_tasks) == 1


class TestSLA:
    def test_default_sla(self, tracker: IssueTracker):
        assert tracker.get_sla_hours(IssueSeverity.CRITICAL) == 4
        assert tracker.get_sla_hours(IssueSeverity.LOW) == 168

    def test_custom_sla(self, tracker: IssueTracker):
        tracker.configure_sla(IssueSeverity.CRITICAL, 2)
        assert tracker.get_sla_hours(IssueSeverity.CRITICAL) == 2

    def test_sla_override_on_init(self):
        t = IssueTracker(sla_overrides={IssueSeverity.HIGH: 12})
        assert t.get_sla_hours(IssueSeverity.HIGH) == 12

    def test_issue_age(self, tracker: IssueTracker):
        issue = tracker.create_issue("Age")
        age = tracker.calculate_issue_age_hours(issue.issue_id)
        assert age >= 0.0
        assert age < 1.0  # just created


class TestDashboard:
    def test_dashboard(self, tracker: IssueTracker):
        tracker.create_issue("A", severity=IssueSeverity.CRITICAL, issue_type=IssueType.BLOCKER)
        tracker.create_issue("B", severity=IssueSeverity.LOW, issue_type=IssueType.QUESTION)
        i3 = tracker.create_issue("C")
        tracker.transition_issue(i3.issue_id, IssueStatus.RESOLVED)

        dashboard = tracker.generate_dashboard()
        assert dashboard["total_issues"] == 3
        assert dashboard["open_issues"] == 2
        assert dashboard["resolved_issues"] == 1
        assert dashboard["by_severity"]["critical"] == 1
        assert dashboard["by_type"]["blocker"] == 1
