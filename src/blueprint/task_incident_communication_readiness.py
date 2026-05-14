"""Assess readiness for incident communication execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskIncidentCommunicationReadinessPlan = SimpleReadinessPlan
TaskIncidentCommunicationReadinessRecord = SimpleReadinessRecord
TaskIncidentCommunicationReadinessFinding = SimpleReadinessRecord
TaskIncidentCommunicationReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "incident_communication": re.compile(r"\b(?:incident communication|incident comms|incident notices?|customer incident notices?|post[- ]incident communication)\b", re.I),
    "status_page": re.compile(r"\b(?:status page|status update|public status|incident update)\b", re.I),
    "escalation_broadcast": re.compile(r"\b(?:escalation broadcast|broadcast escalation|customer broadcast|stakeholder broadcast)\b", re.I),
    "incident_notification": re.compile(r"\b(?:incident notification|outage notification|service disruption notice|degradation notice)\b", re.I),
}
_PATH_SIGNALS = {
    "incident_communication": re.compile(r"(?:incident[_-]?comm|incident[_-]?notice|post[_-]?incident|customer[_-]?incident)", re.I),
    "status_page": re.compile(r"(?:status[_-]?page|status[_-]?update|public[_-]?status)", re.I),
    "escalation_broadcast": re.compile(r"(?:escalation[_-]?broadcast|broadcast|stakeholder[_-]?broadcast)", re.I),
    "incident_notification": re.compile(r"(?:incident|notification|notice|outage|degradation)", re.I),
}
_CRITERIA = {
    "audience_segmentation": re.compile(r"\b(?:audience segmentation|customer segment|affected customers?|tenant segment|region segment|internal versus external|stakeholder groups?)\b", re.I),
    "severity_thresholds": re.compile(r"\b(?:severity thresholds?|sev[0-9]|severity level|impact threshold|incident severity|priority threshold)\b", re.I),
    "message_templates": re.compile(r"\b(?:message templates?|template copy|notification template|status template|email template|sms template)\b", re.I),
    "approval_owner": re.compile(r"\b(?:approval|approver|owner|incident commander|communications owner|comms owner|sign[- ]off)\b", re.I),
    "channel_selection": re.compile(r"\b(?:channel selection|status page|email|sms|slack|in[- ]app|webhook|support portal|notification channel)\b", re.I),
    "timing_cadence": re.compile(r"\b(?:timing cadence|update cadence|communication cadence|every \d+ minutes|initial update|follow[- ]up timing|post[- ]incident timing)\b", re.I),
    "localization_accessibility": re.compile(r"\b(?:localization|translation|localized|accessibility|accessible|screen reader|wcag|plain language)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|template tests?|notification tests?|status page tests?)\b", re.I),
}
_GUIDANCE = {
    "audience_segmentation": "Define audience segmentation for affected customers, tenants, regions, and stakeholder groups.",
    "severity_thresholds": "Specify severity thresholds, incident levels, impact thresholds, or priority rules.",
    "message_templates": "Add message, notification, email, SMS, or status-page templates.",
    "approval_owner": "Name the owner, approver, incident commander, communications owner, or sign-off flow.",
    "channel_selection": "Document channel selection across status page, email, SMS, Slack, in-app, webhook, or support portal.",
    "timing_cadence": "Specify initial timing, update cadence, follow-up timing, or post-incident timing.",
    "localization_accessibility": "Cover localization, translation, accessibility, WCAG, screen-reader, or plain-language needs.",
    "tests": "Add unit, integration, template, notification, or status-page tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:incident communication|status page|customer incident notices?|escalation broadcasts?|incident notification)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_incident_communication_readiness_plan(source: Any) -> TaskIncidentCommunicationReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Incident Communication Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_incident_communication_readiness = build_task_incident_communication_readiness_plan
extract_task_incident_communication_readiness = build_task_incident_communication_readiness_plan
generate_task_incident_communication_readiness = build_task_incident_communication_readiness_plan
derive_task_incident_communication_readiness = build_task_incident_communication_readiness_plan
summarize_task_incident_communication_readiness = build_task_incident_communication_readiness_plan
summarize_task_incident_communication_readiness_plan = build_task_incident_communication_readiness_plan


def recommend_task_incident_communication_readiness(source: Any) -> tuple[TaskIncidentCommunicationReadinessRecord, ...]:
    return build_task_incident_communication_readiness_plan(source).records


def task_incident_communication_readiness_plan_to_dict(plan: TaskIncidentCommunicationReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_incident_communication_readiness_plan_to_dict.__test__ = False


def task_incident_communication_readiness_plan_to_dicts(
    plan: TaskIncidentCommunicationReadinessPlan | Iterable[TaskIncidentCommunicationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_incident_communication_readiness_plan_to_dicts.__test__ = False
task_incident_communication_readiness_to_dicts = task_incident_communication_readiness_plan_to_dicts
task_incident_communication_readiness_to_dicts.__test__ = False


def task_incident_communication_readiness_plan_to_markdown(plan: TaskIncidentCommunicationReadinessPlan) -> str:
    return plan.to_markdown()


task_incident_communication_readiness_plan_to_markdown.__test__ = False
