"""Assess readiness for customer communication planning tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskCustomerCommunicationReadinessFinding = SimpleReadinessRecord
TaskCustomerCommunicationReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "customer_notice": re.compile(
        r"\b(?:customer notice|customer notification|notify customers?|customer comms?|"
        r"customer communication|customer-facing communication|customer email|email customers?|"
        r"advance notice|notice to customers?)\b",
        re.I,
    ),
    "migration_notice": re.compile(
        r"\b(?:migration notice|migration communication|migration comms?|migration email|"
        r"cutover notice|cutover communication|notify customers? about (?:the )?migration|"
        r"migration announcement)\b",
        re.I,
    ),
    "status_page_update": re.compile(
        r"\b(?:status page|statuspage|incident status update|maintenance status update|"
        r"public status update|status update for customers?)\b",
        re.I,
    ),
    "release_communication": re.compile(
        r"\b(?:release communication|release comms?|release announcement|launch announcement|"
        r"customer release notes?|external release notes?|go-live communication|launch email)\b",
        re.I,
    ),
    "support_bulletin": re.compile(
        r"\b(?:support bulletin|support advisory|support announcement|support notice|"
        r"customer support bulletin|agent bulletin|support-facing bulletin)\b",
        re.I,
    ),
    "in_app_announcement": re.compile(
        r"\b(?:in-app announcement|in app announcement|in-app message|in app message|"
        r"product announcement banner|announcement banner|user announcement|customer banner)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "customer_notice": re.compile(r"customer[-_ ]?(?:notice|notification|comms?|email)", re.I),
    "migration_notice": re.compile(r"migration[-_ ]?(?:notice|comms?|email|announcement)|cutover[-_ ]?notice", re.I),
    "status_page_update": re.compile(r"status[-_ ]?page|statuspage|public[-_ ]?status", re.I),
    "release_communication": re.compile(r"release[-_ ]?(?:comms?|communication|announcement)|launch[-_ ]?(?:comms?|email)", re.I),
    "support_bulletin": re.compile(r"support[-_ ]?(?:bulletin|advisory|notice|announcement)", re.I),
    "in_app_announcement": re.compile(r"in[-_ ]?app[-_ ]?(?:announcement|message)|announcement[-_ ]?banner", re.I),
}
_CRITERIA = {
    "audience_segmentation": re.compile(
        r"\b(?:audience segmentation|segment(?:ed|s)? customers?|customer segment|affected customers?|"
        r"target audience|recipient list|customer cohort|account tier|region segment)\b",
        re.I,
    ),
    "message_timing": re.compile(
        r"\b(?:message timing|communication timing|send time|notice period|advance notice|"
        r"schedule(?:d)? (?:notice|send|communication)|timing window|before cutover|after release)\b",
        re.I,
    ),
    "owner_approval": re.compile(
        r"\b(?:owner approval|approval owner|approved by|comms owner|communication owner|"
        r"marketing approval|support approval|legal approval|product owner approval|sign[- ]off)\b",
        re.I,
    ),
    "channel_coverage": re.compile(
        r"\b(?:channel coverage|communication channels?|email and in-app|email,? status page|"
        r"status page and support|support channel|in-app and email|multi[- ]channel|"
        r"channels?: .*(?:email|in-app|status page|support))\b",
        re.I,
    ),
    "rollback_update_messaging": re.compile(
        r"\b(?:rollback messaging|rollback communication|rollback update|follow[- ]up update|"
        r"update messaging|correction notice|delay notice|incident update|post[- ]rollback|"
        r"if rollback|if delayed)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "audience_segmentation": "Define affected customer segments, cohorts, or recipient lists.",
    "message_timing": "Specify communication timing, notice period, and send schedule.",
    "owner_approval": "Name the approval owner and required sign-off path.",
    "channel_coverage": "List all customer communication channels and coverage expectations.",
    "rollback_update_messaging": "Prepare rollback, delay, correction, or follow-up update messaging.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:customer communication|customer comms?|customer notice|"
    r"migration notice|status page|release communication|support bulletin|in-app announcement)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_customer_communication_readiness_plan(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Customer Communication Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def extract_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def generate_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def derive_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def summarize_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def recommend_task_customer_communication_readiness(source: Any) -> TaskCustomerCommunicationReadinessPlan:
    return build_task_customer_communication_readiness_plan(source)


def task_customer_communication_readiness_plan_to_dict(report: TaskCustomerCommunicationReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_customer_communication_readiness_plan_to_dict.__test__ = False


def task_customer_communication_readiness_plan_to_dicts(report: TaskCustomerCommunicationReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_customer_communication_readiness_plan_to_dicts.__test__ = False


def task_customer_communication_readiness_plan_to_markdown(report: TaskCustomerCommunicationReadinessPlan) -> str:
    return report.to_markdown()


task_customer_communication_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskCustomerCommunicationReadinessFinding",
    "TaskCustomerCommunicationReadinessPlan",
    "analyze_task_customer_communication_readiness",
    "build_task_customer_communication_readiness_plan",
    "derive_task_customer_communication_readiness",
    "extract_task_customer_communication_readiness",
    "generate_task_customer_communication_readiness",
    "recommend_task_customer_communication_readiness",
    "summarize_task_customer_communication_readiness",
    "task_customer_communication_readiness_plan_to_dict",
    "task_customer_communication_readiness_plan_to_dicts",
    "task_customer_communication_readiness_plan_to_markdown",
]
