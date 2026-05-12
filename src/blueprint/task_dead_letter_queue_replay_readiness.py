"""Assess readiness for dead-letter queue replay tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDeadLetterQueueReplayReadinessFinding = SimpleReadinessRecord
TaskDeadLetterQueueReplayReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "dead_letter_queue": re.compile(r"\b(?:dead[- ]?letter queue|dead letter topic|dlq|failed message queue)\b", re.I),
    "failed_message_replay": re.compile(r"\b(?:failed message replay|replay failed|redrive|replay dlq|dlq replay)\b", re.I),
    "queue_reprocessing": re.compile(r"\b(?:queue reprocess|queue reprocessing|reprocess messages?|reprocessing failed|rerun messages?)\b", re.I),
    "message_replay": re.compile(r"\b(?:message replay|replay messages?|replay queue|backfill messages?)\b", re.I),
}
_PATH_SIGNALS = {
    "dead_letter_queue": re.compile(r"dead[-_]?letter|dlq|failed[-_]?message", re.I),
    "failed_message_replay": re.compile(r"redrive|replay[-_]?failed|dlq[-_]?replay", re.I),
    "queue_reprocessing": re.compile(r"reprocess|rerun|queue", re.I),
    "message_replay": re.compile(r"replay|messages?", re.I),
}
_CRITERIA = {
    "replay_scope_filtering": re.compile(r"\b(?:scope|filter|selection|tenant|message id|event type|time window|from timestamp|batch size|sample)\b", re.I),
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplicate|duplicate protection|duplicate safe|exactly once)\b", re.I),
    "poison_message_handling": re.compile(r"\b(?:poison message|poison pill|quarantine|skip invalid|terminal failure|malformed|bad payload)\b", re.I),
    "rate_limiting": re.compile(r"\b(?:rate limit|throttle|throttling|pace|batch delay|concurrency limit|qps|backpressure)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit|audit trail|evidence|replay log|operator log|who ran|approval|receipt)\b", re.I),
    "rollback_stop_control": re.compile(r"\b(?:rollback|stop control|kill switch|abort|pause|resume|dry run|cancel)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitoring|monitor|metrics|dashboard|alert|alarm|progress|failure rate|dlq depth)\b", re.I),
}
_GUIDANCE = {
    "replay_scope_filtering": "Define replay selection, filters, tenant/message boundaries, and batch limits.",
    "idempotency": "Document idempotency or duplicate protection before replaying side-effecting messages.",
    "poison_message_handling": "Describe how poison messages are skipped, quarantined, or terminally failed.",
    "rate_limiting": "Add throttling, concurrency limits, and pacing to protect downstream systems.",
    "audit_trail": "Record operator, approval, selected messages, outcomes, and replay evidence.",
    "rollback_stop_control": "Provide dry-run, pause, abort, rollback, or stop controls for replay runs.",
    "monitoring": "Monitor replay progress, failure rates, queue depth, and downstream health.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:dlq|dead[- ]?letter|failed message replay|queue reprocessing)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_dead_letter_queue_replay_readiness_plan(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Dead Letter Queue Replay Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def extract_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def generate_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def derive_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def summarize_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def recommend_task_dead_letter_queue_replay_readiness(source: Any) -> TaskDeadLetterQueueReplayReadinessPlan:
    return build_task_dead_letter_queue_replay_readiness_plan(source)


def task_dead_letter_queue_replay_readiness_plan_to_dict(report: TaskDeadLetterQueueReplayReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_dead_letter_queue_replay_readiness_plan_to_dict.__test__ = False


def task_dead_letter_queue_replay_readiness_plan_to_dicts(report: TaskDeadLetterQueueReplayReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_dead_letter_queue_replay_readiness_plan_to_dicts.__test__ = False


def task_dead_letter_queue_replay_readiness_plan_to_markdown(report: TaskDeadLetterQueueReplayReadinessPlan) -> str:
    return report.to_markdown()


task_dead_letter_queue_replay_readiness_plan_to_markdown.__test__ = False
