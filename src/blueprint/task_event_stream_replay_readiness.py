"""Assess readiness for event stream replay and catch-up tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskEventStreamReplayReadinessFinding = SimpleReadinessRecord
TaskEventStreamReplayReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "event_replay": re.compile(r"\b(?:event replay|replay events?|replay stream|message replay|request replay)\b", re.I),
    "offset_reset": re.compile(r"\b(?:offset reset|reset offsets?|seek offset|rewind offset|checkpoint reset)\b", re.I),
    "stream_catchup": re.compile(r"\b(?:stream catch[- ]?up|catch up stream|consumer catch[- ]?up|catchup job)\b", re.I),
    "consumer_lag": re.compile(r"\b(?:consumer lag|lag recovery|lagging consumer|behind the stream|backlog drain)\b", re.I),
    "event_backfill": re.compile(r"\b(?:event backfill|stream backfill|historical event|backfill events?)\b", re.I),
    "reprocessing": re.compile(r"\b(?:reprocess|reprocessing|rerun events?|re-ingest|reingest)\b", re.I),
}
_PATH_SIGNALS = {
    "event_replay": re.compile(r"replay|events?|messages?", re.I),
    "offset_reset": re.compile(r"offsets?|checkpoints?|cursors?", re.I),
    "stream_catchup": re.compile(r"streams?|catch[-_]?up|catchup", re.I),
    "consumer_lag": re.compile(r"consumers?|lag|backlog", re.I),
    "event_backfill": re.compile(r"backfill|historical", re.I),
    "reprocessing": re.compile(r"reprocess|re[-_]?ingest|rerun", re.I),
}
_CRITERIA = {
    "replay_scope": re.compile(r"\b(?:replay scope|scope|topic|partition|time window|from timestamp|tenant|cohort|event range|start and end)\b", re.I),
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplicate|duplicate safe|exactly once|side effect safe)\b", re.I),
    "ordering": re.compile(r"\b(?:ordering|ordered|event order|sequence|partition order|causal order|out of order)\b", re.I),
    "checkpoint_handling": re.compile(r"\b(?:checkpoint|offset|cursor|watermark|resume point|commit offset|seek position)\b", re.I),
    "monitoring_abort": re.compile(r"\b(?:monitoring|monitor|metrics|alerts?|dashboard|abort|kill switch|stop condition|rollback|pause replay)\b", re.I),
}
_GUIDANCE = {
    "replay_scope": "Define replay scope by topic, partition, tenant, cohort, and time window.",
    "idempotency": "Document idempotency or deduplication safeguards for repeated events.",
    "ordering": "Specify ordering guarantees and handling for out-of-order replay.",
    "checkpoint_handling": "Plan checkpoint, offset, cursor, and resume behavior.",
    "monitoring_abort": "Add monitoring, alerts, and abort conditions for the replay run.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:event replay|stream replay|offset reset|event backfill|reprocessing|consumer lag)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_event_stream_replay_readiness_plan(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Event Stream Replay Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def extract_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def generate_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def derive_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def summarize_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def recommend_task_event_stream_replay_readiness(source: Any) -> TaskEventStreamReplayReadinessPlan:
    return build_task_event_stream_replay_readiness_plan(source)


def task_event_stream_replay_readiness_plan_to_dict(report: TaskEventStreamReplayReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_event_stream_replay_readiness_plan_to_dict.__test__ = False


def task_event_stream_replay_readiness_plan_to_dicts(report: TaskEventStreamReplayReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_event_stream_replay_readiness_plan_to_dicts.__test__ = False


def task_event_stream_replay_readiness_plan_to_markdown(report: TaskEventStreamReplayReadinessPlan) -> str:
    return report.to_markdown()


task_event_stream_replay_readiness_plan_to_markdown.__test__ = False
