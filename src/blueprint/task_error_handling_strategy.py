"""Analyze error handling strategy readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for error handling strategy concepts
_ERROR_TYPES_RE = re.compile(
    r"\b(?:error[_\s-]+type[s]?|exception[_\s-]+type[s]?|error[_\s-]+class(?:es)?|"
    r"custom[_\s-]+error[s]?|error[_\s-]+hierarch(?:y|ies)|"
    r"(?:validation|runtime|network|timeout|database)[_\s-]+error[s]?|"
    r"error[_\s-]+categor(?:y|ies)|categor(?:ize|ise)[_\s-]+error[s]?|"
    r"typed[_\s-]+exception[s]?|"
    r"(?:recoverable|non[_\s-]+recoverable)[_\s-]+error[s]?|"
    r"test[_\s-]+error[_\s-]+type)\b",
    re.I,
)
_ERROR_CODES_RE = re.compile(
    r"\b(?:error[_\s-]+code[s]?|status[_\s-]+code[s]?|error[_\s-]+enum[s]?|"
    r"error[_\s-]+(?:constant|identifier)[s]?|http[_\s-]+status[_\s-]+code[s]?|"
    r"error[_\s-]+number[s]?|error[_\s-]+id[s]?|"
    r"test[_\s-]+error[_\s-]+code)\b",
    re.I,
)
_RETRY_LOGIC_RE = re.compile(
    r"\b(?:retr(?:y|ies)|retry[_\s-]+(?:logic|strateg(?:y|ies)|polic(?:y|ies)|mechanic[s]?)|"
    r"exponential[_\s-]+backoff|backoff[_\s-]+strateg(?:y|ies)|"
    r"retry[_\s-]+(?:attempt[s]?|count|limit[s]?|delay)|"
    r"max[_\s-]+retr(?:y|ies)|idempotent[_\s-]+retr(?:y|ies)|"
    r"jitter|circuit[_\s-]+breaker|test[_\s-]+retr(?:y|ies))\b",
    re.I,
)
_FALLBACK_MECHANISMS_RE = re.compile(
    r"\b(?:fallback[_\s-]*(?:mechanism[s]?|strateg(?:y|ies)|handler[s]?|value[s]?)?|"
    r"graceful[_\s-]+degradation|default[_\s-]+(?:value[s]?|response|behavior)|"
    r"failover|fallback[_\s-]+mode|backup[_\s-]+(?:option|path)|"
    r"alternative[_\s-]+(?:strateg(?:y|ies)|approach)|"
    r"test[_\s-]+fallback)\b",
    re.I,
)
_ERROR_REPORTING_RE = re.compile(
    r"\b(?:error[_\s-]+reporting|error[_\s-]+(?:logging|tracking)|"
    r"(?:log|report|track|capture)[_\s-]+error[s]?|"
    r"error[_\s-]+(?:monitoring|telemetry|metrics)|"
    r"sentry|bugsnag|rollbar|error[_\s-]+aggregation|"
    r"exception[_\s-]+(?:logging|tracking|reporting)|"
    r"test[_\s-]+error[_\s-]+reporting)\b",
    re.I,
)
_SILENT_FAILURES_RE = re.compile(
    r"\b(?:silent[_\s-]+failure[s]?|(?:suppress|swallow(?:ed|ing)?|ignore)[_\s-]+(?:error[s]?|exception[s]?)|"
    r"empty[_\s-]+catch|bare[_\s-]+except|catch[_\s-]+all|"
    r"error[_\s-]+suppression|unhandled[_\s-]+(?:error[s]?|exception[s]?)|"
    r"(?:prevent|avoid|detect)[_\s-]+silent[_\s-]+failure[s]?|"
    r"test[_\s-]+(?:silent[_\s-]+failure|unhandled[_\s-]+error))\b",
    re.I,
)
_ERROR_PROPAGATION_RE = re.compile(
    r"\b(?:error[_\s-]+propagation|propagate[_\s-]+error[s]?|"
    r"error[s]?[_\s-]+(?:bubbl(?:e|ing|e[_\s-]+up|ed?)|chaining|wrapping|context)|"
    r"(?:throw|raise|rethrow)[_\s-]+(?:error[s]?|exception[s]?)|"
    r"error[_\s-]+handling[_\s-]+chain|nested[_\s-]+(?:error[s]?|exception[s]?)|"
    r"error[_\s-]+(?:cause|root[_\s-]+cause)|"
    r"wrap[_\s-]+error[s]?|bubbl(?:e|ing|ed?)[_\s-]+up|"
    r"test[_\s-]+error[_\s-]+propagation)\b",
    re.I,
)
_USER_FACING_MESSAGES_RE = re.compile(
    r"\b(?:user[_\s-]+(?:facing|friendly)[_\s-]+(?:error|message)[s]?|"
    r"error[_\s-]+message[s]?[_\s-]+(?:for[_\s-]+)?user[s]?|"
    r"(?:display|show|present)[_\s-]+error[_\s-]+(?:to[_\s-]+)?user[s]?|"
    r"user[_\s-]+(?:readable|understandable)[_\s-]+(?:error|message)[s]?|"
    r"(?:localized|internationalized)[_\s-]+error[_\s-]+message[s]?|"
    r"error[_\s-]+(?:notification[s]?|alert[s]?)|"
    r"test[_\s-]+(?:user[_\s-]+message|error[_\s-]+message))\b",
    re.I,
)
_LOGGING_COMPLETENESS_RE = re.compile(
    r"\b(?:(?:log|logging)[_\s-]+(?:error[s]?|exception[s]?|failure[s]?|metadata|context|completeness)|"
    r"error[_\s-]+log(?:s|ging)?|structured[_\s-]+log(?:s|ging)?|"
    r"log[_\s-]+(?:level[s]?|context|metadata|stack[_\s-]+trace[s]?)|"
    r"(?:debug|info|warn|error)[_\s-]+log(?:s|ging)?|"
    r"logging[_\s-]+(?:strateg(?:y|ies)|framework|library|completeness)|"
    r"stack[_\s-]+trace[s]?|"
    r"(?:include|add)[_\s-]+(?:context|metadata)|"
    r"test[_\s-]+(?:logging|log[_\s-]+output))\b",
    re.I,
)
_RECOVERY_PROCEDURES_RE = re.compile(
    r"\b(?:recover(?:y|ing)[_\s-]+procedure[s]?|error[_\s-]+recover(?:y|ing)|"
    r"recover[_\s-]+from[_\s-]+(?:error|failure)[s]?|"
    r"(?:auto|automatic)[_\s-]+recover(?:y|ing)|self[_\s-]+healing|"
    r"(?:rollback[s]?|undo)[_\s-]*(?:on[_\s-]+)?(?:error|failure)?|"
    r"error[_\s-]+(?:compensation|remediation)|cleanup[_\s-]*(?:on[_\s-]+)?(?:error[s]?|failure[s]?)?|"
    r"test[_\s-]+(?:recover(?:y|ing)|rollback))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ErrorHandlingStrategyReadiness:
    """Error handling strategy readiness analysis for a task."""

    error_types_defined: bool = False
    error_codes_specified: bool = False
    retry_logic_implemented: bool = False
    fallback_mechanisms_provided: bool = False
    error_reporting_configured: bool = False
    silent_failures_prevented: bool = False
    error_propagation_handled: bool = False
    user_facing_messages_designed: bool = False
    logging_completeness_ensured: bool = False
    recovery_procedures_documented: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        # Coverage breadth (critical for error taxonomy)
        coverage_checks = [
            self.error_types_defined,
            self.error_codes_specified,
            self.silent_failures_prevented,
            self.error_propagation_handled,
        ]

        # User experience (critical for UX)
        ux_checks = [
            self.user_facing_messages_designed,
            self.fallback_mechanisms_provided,
        ]

        # Debugging support (critical for operations)
        debugging_checks = [
            self.logging_completeness_ensured,
            self.error_reporting_configured,
        ]

        # Monitoring integration (important for production)
        monitoring_checks = [
            self.retry_logic_implemented,
            self.recovery_procedures_documented,
        ]

        # Weight: coverage=30%, ux=25%, debugging=25%, monitoring=20%
        coverage_score = sum(coverage_checks) / len(coverage_checks) * 0.3
        ux_score = sum(ux_checks) / len(ux_checks) * 0.25
        debugging_score = sum(debugging_checks) / len(debugging_checks) * 0.25
        monitoring_score = sum(monitoring_checks) / len(monitoring_checks) * 0.2

        return coverage_score + ux_score + debugging_score + monitoring_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "error_types_defined": self.error_types_defined,
            "error_codes_specified": self.error_codes_specified,
            "retry_logic_implemented": self.retry_logic_implemented,
            "fallback_mechanisms_provided": self.fallback_mechanisms_provided,
            "error_reporting_configured": self.error_reporting_configured,
            "silent_failures_prevented": self.silent_failures_prevented,
            "error_propagation_handled": self.error_propagation_handled,
            "user_facing_messages_designed": self.user_facing_messages_designed,
            "logging_completeness_ensured": self.logging_completeness_ensured,
            "recovery_procedures_documented": self.recovery_procedures_documented,
            "readiness_score": self.readiness_score,
        }


def analyze_error_handling_strategy_readiness(task_data: Mapping[str, Any]) -> ErrorHandlingStrategyReadiness:
    """
    Analyze error handling strategy readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        ErrorHandlingStrategyReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return ErrorHandlingStrategyReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return ErrorHandlingStrategyReadiness(
        error_types_defined=bool(_ERROR_TYPES_RE.search(searchable_text)),
        error_codes_specified=bool(_ERROR_CODES_RE.search(searchable_text)),
        retry_logic_implemented=bool(_RETRY_LOGIC_RE.search(searchable_text)),
        fallback_mechanisms_provided=bool(_FALLBACK_MECHANISMS_RE.search(searchable_text)),
        error_reporting_configured=bool(_ERROR_REPORTING_RE.search(searchable_text)),
        silent_failures_prevented=bool(_SILENT_FAILURES_RE.search(searchable_text)),
        error_propagation_handled=bool(_ERROR_PROPAGATION_RE.search(searchable_text)),
        user_facing_messages_designed=bool(_USER_FACING_MESSAGES_RE.search(searchable_text)),
        logging_completeness_ensured=bool(_LOGGING_COMPLETENESS_RE.search(searchable_text)),
        recovery_procedures_documented=bool(_RECOVERY_PROCEDURES_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "ErrorHandlingStrategyReadiness",
    "analyze_error_handling_strategy_readiness",
]
