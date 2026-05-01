"""Tests for the exporter registry."""

from __future__ import annotations

import pytest

from blueprint.cli import EXPORT_SINGLE_TARGET_CHOICES, EXPORT_TARGET_CHOICES
from blueprint.exporters.registry import (
    create_exporter,
    get_exporter_registration,
    resolve_target_name,
    supported_target_aliases,
    supported_target_names,
)


def test_all_supported_targets_create_exporters_with_default_metadata():
    for target in supported_target_names():
        registration = get_exporter_registration(target)
        exporter = create_exporter(target)

        assert registration.target == target
        assert registration.default_format == exporter.get_format()
        assert registration.extension == exporter.get_extension()


def test_aliases_resolve_to_canonical_target_and_factory():
    canonical = get_exporter_registration("claude-code")
    alias = get_exporter_registration("claude_code")

    assert supported_target_aliases()["claude_code"] == "claude-code"
    assert resolve_target_name("claude_code") == "claude-code"
    assert alias == canonical
    assert create_exporter("claude_code").__class__ is create_exporter("claude-code").__class__

    assert supported_target_aliases()["pagerduty_digest"] == "pagerduty-digest"
    assert resolve_target_name("pagerduty_digest") == "pagerduty-digest"
    assert (
        create_exporter("pagerduty_digest").__class__
        is create_exporter("pagerduty-digest").__class__
    )


def test_unknown_target_lookup_raises_value_error():
    with pytest.raises(ValueError, match="Unknown export target: nope"):
        get_exporter_registration("nope")

    with pytest.raises(ValueError, match="Unknown export target: nope"):
        create_exporter("nope")


def test_cli_target_choices_are_derived_from_registry():
    assert EXPORT_SINGLE_TARGET_CHOICES == supported_target_names(include_aliases=True)
    assert EXPORT_TARGET_CHOICES == (*supported_target_names(include_aliases=True), "all")
