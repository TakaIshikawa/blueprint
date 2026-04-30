from pathlib import Path

from blueprint.codeowners import (
    CodeownersRule,
    load_codeowners,
    parse_codeowners,
    resolve_codeowners,
)


def test_parse_codeowners_ignores_comments_blank_lines_and_ownerless_patterns():
    rules = parse_codeowners(
        """
        # Default ownership

        * @team/all
        docs/
        /src/blueprint/ @team/blueprint @team/runtime
        """
    )

    assert rules == [
        CodeownersRule(pattern="*", owners=("@team/all",), line_number=4),
        CodeownersRule(
            pattern="/src/blueprint/",
            owners=("@team/blueprint", "@team/runtime"),
            line_number=6,
        ),
    ]


def test_load_codeowners_reads_utf8_file(tmp_path: Path):
    codeowners_path = tmp_path / "CODEOWNERS"
    codeowners_path.write_text("*.py @python-team\n", encoding="utf-8")

    assert load_codeowners(codeowners_path) == [
        CodeownersRule(pattern="*.py", owners=("@python-team",), line_number=1)
    ]


def test_resolve_codeowners_uses_last_matching_rule_precedence():
    rules = parse_codeowners(
        """
        * @team/all
        *.py @team/python
        /src/blueprint/codeowners.py @team/ownership
        """
    )

    assert resolve_codeowners(
        ["src/blueprint/codeowners.py", "README.md"],
        rules,
    ) == {
        "src/blueprint/codeowners.py": ["@team/ownership"],
        "README.md": ["@team/all"],
    }


def test_resolve_codeowners_returns_empty_list_for_unmatched_files():
    rules = parse_codeowners("/docs/ @docs-team\n")

    assert resolve_codeowners(["src/app.py"], rules) == {"src/app.py": []}


def test_resolve_codeowners_matches_directory_patterns_recursively():
    rules = parse_codeowners(
        """
        /docs/ @docs-team
        src/blueprint/ @core-team
        """
    )

    assert resolve_codeowners(
        ["docs/index.md", "docs/guides/install.md", "src/blueprint/cli.py"],
        rules,
    ) == {
        "docs/index.md": ["@docs-team"],
        "docs/guides/install.md": ["@docs-team"],
        "src/blueprint/cli.py": ["@core-team"],
    }


def test_resolve_codeowners_matches_simple_globs_and_path_globs():
    rules = parse_codeowners(
        """
        *.md @docs-team
        src/**/*.py @python-team
        tests/test_*.py @qa-team
        """
    )

    assert resolve_codeowners(
        [
            "README.md",
            "src/blueprint/codeowners.py",
            "src/blueprint/audits/file_contention.py",
            "tests/test_codeowners.py",
        ],
        rules,
    ) == {
        "README.md": ["@docs-team"],
        "src/blueprint/codeowners.py": ["@python-team"],
        "src/blueprint/audits/file_contention.py": ["@python-team"],
        "tests/test_codeowners.py": ["@qa-team"],
    }


def test_resolve_codeowners_handles_task_style_files_or_modules_lists():
    files_or_modules = [
        "./src/blueprint/codeowners.py",
        "/src/blueprint/audits/file_contention.py",
        "tests/test_codeowners.py",
        "docs/",
        "unknown/module",
    ]
    rules = parse_codeowners(
        """
        src/blueprint/ @core-team
        tests/ @test-team
        docs/ @docs-team
        src/blueprint/audits/ @audit-team @reviewer
        """
    )

    assert resolve_codeowners(files_or_modules, rules) == {
        "src/blueprint/codeowners.py": ["@core-team"],
        "src/blueprint/audits/file_contention.py": ["@audit-team", "@reviewer"],
        "tests/test_codeowners.py": ["@test-team"],
        "docs": ["@docs-team"],
        "unknown/module": [],
    }
