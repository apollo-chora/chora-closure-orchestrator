"""Additional PII_Closure_Map.yaml loader tests for branch coverage."""

from __future__ import annotations

import textwrap

import pytest

from chora_closure_orchestrator.adapter.pii_map import (
    InvalidPIIMapError,
    load_pii_map,
)


class TestLoaderEdgeCases:
    def test_top_level_must_be_mapping(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map("- list\n- top\n")

    def test_missing_version_raises(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(
                "domain: chora_x\nfields_to_tokenize: []\n"
                "retention_days_by_jurisdiction:\n  default: 2557\n"
                "on_creator_closure:\n  strategy: tokenise_authorship_keep_atom\n"
                "  show_authorship_as: x\n"
            )

    def test_fields_must_be_list(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(
                "domain: x\nversion: 1\nfields_to_tokenize: not-a-list\n"
                "retention_days_by_jurisdiction:\n  default: 2557\n"
                "on_creator_closure:\n  strategy: tokenise_authorship_keep_atom\n"
            )

    def test_field_entry_must_be_mapping(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(
                "domain: x\nversion: 1\nfields_to_tokenize:\n  - just-a-string\n"
                "retention_days_by_jurisdiction:\n  default: 2557\n"
                "on_creator_closure:\n  strategy: tokenise_authorship_keep_atom\n"
            )

    def test_field_table_required(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(
                "domain: x\nversion: 1\nfields_to_tokenize:\n  - columns: []\n"
                "retention_days_by_jurisdiction:\n  default: 2557\n"
                "on_creator_closure:\n  strategy: tokenise_authorship_keep_atom\n"
            )

    def test_columns_must_be_list(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize:
              - table: t
                columns: not-a-list
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_column_entry_must_be_mapping(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize:
              - table: t
                columns:
                  - just-a-string
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_column_missing_column_name_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize:
              - table: t
                columns:
                  - strategy: drop
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_column_missing_strategy_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize:
              - table: t
                columns:
                  - column: c
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_retention_must_be_mapping(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize: []
            retention_days_by_jurisdiction: not-a-mapping
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_retention_value_not_int_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize: []
            retention_days_by_jurisdiction:
              SG: not-a-number
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_on_creator_must_be_mapping(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize: []
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure: not-a-mapping
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_on_creator_missing_strategy_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize: []
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              show_authorship_as: x
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_unknown_on_creator_strategy_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: x
            version: 1
            fields_to_tokenize: []
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: nuke
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_fields_for_unknown_table_returns_empty(self) -> None:
        m = load_pii_map(
            textwrap.dedent(
                """\
                domain: x
                version: 1
                fields_to_tokenize:
                  - table: known
                    columns:
                      - column: c
                        strategy: drop
                retention_days_by_jurisdiction:
                  default: 2557
                on_creator_closure:
                  strategy: tokenise_authorship_keep_atom
                """
            )
        )
        assert m.fields_for_table("unknown") == []
