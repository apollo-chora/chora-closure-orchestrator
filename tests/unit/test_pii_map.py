"""PII_Closure_Map.yaml loader + validator tests.

Each domain ships a ``PII_Closure_Map.yaml`` declaring fields-to-tokenize,
retention rules, and on-creator-closure behaviour. The loader is shared
machinery used by per-domain pseudonymisation subscribers (each domain
owns its file; this loader is the canonical reader).
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from chora_closure_orchestrator.adapter.pii_map import (
    InvalidPIIMapError,
    PIIClosureMap,
    PIIFieldRule,
    load_pii_map,
    load_pii_map_from_file,
)

VALID_YAML = textwrap.dedent(
    """\
    domain: chora_creation
    version: 1.0
    fields_to_tokenize:
      - table: learning_atom
        columns:
          - column: created_by_display_name
            strategy: tombstone_string
            value: "Former member"
          - column: created_by_email_snapshot
            strategy: tombstone_email
            value: "user-{hash}@redacted.invalid"
      - table: atom_revision
        columns:
          - column: edited_by_display_name
            strategy: tombstone_string
            value: "Former member"

    retention_days_by_jurisdiction:
      EU: 2557
      SG: 2557
      US: 1826
      default: 2557

    on_creator_closure:
      strategy: tokenise_authorship_keep_atom
      show_authorship_as: "Former member"
    """
)


class TestLoadPIIMap:
    def test_parses_valid_yaml(self) -> None:
        m = load_pii_map(VALID_YAML)
        assert isinstance(m, PIIClosureMap)
        assert m.domain == "chora_creation"
        assert m.version == "1.0"

    def test_field_rules_indexed_by_table(self) -> None:
        m = load_pii_map(VALID_YAML)
        atom_rules = m.fields_for_table("learning_atom")
        assert len(atom_rules) == 2
        assert all(isinstance(r, PIIFieldRule) for r in atom_rules)

    def test_field_rule_strategies(self) -> None:
        m = load_pii_map(VALID_YAML)
        rules = m.fields_for_table("learning_atom")
        first = rules[0]
        assert first.column == "created_by_display_name"
        assert first.strategy == "tombstone_string"
        assert first.value == "Former member"

    def test_retention_lookup(self) -> None:
        m = load_pii_map(VALID_YAML)
        assert m.retention_days("SG") == 2557
        assert m.retention_days("US") == 1826

    def test_retention_unknown_uses_default(self) -> None:
        m = load_pii_map(VALID_YAML)
        # Unknown jurisdiction → use default
        assert m.retention_days("ZA") == 2557

    def test_on_creator_closure_strategy(self) -> None:
        m = load_pii_map(VALID_YAML)
        assert m.on_creator_closure_strategy == "tokenise_authorship_keep_atom"
        assert m.on_creator_closure_show_as == "Former member"

    def test_missing_domain_field_raises(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map("version: 1.0\nfields_to_tokenize: []\n")

    def test_unknown_strategy_raises(self) -> None:
        bad = textwrap.dedent(
            """\
            domain: chora_creation
            version: 1.0
            fields_to_tokenize:
              - table: t
                columns:
                  - column: c
                    strategy: nuke_from_orbit
                    value: x
            retention_days_by_jurisdiction:
              default: 2557
            on_creator_closure:
              strategy: tokenise_authorship_keep_atom
              show_authorship_as: anon
            """
        )
        with pytest.raises(InvalidPIIMapError):
            load_pii_map(bad)

    def test_invalid_yaml_raises(self) -> None:
        with pytest.raises(InvalidPIIMapError):
            load_pii_map("---\nfields_to_tokenize: [unbalanced\n")

    def test_load_from_file(self, tmp_path: Path) -> None:
        f = tmp_path / "PII_Closure_Map.yaml"
        f.write_text(VALID_YAML)
        m = load_pii_map_from_file(f)
        assert m.domain == "chora_creation"


class TestPIIMapShippedFixture:
    """The loader is shared machinery used by per-domain pseudonymisation
    subscribers; this repo ships one canonical fixture map (copied from
    the observability domain) that must exist + parse."""

    FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "PII_Closure_Map.yaml"

    def test_fixture_map_exists(self) -> None:
        assert self.FIXTURE.exists(), f"Missing fixture: {self.FIXTURE}"

    def test_fixture_map_parses(self) -> None:
        m = load_pii_map_from_file(self.FIXTURE)
        # Every map must declare its owning domain explicitly.
        assert m.domain != ""
        assert m.version != ""

    def test_fixture_map_declares_retention(self) -> None:
        m = load_pii_map_from_file(self.FIXTURE)
        # SG/EU floor is 2557 days; the fixture must never fall below it.
        assert m.retention_days("SG") >= 2557

    def test_fixture_map_declares_on_creator_closure(self) -> None:
        m = load_pii_map_from_file(self.FIXTURE)
        assert m.on_creator_closure_strategy != ""
