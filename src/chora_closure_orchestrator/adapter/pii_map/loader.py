"""PII_Closure_Map.yaml loader + validator.

Schema (see .claude/skills/account-closure-saga/SKILL.md):

.. code-block:: yaml

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

    retention_days_by_jurisdiction:
      EU: 2557
      SG: 2557
      US: 1826
      default: 2557

    on_creator_closure:
      strategy: tokenise_authorship_keep_atom
      show_authorship_as: "Former member"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Recognised pseudonymisation strategies (per skill).
_VALID_STRATEGIES = frozenset(
    {
        "tombstone_string",
        "tombstone_email",
        "hash",
        "drop",
        "preserve",
        "encrypt",
        "tokenize",
    }
)

# Recognised on-creator-closure strategies (per skill).
_VALID_ON_CREATOR = frozenset(
    {
        "tokenise_authorship_keep_atom",
        "soft_delete_authored_atoms",
        "transfer_authorship_to_tenant",
        "cascade_pseudonymise",
        "preserve_orphan",
    }
)


class InvalidPIIMapError(ValueError):
    """Raised when a PII_Closure_Map.yaml fails validation."""


@dataclass(frozen=True)
class PIIFieldRule:
    """A single column-level pseudonymisation rule."""

    column: str
    strategy: str
    value: str = ""


@dataclass(frozen=True)
class PIITableRules:
    """All rules for a single table."""

    table: str
    columns: list[PIIFieldRule] = field(default_factory=list)


@dataclass
class PIIClosureMap:
    """Top-level PII_Closure_Map.yaml structure."""

    domain: str
    version: str
    table_rules: list[PIITableRules]
    retention_by_jurisdiction: dict[str, int]
    on_creator_closure_strategy: str
    on_creator_closure_show_as: str

    def fields_for_table(self, table: str) -> list[PIIFieldRule]:
        for tr in self.table_rules:
            if tr.table == table:
                return list(tr.columns)
        return []

    def retention_days(self, jurisdiction: str) -> int:
        """Return retention days for the jurisdiction (case-insensitive),
        falling back to the ``default`` key if absent."""
        upper = jurisdiction.upper()
        if upper in self.retention_by_jurisdiction:
            return self.retention_by_jurisdiction[upper]
        # Try the default fallback
        return self.retention_by_jurisdiction.get("default", 2557)


def load_pii_map(yaml_text: str) -> PIIClosureMap:
    """Parse + validate ``yaml_text`` into a PIIClosureMap.

    :raises InvalidPIIMapError: any required field missing or invalid.
    """
    try:
        data: Any = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise InvalidPIIMapError(f"yaml parse error: {exc}") from exc

    if not isinstance(data, dict):
        raise InvalidPIIMapError("top-level must be a mapping")

    domain = data.get("domain")
    if not domain or not isinstance(domain, str):
        raise InvalidPIIMapError("'domain' is required")

    version = data.get("version")
    if version is None:
        raise InvalidPIIMapError("'version' is required")
    version_str = str(version)

    fields = data.get("fields_to_tokenize") or []
    if not isinstance(fields, list):
        raise InvalidPIIMapError("'fields_to_tokenize' must be a list")

    table_rules: list[PIITableRules] = []
    for entry in fields:
        if not isinstance(entry, dict):
            raise InvalidPIIMapError("each fields_to_tokenize item must be a mapping")
        table = entry.get("table")
        if not table or not isinstance(table, str):
            raise InvalidPIIMapError("each fields_to_tokenize item needs 'table'")
        columns_raw = entry.get("columns") or []
        if not isinstance(columns_raw, list):
            raise InvalidPIIMapError("'columns' must be a list")
        columns: list[PIIFieldRule] = []
        for col in columns_raw:
            if not isinstance(col, dict):
                raise InvalidPIIMapError("each column entry must be a mapping")
            col_name = col.get("column")
            strategy = col.get("strategy")
            value = col.get("value", "")
            if not col_name:
                raise InvalidPIIMapError(f"column missing 'column' field in table {table!r}")
            if not strategy:
                raise InvalidPIIMapError(f"column {col_name!r} missing 'strategy'")
            if strategy not in _VALID_STRATEGIES:
                raise InvalidPIIMapError(
                    f"unknown strategy {strategy!r} for column {col_name!r}; valid: {sorted(_VALID_STRATEGIES)}"
                )
            columns.append(
                PIIFieldRule(
                    column=str(col_name),
                    strategy=str(strategy),
                    value=str(value) if value is not None else "",
                )
            )
        table_rules.append(PIITableRules(table=str(table), columns=columns))

    retention_raw = data.get("retention_days_by_jurisdiction") or {}
    if not isinstance(retention_raw, dict):
        raise InvalidPIIMapError("'retention_days_by_jurisdiction' must be a mapping")
    retention: dict[str, int] = {}
    for k, v in retention_raw.items():
        try:
            retention[str(k).upper() if str(k) != "default" else "default"] = int(v)
        except (TypeError, ValueError) as exc:
            raise InvalidPIIMapError(f"retention value for {k!r} must be int") from exc

    on_creator = data.get("on_creator_closure") or {}
    if not isinstance(on_creator, dict):
        raise InvalidPIIMapError("'on_creator_closure' must be a mapping")
    on_strategy = on_creator.get("strategy")
    if not on_strategy:
        raise InvalidPIIMapError("'on_creator_closure.strategy' is required")
    if on_strategy not in _VALID_ON_CREATOR:
        raise InvalidPIIMapError(
            f"unknown on_creator_closure.strategy {on_strategy!r}; valid: {sorted(_VALID_ON_CREATOR)}"
        )
    on_show_as = on_creator.get("show_authorship_as", "")

    return PIIClosureMap(
        domain=str(domain),
        version=version_str,
        table_rules=table_rules,
        retention_by_jurisdiction=retention,
        on_creator_closure_strategy=str(on_strategy),
        on_creator_closure_show_as=str(on_show_as),
    )


def load_pii_map_from_file(path: Path) -> PIIClosureMap:
    """Load + parse a PII_Closure_Map.yaml file by path."""
    text = Path(path).read_text(encoding="utf-8")
    return load_pii_map(text)
