"""InMemoryColdArchiveClient + jurisdictional retention helper.

Production replacement: the MinIO-backed client (``MinioColdArchiveClient``).
The in-memory variant is used in unit + integration tests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

# Per .claude/skills/account-closure-saga/SKILL.md table:
#   GDPR (EU)  — 7 years  (2557 days)
#   PDPA (SG)  — 7 years  (2557 days)
#   CCPA (US)  — 5 years  (1826 days)
#   POPIA (ZA) — 5 years  (1826 days)
#   LGPD (BR)  — 5 years  (1826 days)
_JURISDICTION_TO_DAYS: dict[str, int] = {
    "EU": 2557,
    "SG": 2557,
    "US": 1826,
    "ZA": 1826,
    "BR": 1826,
}
_DEFAULT_RETENTION_DAYS = 2557  # SG/EU floor


def jurisdictional_retention_days(jurisdiction: str) -> int:
    """Return the retention floor for the jurisdiction.

    Unknown jurisdictions fall back to the SG/EU 2557-day floor (NEVER
    shorter, per skill anti-patterns).
    """
    return _JURISDICTION_TO_DAYS.get(jurisdiction.upper(), _DEFAULT_RETENTION_DAYS)


@dataclass(frozen=True)
class ColdArchiveJobSpec:
    """Cold-archive job request."""

    saga_id: str
    gcid: str
    tenant_id: str
    jurisdiction: str
    payload: bytes
    dek_resource_name: str


@dataclass(frozen=True)
class ColdArchiveResult:
    """Cold-archive object-store write result."""

    success: bool
    object_name: str
    gcs_uri: str
    retention_days: int


class InMemoryColdArchiveClient:
    """In-memory cold-archive — records each archive write in memory."""

    def __init__(self, *, bucket: str) -> None:
        if not bucket:
            raise ValueError("bucket name required")
        self._bucket = bucket
        self._archives: list[ColdArchiveResult] = []
        self._lock = asyncio.Lock()

    async def archive(self, spec: ColdArchiveJobSpec) -> ColdArchiveResult:
        if not spec.payload:
            raise ValueError("cold-archive payload empty")
        if not spec.dek_resource_name:
            raise ValueError("cold-archive requires per-user DEK (dek_resource_name)")
        # Naming convention from spec:
        #   {tenant_id}/{gcid}/{closure_id}.tar.gz.enc
        object_name = f"{spec.tenant_id}/{spec.gcid}/{spec.saga_id}.tar.gz.enc"
        gcs_uri = f"gs://{self._bucket}/{object_name}"
        result = ColdArchiveResult(
            success=True,
            object_name=object_name,
            gcs_uri=gcs_uri,
            retention_days=jurisdictional_retention_days(spec.jurisdiction),
        )
        async with self._lock:
            self._archives.append(result)
        return result

    def list_archives(self) -> list[ColdArchiveResult]:
        return list(self._archives)
