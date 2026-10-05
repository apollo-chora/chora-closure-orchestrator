"""Wrapped-DEK store — the durable home for envelope-encrypted per-user DEKs.

Per **ADR-186** crypto-shred = deleting the wrapped DEK. The wrapped form
must survive the full closure timeline (archive → multi-year retention →
shred), so it CANNOT live in process memory. This module splits the storage
of the wrapped DEK behind a port so ``LocalKMSClient`` is agnostic to
where the wrapped bytes live:

* ``InMemoryWrappedDEKStore`` — tests + the dev escape hatch (lost on restart).
* ``PostgresWrappedDEKStore`` — production, backed by ``chora_ai_kernel``'s
  ``closure_user_dek_wrap`` table (migration 0054). The orchestrator owns
  this table in its OWN database — no cross-DB reads.

Crypto-shred is a **tombstone**: ``mark_deleted`` sets ``deleted_at`` and
scrubs the wrapped bytes. Per ADR-186 §D3 a sweeper hard-deletes rows past
the 24h reversible window; until then ``deleted=True`` already makes the
data functionally unrecoverable (decrypt refuses), giving an operator
escape hatch without a native KMS scheduled-destruction.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

from chora_closure_orchestrator.adapter.kms.fake import DEKMetadata


@dataclass
class WrappedDEK:
    """One persisted wrapped-DEK record.

    ``wrapped_dek`` is the master-KEK-wrapped DEK ciphertext (the plaintext
    DEK is NEVER stored). ``deleted=True`` + empty ``wrapped_dek`` is the
    crypto-shred tombstone.
    """

    tenant_id: str
    gcid: str
    wrapped_dek: bytes
    kek_version: str = ""  # KMS encrypt response name — for re-wrap on KEK rotation
    deleted: bool = False
    kms_operation_id: str = ""

    def to_metadata(self) -> DEKMetadata:
        # Logical identifier of the wrapped-DEK RECORD (app DB), NOT a Cloud
        # KMS resource path — the wrapped DEK lives in chora_ai_kernel.
        return DEKMetadata(
            tenant_id=self.tenant_id,
            gcid=self.gcid,
            dek_resource_name=f"chora-closure/dek-wrap/{self.tenant_id}/{self.gcid}",
            deleted=self.deleted,
            kms_operation_id=self.kms_operation_id,
        )


class WrappedDEKStore(Protocol):
    """Port: persist + tombstone wrapped per-user DEKs, keyed by (tenant, gcid)."""

    async def get(self, *, tenant_id: str, gcid: str) -> WrappedDEK | None:
        """Return the wrapped DEK record (alive OR tombstoned) or None."""
        ...

    async def put(self, dek: WrappedDEK) -> None:
        """Persist a freshly wrapped DEK. Idempotent — re-put is a no-op."""
        ...

    async def mark_deleted(self, *, tenant_id: str, gcid: str, op_id: str) -> None:
        """Crypto-shred: tombstone the row (``deleted_at``) + scrub bytes.

        Idempotent — absent / already-deleted is a no-op (never raises).
        """
        ...


@dataclass
class InMemoryWrappedDEKStore:
    """Process-local store. Dev escape hatch + unit tests ONLY — lost on
    restart, so NEVER selected when a Postgres DSN is available."""

    def __post_init__(self) -> None:
        self._w: dict[tuple[str, str], WrappedDEK] = {}
        self._lock = asyncio.Lock()

    async def get(self, *, tenant_id: str, gcid: str) -> WrappedDEK | None:
        async with self._lock:
            return self._w.get((tenant_id, gcid))

    async def put(self, dek: WrappedDEK) -> None:
        async with self._lock:
            self._w[(dek.tenant_id, dek.gcid)] = dek

    async def mark_deleted(self, *, tenant_id: str, gcid: str, op_id: str) -> None:
        async with self._lock:
            w = self._w.get((tenant_id, gcid))
            if w is None or w.deleted:
                return
            w.wrapped_dek = b""  # scrub — defensive against memory disclosure
            w.deleted = True
            w.kms_operation_id = op_id


_INSERT = """
    INSERT INTO closure_user_dek_wrap
        (tenant_id, gcid, wrapped_dek, kek_version, created_at)
    VALUES (%(tenant_id)s, %(gcid)s, %(wrapped_dek)s, %(kek_version)s, now())
    ON CONFLICT (tenant_id, gcid) DO NOTHING
"""

_SELECT = """
    SELECT wrapped_dek, kek_version, deleted_at, kms_operation_id
    FROM closure_user_dek_wrap
    WHERE tenant_id = %(tenant_id)s AND gcid = %(gcid)s
"""

# Tombstone + scrub in one statement; only the first shred wins (deleted_at
# stays NULL gate keeps the op_id of the original shred). Idempotent.
_UPDATE_DELETE = """
    UPDATE closure_user_dek_wrap
    SET deleted_at = now(),
        wrapped_dek = ''::bytea,
        kms_operation_id = %(op_id)s
    WHERE tenant_id = %(tenant_id)s AND gcid = %(gcid)s
      AND deleted_at IS NULL
"""


@dataclass
class PostgresWrappedDEKStore:
    """Durable wrapped-DEK store on an **autocommit** psycopg AsyncConnection
    to chora_ai_kernel (the orchestrator's own DB).

    Autocommit avoids the long-lived-conn ``InFailedSqlTransaction`` sticky
    state the orchestrator hit elsewhere — every statement stands alone.
    """

    conn: Any  # psycopg.AsyncConnection (autocommit=True)

    async def get(self, *, tenant_id: str, gcid: str) -> WrappedDEK | None:
        async with self.conn.cursor() as cur:
            await cur.execute(_SELECT, {"tenant_id": tenant_id, "gcid": gcid})
            row = await cur.fetchone()
        if row is None:
            return None
        wrapped, kek_version, deleted_at, op_id = row
        return WrappedDEK(
            tenant_id=tenant_id,
            gcid=gcid,
            wrapped_dek=bytes(wrapped) if wrapped is not None else b"",
            kek_version=kek_version or "",
            deleted=deleted_at is not None,
            kms_operation_id=op_id or "",
        )

    async def put(self, dek: WrappedDEK) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                _INSERT,
                {
                    "tenant_id": dek.tenant_id,
                    "gcid": dek.gcid,
                    "wrapped_dek": dek.wrapped_dek,
                    "kek_version": dek.kek_version,
                },
            )

    async def mark_deleted(self, *, tenant_id: str, gcid: str, op_id: str) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(
                _UPDATE_DELETE,
                {"tenant_id": tenant_id, "gcid": gcid, "op_id": op_id},
            )


__all__ = [
    "WrappedDEK",
    "WrappedDEKStore",
    "InMemoryWrappedDEKStore",
    "PostgresWrappedDEKStore",
]
