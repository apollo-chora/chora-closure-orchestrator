"""Durable wrapped-DEK store (ADR-186) — the orchestrator's crypto-shred
must survive the archive→retention→shred gap (years in production), so the
wrapped DEK CANNOT live in an in-memory dict.

These tests pin:
  * the ``WrappedDEKStore`` port + its in-memory impl (tombstone on delete);
  * ``LocalKMSClient`` persisting via an INJECTED store (durability seam);
  * ``PostgresWrappedDEKStore`` issuing the right SQL against an autocommit
    psycopg AsyncConnection (chora_ai_kernel-backed, adapter-level).
"""

from __future__ import annotations

import base64
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.kms import DEKDeletedError, LocalKMSClient
from chora_closure_orchestrator.adapter.kms.store import (
    InMemoryWrappedDEKStore,
    PostgresWrappedDEKStore,
    WrappedDEK,
)

TENANT = "01970000-0000-7000-8000-000000000001"
GCID = "01970000-0000-7000-9000-00000000000a"
# 32-byte test KEK (the local substitute for a Cloud KMS master KEK).
TEST_KEK = b"\x01" * 32
TEST_KEK_B64 = base64.b64encode(TEST_KEK).decode("ascii")


# -----------------------------------------------------------------------------
# InMemoryWrappedDEKStore
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inmem_store_get_absent_returns_none() -> None:
    store = InMemoryWrappedDEKStore()
    assert await store.get(tenant_id=TENANT, gcid=GCID) is None


@pytest.mark.asyncio
async def test_inmem_store_put_then_get() -> None:
    store = InMemoryWrappedDEKStore()
    await store.put(WrappedDEK(tenant_id=TENANT, gcid=GCID, wrapped_dek=b"wrapped", kek_version="v1"))
    got = await store.get(tenant_id=TENANT, gcid=GCID)
    assert got is not None
    assert got.wrapped_dek == b"wrapped"
    assert got.kek_version == "v1"
    assert got.deleted is False


@pytest.mark.asyncio
async def test_inmem_store_mark_deleted_tombstones_and_scrubs() -> None:
    store = InMemoryWrappedDEKStore()
    await store.put(WrappedDEK(tenant_id=TENANT, gcid=GCID, wrapped_dek=b"secret-wrapped"))
    await store.mark_deleted(tenant_id=TENANT, gcid=GCID, op_id="shred-abc")
    got = await store.get(tenant_id=TENANT, gcid=GCID)
    assert got is not None
    assert got.deleted is True
    assert got.wrapped_dek == b""  # scrubbed
    assert got.kms_operation_id == "shred-abc"


@pytest.mark.asyncio
async def test_inmem_store_mark_deleted_absent_is_noop() -> None:
    store = InMemoryWrappedDEKStore()
    # Must not raise — crypto-shred is idempotent even when nothing was stored.
    await store.mark_deleted(tenant_id=TENANT, gcid=GCID, op_id="shred-x")
    assert await store.get(tenant_id=TENANT, gcid=GCID) is None


# -----------------------------------------------------------------------------
# LocalKMSClient persists via the INJECTED store (durability seam)
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_writes_wrapped_dek_to_injected_store() -> None:
    store = InMemoryWrappedDEKStore()
    client = LocalKMSClient(kek=TEST_KEK, store=store)
    await client.create_user_dek(tenant_id=TENANT, gcid=GCID)

    # The wrapped DEK landed in the durable store, NOT an internal dict.
    persisted = await store.get(tenant_id=TENANT, gcid=GCID)
    assert persisted is not None
    assert persisted.wrapped_dek != b""
    assert persisted.deleted is False


@pytest.mark.asyncio
async def test_client_shred_marks_store_deleted_and_blocks_decrypt() -> None:
    store = InMemoryWrappedDEKStore()
    client = LocalKMSClient(kek=TEST_KEK, store=store)
    await client.create_user_dek(tenant_id=TENANT, gcid=GCID)
    ct = await client.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"PII")

    op_id = await client.delete_user_dek(tenant_id=TENANT, gcid=GCID)
    assert op_id.startswith("shred-")

    persisted = await store.get(tenant_id=TENANT, gcid=GCID)
    assert persisted is not None and persisted.deleted is True
    with pytest.raises(DEKDeletedError):
        await client.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=ct)


@pytest.mark.asyncio
async def test_client_decrypt_uses_freshly_constructed_store() -> None:
    """A client built over a store that already holds the wrapped DEK can
    decrypt WITHOUT a prior create — proving the read path goes through the
    store (survives a process restart that lost any in-memory dict)."""
    store = InMemoryWrappedDEKStore()
    # Seed the store via a first client (the "pre-restart" process).
    seeder = LocalKMSClient(kek=TEST_KEK, store=store)
    await seeder.create_user_dek(tenant_id=TENANT, gcid=GCID)
    ct = await seeder.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"survives-restart")

    # A brand-new client (the "post-restart" process) over the SAME store.
    revived = LocalKMSClient(kek=TEST_KEK, store=store)
    assert await revived.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=ct) == b"survives-restart"


# -----------------------------------------------------------------------------
# from_env store injection
# -----------------------------------------------------------------------------


def test_from_env_defaults_to_inmem_store(monkeypatch: pytest.MonkeyPatch) -> None:
    from chora_closure_orchestrator.adapter.kms import LOCAL_KEK_ENV

    monkeypatch.setenv(LOCAL_KEK_ENV, TEST_KEK_B64)
    client = LocalKMSClient.from_env()
    assert isinstance(client.store, InMemoryWrappedDEKStore)
    assert client.kek == TEST_KEK  # noqa: SLF001


def test_from_env_uses_injected_store(monkeypatch: pytest.MonkeyPatch) -> None:
    from chora_closure_orchestrator.adapter.kms import LOCAL_KEK_ENV

    monkeypatch.setenv(LOCAL_KEK_ENV, TEST_KEK_B64)
    pg = PostgresWrappedDEKStore(conn=_FakeConn())
    client = LocalKMSClient.from_env(store=pg)
    assert client.store is pg


def test_from_env_rejects_short_kek(monkeypatch: pytest.MonkeyPatch) -> None:
    from chora_closure_orchestrator.adapter.kms import LOCAL_KEK_ENV

    monkeypatch.setenv(LOCAL_KEK_ENV, base64.b64encode(b"\x01" * 16).decode("ascii"))
    with pytest.raises(RuntimeError, match="AES-256"):
        LocalKMSClient.from_env()


# -----------------------------------------------------------------------------
# PostgresWrappedDEKStore — SQL shape against a fake autocommit AsyncConnection
# -----------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *a: Any) -> bool:
        return False

    async def execute(self, sql: str, params: dict | None = None) -> None:
        self._conn.calls.append((" ".join(sql.split()), params))

    async def fetchone(self) -> Any:
        return self._conn.next_row


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.next_row: Any = None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


@pytest.mark.asyncio
async def test_pg_store_put_issues_insert_on_conflict() -> None:
    conn = _FakeConn()
    store = PostgresWrappedDEKStore(conn=conn)
    await store.put(WrappedDEK(tenant_id=TENANT, gcid=GCID, wrapped_dek=b"wrapped", kek_version="kv1"))
    sql, params = conn.calls[-1]
    assert "INSERT INTO closure_user_dek_wrap" in sql
    assert "ON CONFLICT" in sql  # idempotent re-create
    assert params["tenant_id"] == TENANT
    assert params["gcid"] == GCID
    assert params["wrapped_dek"] == b"wrapped"
    assert params["kek_version"] == "kv1"


@pytest.mark.asyncio
async def test_pg_store_get_maps_row() -> None:
    conn = _FakeConn()
    conn.next_row = (b"wrapped-bytes", "kv9", None, "")  # wrapped, kek, deleted_at, op_id
    store = PostgresWrappedDEKStore(conn=conn)
    got = await store.get(tenant_id=TENANT, gcid=GCID)
    sql, params = conn.calls[-1]
    assert "SELECT" in sql and "closure_user_dek_wrap" in sql
    assert params["tenant_id"] == TENANT and params["gcid"] == GCID
    assert got is not None
    assert got.wrapped_dek == b"wrapped-bytes"
    assert got.kek_version == "kv9"
    assert got.deleted is False


@pytest.mark.asyncio
async def test_pg_store_get_absent_returns_none() -> None:
    conn = _FakeConn()
    conn.next_row = None
    store = PostgresWrappedDEKStore(conn=conn)
    assert await store.get(tenant_id=TENANT, gcid=GCID) is None


@pytest.mark.asyncio
async def test_pg_store_get_reflects_tombstone() -> None:
    conn = _FakeConn()
    conn.next_row = (b"", "kv9", "2026-06-20T00:00:00Z", "shred-1")  # deleted_at set
    store = PostgresWrappedDEKStore(conn=conn)
    got = await store.get(tenant_id=TENANT, gcid=GCID)
    assert got is not None
    assert got.deleted is True
    assert got.kms_operation_id == "shred-1"


@pytest.mark.asyncio
async def test_pg_store_mark_deleted_updates_tombstone_and_scrubs() -> None:
    conn = _FakeConn()
    store = PostgresWrappedDEKStore(conn=conn)
    await store.mark_deleted(tenant_id=TENANT, gcid=GCID, op_id="shred-z")
    sql, params = conn.calls[-1]
    assert "UPDATE closure_user_dek_wrap" in sql
    assert "deleted_at" in sql
    assert "wrapped_dek" in sql  # scrub the bytes in the same statement
    assert params["op_id"] == "shred-z"
    assert params["tenant_id"] == TENANT and params["gcid"] == GCID
