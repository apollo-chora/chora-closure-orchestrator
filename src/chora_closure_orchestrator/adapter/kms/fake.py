"""FakeKMSClient — in-memory KMS adapter for tests + MVP.

Models the per-tenant CMEK + per-user DEK hierarchy. Encryption uses a
trivial XOR cipher (NOT secure — just enough to verify the lifecycle
guarantee that ``decrypt`` works pre-shred and FAILS post-shred).

Production replacement: the local KMS adapter (``LocalKMSClient``)
wrapped in the same ``KMSClient`` Protocol shape.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass


class DEKDeletedError(Exception):
    """Raised when an encrypt/decrypt operation targets a deleted DEK.

    Crypto-shred renders prior ciphertext UNRECOVERABLE — this error
    signals the GDPR Art. 17 / PDPA "destroyed" state.
    """


@dataclass
class DEKMetadata:
    """Per-user DEK metadata."""

    tenant_id: str
    gcid: str
    dek_resource_name: str
    deleted: bool = False
    kms_operation_id: str = ""


class FakeKMSClient:
    """In-memory KMS adapter for tests."""

    def __init__(self) -> None:
        # Keyed by (tenant_id, gcid)
        self._deks: dict[tuple[str, str], DEKMetadata] = {}
        # Per-DEK key bytes (used for the XOR cipher)
        self._key_bytes: dict[tuple[str, str], bytes] = {}
        self._lock = asyncio.Lock()

    async def create_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata:
        async with self._lock:
            key = (tenant_id, gcid)
            if key in self._deks:
                return self._deks[key]
            dek_id = f"local/dek-wrap/{tenant_id}/{gcid}/{_uuid()}"
            meta = DEKMetadata(
                tenant_id=tenant_id,
                gcid=gcid,
                dek_resource_name=dek_id,
                deleted=False,
            )
            self._deks[key] = meta
            self._key_bytes[key] = os.urandom(32)
            return meta

    async def get_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata | None:
        async with self._lock:
            return self._deks.get((tenant_id, gcid))

    async def encrypt(self, *, tenant_id: str, gcid: str, plaintext: bytes) -> bytes:
        async with self._lock:
            key = (tenant_id, gcid)
            meta = self._deks.get(key)
            if meta is None:
                raise DEKDeletedError(f"DEK not provisioned for ({tenant_id}, {gcid})")
            if meta.deleted:
                raise DEKDeletedError(f"DEK deleted (crypto-shredded) for ({tenant_id}, {gcid})")
            return _xor(plaintext, self._key_bytes[key])

    async def decrypt(self, *, tenant_id: str, gcid: str, ciphertext: bytes) -> bytes:
        async with self._lock:
            key = (tenant_id, gcid)
            meta = self._deks.get(key)
            if meta is None:
                raise DEKDeletedError(f"DEK not provisioned for ({tenant_id}, {gcid})")
            if meta.deleted:
                raise DEKDeletedError(
                    f"DEK deleted (crypto-shredded) for ({tenant_id}, {gcid}); prior ciphertext UNRECOVERABLE"
                )
            return _xor(ciphertext, self._key_bytes[key])

    async def delete_user_dek(self, *, tenant_id: str, gcid: str) -> str:
        """Crypto-shred = mark DEK deleted + scrub key bytes.

        Idempotent. Returns a fresh KMS operation ID per call.
        """
        async with self._lock:
            key = (tenant_id, gcid)
            meta = self._deks.get(key)
            op_id = f"op-{_uuid()}"
            if meta is None:
                # No DEK to delete; idempotent — return op id anyway.
                return op_id
            if not meta.deleted:
                # Scrub key bytes — primary mandate of crypto-shred
                self._key_bytes[key] = b""
                meta.deleted = True
                meta.kms_operation_id = op_id
            return op_id


def _xor(data: bytes, key: bytes) -> bytes:
    if not key:
        # Defensive: scrubbed key
        raise DEKDeletedError("DEK key bytes scrubbed")
    out = bytearray(len(data))
    for i in range(len(data)):
        out[i] = data[i] ^ key[i % len(key)]
    return bytes(out)


def _uuid() -> str:
    return str(uuid.uuid4()).replace("-", "")[:16]
