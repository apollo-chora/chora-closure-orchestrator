"""LocalKMSClient — cloud-neutral ``KMSClient`` backed by AES-256-GCM with
the master KEK from the ``CHORA_LOCAL_KEK`` environment variable.

The local substitute for Cloud KMS, preserving the envelope encryption
pattern + crypto-shred semantics exactly:

* **Master KEK** — 32 bytes, base64-encoded in ``CHORA_LOCAL_KEK`` (env-backed
  secret; compose injects it). NEVER stored in the database; lives only in
  the process environment. Rotate by rotating the env value + re-wrapping.
* **Per-user DEK** — 32 random bytes generated client-side
  (``secrets.token_bytes``). Wrapped by ``AESGCM(kek).encrypt(nonce, DEK,
  AAD=tenant|gcid)`` into a ``wrapped_dek`` ciphertext bundle
  ``nonce (12B) | tag (16B) | ciphertext``. The PLAINTEXT DEK is NEVER
  persisted — only held in memory for the duration of an operation.
* **Wrapped DEK store** — the ``wrapped_dek`` ciphertext is persisted via
  the injected ``WrappedDEKStore`` (ADR-186):
  ``PostgresWrappedDEKStore`` over ``chora_ai_kernel.closure_user_dek_wrap``
  (migration 0054) in the deployed topology so it survives the
  archive→retention→shred gap; ``InMemoryWrappedDEKStore`` for tests / the
  dev escape hatch. Keyed by ``(tenant_id, gcid)``.
* **Per-payload encryption** — AES-256-GCM with the unwrapped DEK.
  Ciphertext bundle = ``nonce (12B) | tag (16B) | ciphertext``.
* **Crypto-shred** — DELETE the ``wrapped_dek`` row from the store. The
  plaintext DEK is non-recoverable (never persisted; only the wrapped form
  was, and that's now gone). All prior payloads encrypted under this DEK
  are permanently unrecoverable. Idempotent.

Why this satisfies the D4 (GDPR shred) gate without Cloud KMS:

1. The PLAINTEXT DEK is never persisted (only the wrapped form).
2. The wrapping KEK lives only in the service environment; the database
   holds no key material.
3. Therefore deleting the wrapped DEK destroys the only artifact that
   could recover the plaintext DEK — without it, the AES-GCM ciphertext
   is permanently undecryptable.

Env vars (per ``feedback_no_inline_config``):

* ``CHORA_LOCAL_KEK`` — base64-encoded 32-byte master KEK. Example:
  ``openssl rand -base64 32``.
"""

from __future__ import annotations

import asyncio
import base64
import os
import secrets
import uuid
from dataclasses import dataclass, field

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from chora_closure_orchestrator.adapter.kms.fake import DEKDeletedError, DEKMetadata
from chora_closure_orchestrator.adapter.kms.store import (
    InMemoryWrappedDEKStore,
    WrappedDEK,
    WrappedDEKStore,
)

LOCAL_KEK_ENV = "CHORA_LOCAL_KEK"

# Logical kek_version stamped on wrapped DEKs — identifies the local KEK
# generation for re-wrap planning when CHORA_LOCAL_KEK rotates.
LOCAL_KEK_VERSION = "local-kek-v1"

# AES-256-GCM uses 96-bit nonces (12 bytes) per NIST SP 800-38D.
_AESGCM_NONCE_BYTES = 12
# DEK length: 32 bytes = AES-256.
_DEK_BYTES = 32


def kek_from_env() -> bytes:
    """Read + decode the master KEK from the ``CHORA_LOCAL_KEK`` env var.

    Refuses to fall back per ``feedback_no_inline_config``: the deployment
    (compose / operator) injects the value.
    """
    v = os.environ.get(LOCAL_KEK_ENV)
    if not v:
        raise RuntimeError(
            f"{LOCAL_KEK_ENV} env var required; deployment must inject "
            f"the base64-encoded 32-byte master KEK. No inline default "
            f"per feedback_no_inline_config."
        )
    try:
        raw = base64.b64decode(v, validate=True)
    except Exception as exc:  # noqa: BLE001 — any decode failure is fatal
        raise RuntimeError(f"{LOCAL_KEK_ENV} is not valid base64: {exc}") from exc
    if len(raw) != _DEK_BYTES:
        raise RuntimeError(f"{LOCAL_KEK_ENV} must decode to {_DEK_BYTES} bytes (AES-256); got {len(raw)}")
    return raw


def _aad(*, tenant_id: str, gcid: str) -> bytes:
    """Additional Authenticated Data binding ciphertext to tenant + user.

    The wrap/unwrap AAD must match exactly — mismatch raises InvalidTag.
    This prevents cross-tenant/cross-user ciphertext substitution even when
    the same master KEK is shared.
    """
    return f"tenant:{tenant_id}|gcid:{gcid}".encode()


@dataclass
class LocalKMSClient:
    """Cloud-neutral ``KMSClient`` backed by env KEK envelope encryption.

    Use ``LocalKMSClient.from_env()`` to construct from
    ``CHORA_LOCAL_KEK``. Tests can pass an explicit ``kek`` + an explicit
    ``store`` (e.g. an ``InMemoryWrappedDEKStore``).

    The **wrapped DEKs are persisted via the injected ``store``** (ADR-186)
    — Postgres (``chora_ai_kernel.closure_user_dek_wrap``) in the deployed
    topology so a wrapped DEK survives the archive→retention→shred gap;
    in-memory for tests / the dev escape hatch. Crypto-shred deletes the
    wrapped row; the plaintext DEK is never persisted, so the ciphertext
    is then permanently undecryptable.

    Thread-safety: an internal ``asyncio.Lock`` serializes
    ``create_user_dek`` so a concurrent double-create can't double-wrap.
    """

    kek: bytes
    store: WrappedDEKStore = field(default_factory=InMemoryWrappedDEKStore)

    def __post_init__(self) -> None:
        self._lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    def from_env(cls, store: WrappedDEKStore | None = None) -> LocalKMSClient:
        """Construct from ``CHORA_LOCAL_KEK`` env var.

        ``store`` defaults to a fresh in-memory store; wiring passes a
        ``PostgresWrappedDEKStore`` when a DSN is available.
        """
        kek = kek_from_env()
        if store is None:
            return cls(kek=kek)
        return cls(kek=kek, store=store)

    # ---------- KMSClient Protocol impl ----------

    async def create_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata:
        """Generate a fresh DEK; wrap with master KEK + AAD; persist wrapped form.

        Idempotent: a second call with the same (tenant_id, gcid) returns
        the existing wrapped DEK's metadata without re-wrapping. This
        matches the FakeKMSClient semantics + protects against re-issuing
        DEKs that would break decryption of in-flight ciphertexts.
        """
        async with self._lock:
            existing = await self.store.get(tenant_id=tenant_id, gcid=gcid)
            if existing is not None and not existing.deleted:
                return existing.to_metadata()

            dek = secrets.token_bytes(_DEK_BYTES)
            aes = AESGCM(self.kek)
            nonce = secrets.token_bytes(_AESGCM_NONCE_BYTES)
            wrapped = nonce + aes.encrypt(
                nonce,
                dek,
                _aad(tenant_id=tenant_id, gcid=gcid),
            )
            record = WrappedDEK(
                tenant_id=tenant_id,
                gcid=gcid,
                wrapped_dek=wrapped,
                kek_version=LOCAL_KEK_VERSION,
                deleted=False,
            )
            await self.store.put(record)
            return record.to_metadata()

    async def get_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata | None:
        w = await self.store.get(tenant_id=tenant_id, gcid=gcid)
        return w.to_metadata() if w is not None else None

    async def encrypt(self, *, tenant_id: str, gcid: str, plaintext: bytes) -> bytes:
        """AES-256-GCM encrypt with the unwrapped per-user DEK.

        Ciphertext bundle layout: ``nonce (12B) | aesgcm_output``
        where ``aesgcm_output`` = ``ciphertext | tag (16B)``.

        Raises DEKDeletedError if the wrapped DEK has been crypto-shredded
        (or never created).
        """
        dek = await self._unwrap_for(tenant_id=tenant_id, gcid=gcid)
        aes = AESGCM(dek)
        nonce = secrets.token_bytes(_AESGCM_NONCE_BYTES)
        ct = aes.encrypt(
            nonce,
            plaintext,
            _aad(tenant_id=tenant_id, gcid=gcid),
        )
        return nonce + ct

    async def decrypt(self, *, tenant_id: str, gcid: str, ciphertext: bytes) -> bytes:
        """AES-256-GCM decrypt with the unwrapped per-user DEK.

        Expects the ciphertext bundle layout produced by ``encrypt`` above.
        Raises DEKDeletedError if the wrapped DEK has been crypto-shredded.
        """
        if len(ciphertext) < _AESGCM_NONCE_BYTES:
            raise ValueError(
                f"ciphertext too short ({len(ciphertext)} bytes); "
                f"expected ≥{_AESGCM_NONCE_BYTES} bytes for nonce prefix"
            )
        dek = await self._unwrap_for(tenant_id=tenant_id, gcid=gcid)
        aes = AESGCM(dek)
        nonce, body = ciphertext[:_AESGCM_NONCE_BYTES], ciphertext[_AESGCM_NONCE_BYTES:]
        return aes.decrypt(nonce, body, _aad(tenant_id=tenant_id, gcid=gcid))

    async def delete_user_dek(self, *, tenant_id: str, gcid: str) -> str:
        """Crypto-shred: remove the wrapped DEK from the store.

        The plaintext DEK was never persisted, so deleting the wrapped DEK
        renders all prior ciphertexts permanently undecryptable.

        Idempotent: a second call returns a fresh op_id without raising.
        Returns the operation ID (logical, prefixed ``shred-``) for audit
        traceback.

        The store ``mark_deleted`` tombstones the wrapped row
        (``deleted_at``) + scrubs the bytes. After this, the only path to
        recover the plaintext DEK would be an unwrap of a wrapped
        ciphertext that no longer exists — so prior ciphertext is
        permanently unrecoverable.
        """
        op_id = f"shred-{uuid.uuid4().hex[:16]}"
        await self.store.mark_deleted(tenant_id=tenant_id, gcid=gcid, op_id=op_id)
        return op_id

    # ---------- internals ----------

    async def _unwrap_for(self, *, tenant_id: str, gcid: str) -> bytes:
        """Fetch the wrapped DEK + unwrap to plaintext DEK with the KEK.

        The plaintext DEK is returned for in-memory use ONLY (never
        persisted). Callers MUST discard after each operation.
        """
        w = await self.store.get(tenant_id=tenant_id, gcid=gcid)
        if w is None:
            raise DEKDeletedError(f"DEK not provisioned for ({tenant_id}, {gcid})")
        if w.deleted or not w.wrapped_dek:
            raise DEKDeletedError(
                f"DEK deleted (crypto-shredded) for ({tenant_id}, {gcid}); prior ciphertext UNRECOVERABLE"
            )
        nonce, body = (
            w.wrapped_dek[:_AESGCM_NONCE_BYTES],
            w.wrapped_dek[_AESGCM_NONCE_BYTES:],
        )
        aes = AESGCM(self.kek)
        return aes.decrypt(nonce, body, _aad(tenant_id=tenant_id, gcid=gcid))


__all__ = [
    "LOCAL_KEK_ENV",
    "LOCAL_KEK_VERSION",
    "LocalKMSClient",
    "kek_from_env",
]
