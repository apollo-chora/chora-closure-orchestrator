"""KMSClient port (Protocol)."""

from __future__ import annotations

from typing import Protocol

from chora_closure_orchestrator.adapter.kms.fake import DEKMetadata


class KMSClient(Protocol):
    """Per-user DEK lifecycle port.

    Production: backed by the local KMS adapter (``LocalKMSClient``) —
    per-user DEKs wrapped by the ``CHORA_LOCAL_KEK`` master key with
    per-tenant AAD binding.
    """

    async def create_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata:
        """Provision a new per-user DEK under the tenant master key."""
        ...

    async def get_user_dek(self, *, tenant_id: str, gcid: str) -> DEKMetadata | None:
        """Return DEK metadata or None if absent."""
        ...

    async def encrypt(self, *, tenant_id: str, gcid: str, plaintext: bytes) -> bytes:
        """Encrypt with the per-user DEK.

        :raises DEKDeletedError: DEK has been crypto-shredded.
        """
        ...

    async def decrypt(self, *, tenant_id: str, gcid: str, ciphertext: bytes) -> bytes:
        """Decrypt with the per-user DEK.

        :raises DEKDeletedError: DEK has been crypto-shredded.
        """
        ...

    async def delete_user_dek(self, *, tenant_id: str, gcid: str) -> str:
        """Schedule the per-user DEK for KMS scheduled-destruction.

        Returns the KMS operation ID for audit traceback. Idempotent —
        a second call returns a fresh op ID without raising.
        """
        ...
