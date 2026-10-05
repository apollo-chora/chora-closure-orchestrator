"""Adapter layer — infrastructure-specific implementations of domain ports.

Hexagonal: adapters depend on domain ports (interfaces); domain NEVER
imports adapters. Each subpackage holds one adapter family + its in-memory
test double:

- repository: Coordinator persistence (in-mem now; Postgres at S2/M14)
- events: closure event publishers (in-mem now; NATS JetStream at S2/M14)
- kms: local KMS DEK lifecycle (FakeKMSClient now; LocalKMSClient at M14)
- coldarchive: MinIO cold-archive writer
- pii_map: PII_Closure_Map.yaml loader (shared)
- http: FastAPI handlers
- grpc: gRPC server (deferred to S2)
"""
