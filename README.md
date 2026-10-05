# chora-closure-orchestrator

Closure Orchestrator for Chora: the Python LangGraph half of the federated
account-closure saga (Tier 3 D11; Tier 2 D5 hybrid kernel). It owns the
closure-saga lifecycle, fans out per-domain pseudonymisation requests on the
event bus, awaits the per-domain acks, cold-archives the DEK-encrypted
bundle, and crypto-shreds the per-user DEK — pseudonymise + crypto-shred,
NEVER hard-delete.

The service is cloud-neutral: PostgreSQL for persistence, NATS JetStream for
events, MinIO for the cold archive, env-backed configuration for secrets. No
cloud account or managed services (managed SQL, Pub/Sub, Secret Manager, or
CI/CD) are required.

## What it does

1. **Federated closure saga** — `POST /v1/closure/request` starts a saga
   (grace window, default 30 days); the SagaDriver background loop advances
   it: `CLOSING → SUSPENDED` (grace expiry) → fan-out of 10
   `chora.{domain}.pii.pseudonymise.requested.v1` events →
   `PSEUDONYMIZED` (all domains acked) → `COLD_ARCHIVED` →
   `CRYPTO_SHREDDED`.
2. **Per-domain ack federation** — domain services emit
   `chora.{domain}.account.pseudonymised.v1`; the orchestrator subscribes
   and records each ack on the saga. `COLD_ARCHIVED` is only reachable
   once every required domain has acked. An ack timeout (default 24h)
   escalates to `PSEUDONYMISE_PARTIAL` for operator attention.
3. **Envelope encryption + crypto-shred** — per-user DEKs (32 random bytes)
   wrapped by the master KEK (`CHORA_LOCAL_KEK`, AES-256-GCM with
   tenant+gcid AAD) and persisted in `chora_ai_kernel.closure_user_dek_wrap`
   (migration 0054). Crypto-shred deletes the wrapped DEK; the plaintext DEK
   is never persisted, so prior ciphertext is permanently unrecoverable.
4. **Transactional outbox** — every saga event is written to
   `chora_ai_kernel.closure_outbox_events` in the same database, then
   drained to the NATS JetStream event bus by the outbox dispatcher with
   retry + dead-letter semantics. The 6 `chora.closure.*` lifecycle topics
   are published as canonical Protobuf binary (vendored
   `chora_contracts_gen` stubs); the per-domain fan-out/ack topics are JSON.
5. **Cold archive** — the DEK-encrypted bundle is written to MinIO at
   `{tenant_id}/{gcid}/{saga_id}.tar.gz.enc` with per-jurisdiction retention
   metadata (SG/EU 2557 days, US/ZA/BR 1826 days).

## Architecture

- **Compute**: any host running the Python venv or the container image.
- **Database**: PostgreSQL (`chora_ai_kernel`). Schema changes live in
  `migrations/` and are applied with the shared migration runner.
- **Event bus**: NATS JetStream. The outbox dispatcher drains pending rows;
  the per-domain ack consumers subscribe to the ack subjects.
- **Object store**: MinIO (cold archive).
- **Ports**: HTTP `:8080` (`/healthz`, `/readyz`, `/v1/closure/*`).

## Configuration

Copy the example environment file:

```sh
cp .env.example .env
```

Important variables:

| Variable | Purpose |
|---|---|
| `CHORA_AI_KERNEL_PG_DSN` | DSN for the `chora_ai_kernel` database (coordinator repo + outbox + wrapped-DEK store). Alias: `CHORA_AI_KERNEL_CONNINFO`. |
| `CHORA_AI_KERNEL_PG_DSN_SECRET_ID` | Env-backed secret alias — when set (and no direct DSN), its value IS the DSN. |
| `CHORA_SOURCE_PROJECT` | Logical source name stamped into the event envelope; enables the outbox dispatcher (requires the DSN too). |
| `CHORA_LOCAL_KEK` | base64-encoded 32-byte master KEK for the local KMS adapter (`openssl rand -base64 32`). |
| `CHORA_COLD_ARCHIVE_BUCKET` | MinIO bucket for the cold archive. |
| `CHORA_NATS_URL` | NATS broker URL (default `nats://localhost:4222`). |
| `CHORA_MINIO_ENDPOINT` / `CHORA_MINIO_ACCESS_KEY` / `CHORA_MINIO_SECRET_KEY` / `CHORA_MINIO_SECURE` | MinIO connection for the cold-archive writer. |
| `CLOSURE_ACK_SUBSCRIPTIONS` | Comma-joined ack topic names (default: the 10 `chora.{domain}.account.pseudonymised.v1` subjects). |
| `CLOSURE_DRIVER_POLL_SECONDS` | Saga-driver poll interval (default 30). |
| `CLOSURE_OUTBOX_POLL_SECONDS` | Outbox dispatcher poll interval (default 0.5). |
| `CLOSURE_ACK_TIMEOUT_SECONDS` | SUSPENDED → PSEUDONYMISE_PARTIAL escalation (default 86400). |
| `CHORA_CLOSURE_FAKE_ADAPTERS` | `true` forces in-memory/fake adapters (dev escape hatch; NEVER set in prod). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP-gRPC collector endpoint for tracing (unset = no-op). |

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness probe. |
| `GET` | `/readyz` | Readiness probe (503 until adapters are configured). |
| `POST` | `/v1/closure/request` | Start a closure saga (idempotent on saga_id). |
| `POST` | `/v1/closure/{closure_id}/cancel` | Cancel during the grace period. |
| `GET` | `/v1/closure/{closure_id}/status` | Current state + history + per-domain acks. |

Auth: `Bearer <GCID>` header. Operator fast-close (`fast_close: true`)
collapses the grace window and is restricted to `PLATFORM_OPERATOR` sessions
(gateway-stamped `X-Chora-Role` header).

## Development

```sh
uv sync --all-groups   # or: uv sync --extra dev
uv run pytest          # 375 tests
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

## Build

```sh
docker build -t chora-closure-orchestrator .
```

The image runs as nonroot (uid 65532) and listens on port 8080.
