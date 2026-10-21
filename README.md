# chora-closure-orchestrator

> The federated account-closure saga: start a closure, fan out per-domain
> pseudonymisation requests, collect acknowledgements, write an encrypted cold
> archive, and crypto-shred the user's data-encryption key.

Python and FastAPI, with PostgreSQL for durable saga state, NATS JetStream for
events, and object storage for the archive.

## What it is

Closing a Chora account is not one delete — it is a saga across ten domains. This
service owns that saga:

- **Starts and advances it** — a closure request enters `CLOSING`, a background
  `SagaDriver` advances it through the grace period, and on expiry it fans out
  per-domain pseudonymisation requests.
- **Collects acknowledgements** — each domain reports back; once every required
  domain has acknowledged, the saga can reach `PSEUDONYMIZED`.
- **Archives** — the closed account's data is written to object storage as an
  encrypted tarball.
- **Crypto-shreds** — the per-user data-encryption key is removed, so the archive
  ciphertext cannot be decrypted through this service.

In the stack it is built from `../chora-closure-orchestrator` and listens on
`closure-orchestrator:8080` **inside the Compose network only** — no host port.

### The saga states

`_ALLOWED` in `domain/closure/state.py` is the complete transition map:

```text
ACTIVE ──► CLOSING ──(cancel during grace)──► ACTIVE
              │
              └──(grace expires)──► SUSPENDED ──┬──(all required domains acked)──► PSEUDONYMIZED
                                                 │
                                                 └──(ack timeout)──► PSEUDONYMISE_PARTIAL
                                                                             │
                                                                             │ (a late ack lands)
                                                                             ▼
                                            PSEUDONYMIZED ──► COLD_ARCHIVED ──► CRYPTO_SHREDDED
```

Spelling matters when you grep or query: the state value is `PSEUDONYMIZED`
(American), while the partial state is `PSEUDONYMISE_PARTIAL` (British). Both are
single values in the enum; neither is `PSEUDONYMISED`.

Only `CLOSING` is cancellable (`is_cancellable`), and a cancel returns the saga to
`ACTIVE`. Fan-out on its own never advances the saga — it waits in `SUSPENDED` until
every required domain acknowledges, or the acknowledgement timeout (default 24
hours, `CLOSURE_ACK_TIMEOUT_SECONDS`) moves it to `PSEUDONYMISE_PARTIAL`. That
partial state is not terminal: a late acknowledgement still moves it forward to
`PSEUDONYMIZED`.

### The ten required domains

`REQUIRED_DOMAINS` in `domain/closure/state.py` — five core Content domains and five
supporting, with `ai_kernel` deliberately excluded because this orchestrator runs
inside it:

`creation`, `consumption`, `delivery`, `sharing`, `a2a`,
`identity`, `tenancy`, `governance`, `observability`, `notifications`.

A domain name outside this set is rejected (`ErrUnknownDomain`), so a typo in a
fan-out target fails loudly rather than being ignored.

## Prerequisites

| Route | What you need |
| --- | --- |
| In the stack | A working `chora-stack` deployment. PostgreSQL, NATS and Garage come from it. |
| Standalone | Python **3.13** and `uv`. |
| Standalone with real adapters | Plus PostgreSQL, a NATS JetStream broker, object storage, and a valid `CHORA_LOCAL_KEK`. |
| Any database you supply | The schema this repository's migrations create. Note they target the **`chora_ai_kernel`** database, shared with `chora-ai-kernel-orchestrator` — see [Migrations](#migrations). |

## Credentials

This service holds **no model-provider credentials** — it calls no LLM and no search
API. What it needs:

| Value | Where it comes from | Variable |
| --- | --- | --- |
| **Master key** | you generate it — and it must match the stack's | `CHORA_LOCAL_KEK` |
| PostgreSQL password | the stack's `POSTGRES_PASSWORD`, carried into both DSNs as `chora_ai_kernel_app_rw` | `CHORA_CLOSURE_PG_DSN`, `CHORA_AI_KERNEL_PG_DSN` |
| Object-storage credentials | the stack's Garage key and secret | `CHORA_MINIO_ACCESS_KEY`, `CHORA_MINIO_SECRET_KEY` |
| NATS endpoint | the stack's NATS service; no credential locally | `CHORA_NATS_URL` |

**The master key is not optional and not decorative.** Every per-user data-encryption
key is a 32-byte value wrapped by this master key with AES-256-GCM, using tenant and
GCID as additional authenticated data. Generate it once:

```sh
openssl rand -base64 32
```

It must decode to **exactly 32 bytes**. This service is one of the two in the stack
that refuse to start on a bad value — the failure is `CHORA_LOCAL_KEK is not valid
base64` or `invalid KEK: decoded length N, want 32`, and the process exits rather
than running without envelope encryption. Keep the same value: changing it makes
every previously wrapped key unreadable.

Where the stack's shared credentials come from, and how to obtain the provider keys
the *other* services need, is documented once in
[`chora-stack/README.md`](../chora-stack/README.md#credentials).

## Run it in the stack

`compose.yml` does four things:

1. Builds the image from the sibling checkout at `../chora-closure-orchestrator`.
2. Runs `closure-migrate` first — a `postgres:18.6-alpine` one-shot applying this
   repository's migrations to `chora_ai_kernel`, using the
   `chora_ai_kernel_migrate` role.
3. Starts the service once `postgres` and `nats` are healthy, `nats-init` and
   `garage-init` have completed, the `garage` service is healthy, and the migrate
   one-shot has completed successfully. It needs Garage healthy because the cold
   archive is written there.
4. Publishes no host port; it `expose`s `8080` to the Compose network only.

```sh
cd chora-stack
docker compose up -d closure-orchestrator
docker compose ps closure-orchestrator
```

Compose sets **two** PostgreSQL DSNs by design — `CHORA_CLOSURE_PG_DSN` for the saga
and checkpointer path, and `CHORA_AI_KERNEL_PG_DSN` for the live kernel adapters
(coordinator repository, outbox, wrapped-DEK store).

**One inconsistency to know about.** `adapter/secrets/resolver.py` documents
`CHORA_CLOSURE_PG_DSN` as the **`chora_identity`** DSN, but `compose.yml` points it
at `chora_ai_kernel` — the same database the kernel DSN uses. The stack therefore
runs both paths against one database. Compose states the intent explicitly
(`# Shares the chora_ai_kernel database by design: its saga state lives in …`), so
treat the shared database as deliberate and the resolver docstring as the stale
half. Deploying outside the stack, either give the saga its own database as the
resolver intends, or share one knowingly — but do not read the docstring as the
deployment contract.

It also sets `CHORA_NATS_URL`,
`CHORA_SOURCE_PROJECT`, the four `CHORA_MINIO_*` values (against
`http://garage:3900`), `CHORA_LOCAL_KEK` from the stack's `.env`, and
`OTEL_EXPORTER_OTLP_ENDPOINT`.

Probe it from inside the network:

```sh
docker compose exec -T nats wget -qO- http://closure-orchestrator:8080/readyz
```

## Run it standalone

```sh
git clone https://github.com/apollo-chora/chora-closure-orchestrator.git
cd chora-closure-orchestrator
uv sync --all-groups
cp .env.example .env
uv run uvicorn chora_closure_orchestrator.main:app --host 0.0.0.0 --port 8080
```

`.env.example` is deliberately minimal — it carries only `CHORA_NATS_URL` and
`CHORA_SOURCE_PROJECT`. The [Configuration](#configuration) table below is the real
reference; the example file is not a complete deployment configuration.

For a run with no PostgreSQL, no broker and no object store, switch to the in-memory
adapters:

```sh
CHORA_CLOSURE_FAKE_ADAPTERS=true uv run uvicorn chora_closure_orchestrator.main:app --port 8080
```

That is a development escape hatch — `readyz` reports the mode it is actually
running in, so you can tell. Do not use it for anything you care about: with fake
adapters the saga state does not survive a restart and nothing is archived.

Container instead:

```sh
docker build -t chora-closure-orchestrator .
docker run --rm -p 8080:8080 --env-file .env chora-closure-orchestrator
```

The image runs as UID `65532` and starts Uvicorn on `8080`. The build installs the
wheel from the repository itself, including the vendored `chora_contracts_gen`
package, so no sibling `chora-contracts` checkout is needed.

Development checks:

```sh
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
uv run pytest --cov        # 85 percent floor
```

`pytest` runs with strict markers; the `integration` marker covers the end-to-end
saga behaviour (closure, cancellation, compensation).

## Configuration

Every setting is an environment variable.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CHORA_CLOSURE_PG_DSN` | unset | PostgreSQL DSN for the saga and checkpointer path |
| `CHORA_CLOSURE_PG_DSN_SECRET_ID` | unset | secret alias resolved to the above when the direct DSN is absent |
| `CHORA_AI_KERNEL_PG_DSN` | unset | PostgreSQL DSN for the live kernel adapters (coordinator repo, outbox, wrapped-DEK store) |
| `CHORA_AI_KERNEL_CONNINFO` | unset | alias for `CHORA_AI_KERNEL_PG_DSN` |
| `CHORA_AI_KERNEL_PG_DSN_SECRET_ID` | unset | secret alias for the kernel DSN |
| `CHORA_LOCAL_KEK` | unset | base64 **32-byte** master key for the local KMS adapter |
| `CHORA_NATS_URL` | `nats://localhost:4222` | NATS broker URL |
| `CHORA_SOURCE_PROJECT` | unset | logical event source; also what enables the outbox dispatcher once a DSN is configured |
| `CHORA_COLD_ARCHIVE_BUCKET` | unset | object-storage bucket for cold archives |
| `CHORA_MINIO_ENDPOINT` | unset | object-storage endpoint (stack: `http://garage:3900`) |
| `CHORA_MINIO_ACCESS_KEY` | unset | object-storage access key |
| `CHORA_MINIO_SECRET_KEY` | unset | object-storage secret key |
| `CHORA_MINIO_SECURE` | unset | TLS for the object-storage client |
| `CLOSURE_ACK_SUBSCRIPTIONS` | the ten canonical `chora.{domain}.account.pseudonymised.v1` subjects | comma-separated acknowledgement-subject override |
| `CLOSURE_ACK_TIMEOUT_SECONDS` | `86400` | domain-ack timeout before `PSEUDONYMISE_PARTIAL` |
| `CLOSURE_DRIVER_POLL_SECONDS` | `30` | saga-driver polling interval |
| `CLOSURE_OUTBOX_POLL_SECONDS` | `0.5` | outbox polling interval |
| `CHORA_CLOSURE_FAKE_ADAPTERS` | `false` | force in-memory/fake adapters (development only) |
| `PORT` | `8080` | HTTP listen port |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/gRPC tracing endpoint |

`CHORA_COLD_ARCHIVE_BUCKET` is **not set** by `compose.yml`, and that decides which
archive adapter runs: `wiring.py` selects the MinIO client only when the value is
non-empty (`archive_kind="minio" if bucket else "inmem"`). With it unset the service
falls back to an in-memory archive client using the default bucket name
`chora-cold-archive-dev`, so nothing reaches Garage. Set it — and make sure the
bucket exists — before relying on a cold archive surviving a restart.

## Verify it works

### It is up

```sh
docker compose exec -T nats wget -qO- http://closure-orchestrator:8080/healthz
docker compose exec -T nats wget -qO- http://closure-orchestrator:8080/readyz
```

```json
{"status":"ok","time":"..."}
{"status":"ready","mode":"real","time":"..."}
```

`readyz` reports a **mode**, but do not read it as an adapter audit. `"real"` is set
by the lifespan as soon as the runtime starts, and the lifespan starts whenever the
plan is not entirely fake *or* the plan carries acknowledgement subscriptions —
which the default ten-domain plan always does. In practice a running process whose
archive is fully in-memory still reports `"real"`. The dedicated sections below are
the checks that actually distinguish durable from ephemeral.

### Its migrations landed

```sh
docker compose exec -T postgres psql -U chora_ai_kernel_migrate -d chora_ai_kernel \
  -c "select count(*) from schema_migrations;"
```

The count covers every service that migrates this database, not just this one — see
[Migrations](#migrations) for why.

### It can start and answer for a saga

The routes authenticate with `Bearer <GCID>`. A status read for an unknown id is the
cheapest proof that routing and auth work without starting a real closure:

```sh
docker compose exec -T nats wget -qO- \
  --header='Authorization: Bearer 1ebe8ad8-218d-5185-ab8a-f64fcf558f8d' \
  http://closure-orchestrator:8080/v1/closure/0197a000-0000-7000-8000-000000000000/status
```

A `404` (unknown closure) or a `200` with saga state is the pass; a `401` means the
header did not reach the route, and a connection error means the service is not up.

## Troubleshooting

- **The service exits at startup with `CHORA_LOCAL_KEK is not valid base64` or
  `invalid KEK: decoded length N, want 32`** — the master key is missing, malformed,
  or a hex string where base64 is expected. Generate one with
  `openssl rand -base64 32`; it must decode to exactly 32 bytes.
- **Previously archived data can no longer be decrypted** — the master key changed.
  Every wrapped DEK was sealed under the old one. Restore the original value; there
  is no re-wrap path.
- **`readyz` does not report `"mode":"real"`** — the process is running with fake
  adapters. `CHORA_CLOSURE_FAKE_ADAPTERS` is set, or the real adapters could not be
  wired at startup. A fake-adapter process loses saga state on restart and archives
  nothing.
- **Cold archives vanish when the container restarts** — `CHORA_COLD_ARCHIVE_BUCKET`
  is unset, so `wiring.py` keeps the in-memory archive client (bucket name
  `chora-cold-archive-dev`) and nothing is written to object storage. Set the
  variable to a bucket that exists; the stack's `garage-init` does not create one.
- **The migrate one-shot cannot find the `chora_ai_kernel` database or its role** —
  that database and the `chora_ai_kernel_migrate` / `_app_rw` roles are created by
  the stack's `postgres-init` scripts, which run only when PostgreSQL initializes its
  data directory for the first time. On a re-used volume where they did not complete,
  starting this service alone will not create them.
- **Archive writes fail** — the cold-archive bucket must exist and the `CHORA_MINIO_*`
  credentials must be allowed to write it. In the stack `garage-init` creates the
  buckets; anywhere else you create them yourself.
- **A closure never leaves `CLOSING`** — the grace period has not expired, or the
  saga driver is not running. Check `CLOSURE_DRIVER_POLL_SECONDS` and the logs; the
  driver advances the saga, it is not triggered by the request.
- **A closure sits in `PSEUDONYMISE_PARTIAL`** — one or more domains never
  acknowledged within `CLOSURE_ACK_TIMEOUT_SECONDS`. `GET .../status` lists the
  per-domain acknowledgements, which names the missing domain.
- **Fan-out targets the wrong subjects** — `CLOSURE_ACK_SUBSCRIPTIONS` is set and
  overrides the ten canonical subjects. Unset it to use the defaults derived from
  `REQUIRED_DOMAINS`.
- **The status route returns `404` for a saga you just created** — the id must be the
  one the create call returned, and the `Bearer` GCID must be the account that owns
  it.

## API surface

Five routes. All closure routes authenticate with `Bearer <GCID>`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness, including the adapter mode |
| `POST` | `/v1/closure/request` | Start a closure saga |
| `POST` | `/v1/closure/{closure_id}/cancel` | Cancel during the grace period |
| `GET` | `/v1/closure/{closure_id}/status` | Current saga state, history, and domain acknowledgements |

An operator fast-close passes `fast_close: true` and needs the gateway to stamp
`X-Chora-Role: PLATFORM_OPERATOR`; without that header the request is treated as an
ordinary closure and still honours the grace period.

## Architecture

```text
src/chora_closure_orchestrator/
  main.py                     FastAPI entrypoint and service startup
  wiring.py                   Runtime adapter and saga wiring
  adapter/
    coldarchive/              object-storage cold-archive adapter
    kms/                      envelope encryption and wrapped-key storage
    pii_map/                  PII closure-map loading
    postgres/                 PostgreSQL runtime helpers
    pubsub/                   NATS subjects, subscribers, publisher, outbox
    repository/               closure repository ports and implementations
    secrets/                  DSN and secret resolution
  domain/
    closure/                  saga coordinator, states, domain errors
    state/                    shared state types
  orchestrators/
    closure_graph.py          the LangGraph closure workflow
    saga_driver.py            background saga advancement
src/chora_contracts_gen/      vendored generated Protobuf bindings
config/                       PII closure configuration
migrations/                   PostgreSQL schema
```

The saga logic is in `domain/closure/coordinator.py` — a pure state machine with
`can_transition` and `is_cancellable` helpers — while `orchestrators/` runs it. That
separation is why the state machine can be tested without a database or a broker.

## Events

Events go through a PostgreSQL transactional outbox and are then published to NATS
JetStream, so a crash between the state change and the publish replays rather than
losing the event.

Two wire formats are in play:

- **Lifecycle events** under `chora.closure.*` use the vendored Protobuf bindings in
  `src/chora_contracts_gen`.
- **Per-domain pseudonymisation requests and acknowledgements** use JSON.

On the inbound side the service subscribes to the ten acknowledgement subjects —
`chora.{domain}.account.pseudonymised.v1` for each entry in `REQUIRED_DOMAINS` —
unless `CLOSURE_ACK_SUBSCRIPTIONS` overrides the list.

The outbox dispatcher is enabled by `CHORA_SOURCE_PROJECT` together with a
configured DSN; with either missing, events accumulate in the outbox and are never
published.

## Migrations

Five forward migration files, with mixed suffixes:

- `0050_closure_initial.sql`
- `0051_closure_outbox.sql`
- `0052_closure_saga_platform_policy.up.sql`
- `0053_closure_pseudonymise_partial.up.sql`
- `0054_closure_user_dek_wrap.up.sql`

They are applied by the **stack's** `scripts/migrate.sh` — the runner lives in
`chora-stack` and is mounted into the `closure-migrate` one-shot, it is not shipped
in this repository. A standalone deployment needs that script or an equivalent.

**They target the `chora_ai_kernel` database, not a database of their own** — the
same one `chora-ai-kernel-orchestrator` migrates, with the same
`chora_ai_kernel_migrate` role. The `0050–0054` numbering is contiguous with the
kernel's `0001–0010` and `0055–0063` range, so a `schema_migrations` count on that
database reflects both services. Expect that when you read the count as a health
check.

The `*.down.sql` siblings are destructive rollbacks and are never applied: the
runner takes every `*.sql` except `*.down.sql`, records a SHA-256 checksum per file,
and refuses a file whose contents changed after it was applied. A session-scoped
advisory lock serialises concurrent migrators, which matters here precisely because
two services migrate the same database.
