# Rusuh `queue` service (Go) — Implementation Plan

Written 24 Oct 2026 for Backend 1 (BE-Go). Due with the rest of the app on **Fri 13 Nov 2026**, which leaves 14 working
days from Mon 26 Oct. Source of truth for behaviour: `../rusuh-documentation` (ADR-0003, 0004, 0005, 0011, 0013, 0015,
the Solution Architecture, `api/openapi.yaml`). Tasks come from the project items in
`../docs/project/rusuh-project-items-detailed.md` (local only).

## Problem Statement

`queue` is the Go service that takes the 300,000-device spike. It issues device identities, gives each device one
place per event, admits users only while stock remains, reserves 3 units for each user it admits, and signs the
admission token that `core` trusts. It tells clients about the queue by CDN-cached polling and, near the front, by
Server-Sent Events. Everything after admission belongs to `core` (Node.js).

## Requirements

- Every `queue` operation in `api/openapi.yaml`: `registerDevice`, `joinQueue`, `getMyPlace`, `getFrontier`,
  `streamQueue`, `claimTurn`, `adminUpdateQueue`.
- Admission controller, lottery, purge, reconcile and the `stream:booking` consumer, as in the Solution Architecture.
- No oversell, no admitted user without stock, first come first served, one place per device and per account.
- Stateless tasks: all shared state in Valkey, checkpoints in PostgreSQL (`queue` schema).
- Targets: join p95 < 200 ms, frontier < 50 ms at the origin, claim < 200 ms, 5,000 SSE streams per task.
- Runs on the lab (`../rusuh-test-infra`) and later on ECS Fargate ARM64, from the same multi-arch image.

## How `queue` talks to `core`

There are two backend codebases. At runtime there are four deployments: `queue`, `core-api` and `core-worker` (one
Node.js image in two roles), and `dukcapil-mock`. `queue` and `core` **never call each other over HTTP**; they share
one Valkey cluster and one PostgreSQL instance, each with its own keys and schema.

| # | Link | Direction | How | When |
| --- | --- | --- | --- | --- |
| 1 | Tokens | both | Each signs its own EdDSA tokens and serves `/.well-known/jwks.json`; the other verifies locally with cached keys | Every request with a token |
| 2 | Booking events | `core-worker` → `queue` | Outbox → relay → Valkey Stream `stream:booking` → consumer group `queue` (at least once, acked, dead-letter) | `booking.held`, `booking.paid`, `booking.expired` |
| 3 | Shared state, key + nudge | both | The owner writes a key; sharded pub/sub (`SPUBLISH`) only says "re-read it". Readers also re-read on reconnect and on a timer, so a lost message costs only delay | Stock and sale settings from `core`; queue state from `queue` |
| 4 | The client | `queue` → app → `core` | The app sends the admission token to `core` as `X-Admission-Token` | Detail, attendee check, hold |

**Rule:** anything that changes a counter by a delta (held, paid, expired) goes on the stream, never on plain pub/sub,
because pub/sub has no ack or replay. Pub/sub carries only "latest value wins" state that sits in a key.

| Key (owner writes, other reads) | Channel | Content | Writer cadence |
| --- | --- | --- | --- |
| `core:{evt}:stock` (`core-worker`) | `core:{evt}:changed` | `available` per event from PostgreSQL (reserved units still count as available), `ts`, `version` | After each hold, expiry, cancel; and every 5 s while the sale is live |
| `core:{evt}:sale` (`core` admin/catalog) | `core:{evt}:changed` | `waitingRoomOpensAt`, `saleOpensAt`, `endsAt`, `status` (published / cancelled) | On change |
| `core:incident` (`core` admin) | `core:incident` | TTL freeze on/off, `since` | On change |
| `q:{evt}:frontier` (`queue`) | `q:{evt}:tick` | The frontier JSON (`state` open / paused / closed) | Every tick |

`core`'s catalog reads `q:{evt}:frontier` (or subscribes to `tick`) to show Sold out on Home within a second (FR-03).

## Project items covered

| Item | What `queue` delivers | Milestone |
| --- | --- | --- |
| FR-03 | `state` = `closed` in the frontier; join refuses with 410 `QUEUE_CLOSED` | M2, M3 |
| FR-04 | Device token (Turnstile / attestation), join script, one place per device per event | M1, M2 |
| FR-05 | Controller admits at once when slots and stock are free (same path as everyone) | M3 |
| FR-06 | Frontier writer; `seq` in the queue token; `getMyPlace` after the lottery | M2, M3, M5 |
| FR-07 | 1 s tick, frontier publish to `q:{evt}:tick`, SSE handler, limits, drain, metrics | M3, M5 |
| FR-08 | Claim script (`OK`, `NOT_YET`, `FULL`, `NO_STOCK`, `MISSED`, `EXPIRED`), admission token, `turn` event | M3, M5 |
| FR-09 | `ratePerMin` from the last 5 minutes of admissions, plus the range `rateLow`/`rateHigh` (p25/p75 of 10 window slices, clamped 0.6×–1.5×) and `rateSource` (`prior` = `maxSlots ÷ RATE_PRIOR_ORDER_TIME` during warm-up, architecture 08) | M3 |
| FR-10 | Idempotent join; account attach (one place per account per event) | M2 |
| FR-16 | Release of the 3 − n unused units on `booking.held` (idempotent, correct after a purge) | M4 |
| FR-17, BR-03 | `exp` = claim + 5 min in the admission token, `expiresAt` + `serverNow` | M3 |
| FR-18, BR-04 | Purge 30 s after the lease ends: units back, place removed, user rejoins at the back | M3 |
| FR-19 | Server-only timers (token `exp`, lease scores in Valkey time) | M3 |
| FR-23, BR-06 | On `booking.expired`: n units back, lease and place removed | M4 |
| NFR-01 | Slot cap, rate cap, pause switch | M3, M5 |
| NFR-02 | Reservation at admission, sold-out close | M3 |
| NFR-03 | Stateless tasks, Lua functions, graceful drain | all |
| NFR-04 | Claim, release, purge scripts; reconcile with `core`'s stock snapshot (nudge + every 10 s) | M3, M4 |
| NFR-05 | Strict `seq` order, pre-open lottery, device-bound admission token | M2, M3, M5 |
| NFR-06 | `Cache-Control` on the frontier, cheap reads | M2 |
| NFR-07 | EdDSA tokens with rotation, JWKS for `core` | M1 |
| NFR-09 | Queue metrics from the controller, stream metrics | all, M6 |
| NFR-10 | Consumer with idempotency, `XAUTOCLAIM`, dead-letter stream | M4 |
| BR-07 | Device and account place rules (409 `ACCOUNT_HAS_PLACE_ELSEWHERE`) | M2 |
| OPS-01 (gap) | `adminUpdateQueue`: state, slot cap, rate cap, `streamFrom` | M5 |

**Not in `queue`:** login and OTP (`core` auth), stock in PostgreSQL, holds, payments, email, and the TTL freeze
switch itself (`core`; `queue` only reacts to it, D-11). **FR-27** (push "your turn" in the background) is not decided and stays out.

## Decisions needed before or during M1

Each has a recommendation, so work can start; confirm with Backend 2 (Node.js) by **Wed 28 Oct**. D-1, D-2, D-9 and D-11 go into a new ADR-0016 (key + nudge between services; amends ADR-0013's event section).

| # | Question | Recommendation |
| --- | --- | --- |
| D-1 | How does `queue` learn an event's stock for `unreserved` and the reconcile? The services share no tables. | Key + nudge (24 Oct, proposed): `core-worker` writes `core:{evt}:stock` and `SPUBLISH core:{evt}:changed`. The key sits in the event's hash tag, so the reconcile script reads it atomically with the leases. `unreserved = available − resv`. No `queue_ro` database role, no cross-schema views. |
| D-2 | Where do sale times and queue settings come from? | Sale times from `core:{evt}:sale` (same pattern). Queue settings (slot cap, rate cap, `streamFrom`) in `queue.event_queue_config`, written only by `adminUpdateQueue`. |
| D-3 | How does `core` verify our tokens? | JWT, `alg: EdDSA` (Ed25519), `kid` per key. `queue` serves `GET /.well-known/jwks.json` (to add to `openapi.yaml`). Two keys are live during a rotation. |
| D-4 | How does `queue` verify a logged-in caller on join (account attach)? | `core` serves its own JWKS for access tokens; `queue` checks `sub`, `exp`, `amr = otp`. |
| D-5 | Event payloads on `stream:booking`. | JSON Schema in `rusuh-documentation/api/events/` (new): `id` (outbox id, used for idempotency), `type`, `eventId`, `slotId`, `deviceId`, `units`, `paymentDeadline`, `occurredAt`, `v: 1`. |
| D-6 | Mobile attestation (Play Integrity / App Attest) by 13 Nov? | An `Attestor` interface. Ship Turnstile for web and a sandbox attestor for mobile that only rate-limits; real verifiers are a stretch goal. Say so in the demo. |
| D-7 | Users invited (`seq ≤ served`) who never claim do not use slots, so `served` can run ahead of the slot cap. | Count invitations younger than 30 s as provisional slots: `free = maxSlots − active − invitedPending`. Covered by a controller test. |
| D-8 | Key layout. The docs list `q:{evt}:dev:{device}` per device, but a Lua script must declare every key it touches in cluster mode. | Hashes per event: `q:{evt}:devs` (device → seq), `q:{evt}:accts` (user → device), `q:{evt}:leases` (slot → lease). Same hash tag, declared keys only. Update the Solution Architecture's key table once agreed. |
| D-9 | How does Home learn that the queue is `closed` (FR-03)? | `core` catalog reads `q:{evt}:frontier` when it rebuilds the Home model, or subscribes to `q:{evt}:tick`. `queue` documents the frontier JSON as a contract. |
| D-10 | Who attaches the account to a place after login? The journey has `core` auth writing `q:{evt}:acct:{user}`, which breaks key ownership. | The app calls `joinQueue` again with its access token; `queue` attaches the account (idempotent). Fix journey node LG15. |
| D-12 | How long does an admitted device have to claim its turn? (HiFi 26 Oct: "60 detik untuk masuk") | **60 s** (`MISS_WINDOW`), was 5 min. Clients claim automatically at the turn. The D-7 invitation window follows it; `RATE_WINDOW` (5 min) keeps the history for `ratePerMin`. A missed, used or left place is over: join gives a new number at the back. |
| D-11 | Does the TTL freeze affect `queue`? | Yes, for admitted users without a hold: on `core:incident` = frozen, the controller stops purging and pushes lease ends forward by the frozen time when it lifts. Held leases already wait for `core`'s events. |

## Repository layout

Its own repo `PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue` (Go module
`github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue`). Scaffolded on 24 Oct with justgo (Modular Monolith);
restructured on 31 Oct to the domain-based hexagonal layout of the team's `boilerplate-golang`, keeping justgo's
packages (goleggo/observer, `pkg/database`) instead of the boilerplate's Telkomsel-internal ones. Rules:
`AGENTS.md` and `../rusuh-documentation/ai-guideline/`.

```text
rusuh-queue/
├── cmd/main.go                     # config, observer, delivery.New, graceful shutdown
├── internal/
│   ├── config/                     # config.go (types), load.go (env)
│   ├── delivery/                   # composition root: server.go (clients, health, CORS), domains.go (wiring)
│   ├── device/                     # registerDevice, JWKS, Turnstile, Attestor
│   ├── queue/                      # join, me, leave, frontier, claim
│   ├── controller/                 # leader election, 1 s tick, lottery, purge, reconcile, checkpoint (no routes)
│   ├── admin/                      # queue settings; adminUpdateQueue (M5)
│   ├── stream/                     # SSE hub (M5); route answers 501 until then
│   │   └── each: port/{inbound,outbound}/, domain/{entity,dto}/, application/usecase/, infrastructure/{http,valkey,postgres}/
│   └── testkit/                    # shared Valkey / PostgreSQL containers and script helpers for tests
├── _mock/<domain>/port/{inbound,outbound}/   # mockgen output (make mock)
├── api/                            # oapi-codegen types only, from ../rusuh-documentation/api/openapi.yaml
├── pkg/                            # token, auth, remotejwks, ratelimit, turnstile, attest, clientip, valkeyx,
│                                   # qkeys, coredocs, problem, health, httpmw, database
├── lua/rusuh_q.lua                 # one Valkey library, versioned function names (rusuh_join_v1, …)
├── db/migrations/                  # goose; also the schema sqlc reads
├── db/queries/<domain>/            # sqlc queries for controller and admin (consumer from M4)
├── test/integration/               # HTTP and controller tests on real Valkey and PostgreSQL
├── .github/workflows/ci.yml        # gofmt, vet, golangci-lint, govulncheck, race tests, sqlc drift, multi-arch image
├── Dockerfile                      # distroless, non-root, cross-compiled amd64 + arm64
├── docker-compose.yml              # PostgreSQL 16, Valkey 8 (AOF off), app, Prism mock (profile "mock")
└── Makefile                        # run, test, lint, vuln, gen, migrate-*, deps-up, mock-api, image, ci
```

Libraries: **Go 1.26** (`toolchain go1.26.10`; Go 1.25.10 had reachable standard-library vulnerabilities), std
`net/http` routing (Go 1.22 method patterns), `valkey-go`, `pgx/v5` + `sqlc`, `oapi-codegen` (types),
`golang-jwt/jwt/v5` (EdDSA, M1), OpenTelemetry 1.47 through `github.com/goleggo/observer` v0.1.4,
`testcontainers-go`, `pgregory.net/rapid` for property tests. Tools run with `go run …@version`, never as `tool`
directives.

## Design

### Valkey keys (one event = one hash tag)

| Key | Type | Content |
| --- | --- | --- |
| `q:{evt}:cfg` | hash | `state`, `maxSlots`, `rateCap`, `streamFrom`, `opensAt`, `endsAt`, `expireAt` (copy of D-2); `hold` (operator pause, M5); `histStart`, `histFloor`, `invFloor` (tick bookkeeping) |
| `q:{evt}:seq` | string | last issued sequence number |
| `q:{evt}:served` | string | highest admitted sequence number |
| `q:{evt}:devs` | hash | device → `seq` \| `pre` \| `x:<seq>` (place used or expired) |
| `q:{evt}:accts` | hash | user → device |
| `q:{evt}:pre` | set | devices waiting for the lottery |
| `q:{evt}:active` | sorted set | slot → lease end (ms) |
| `q:{evt}:leases` | hash | slot → `device|seq|units|status|endMs` (status `reserved` \| `held` \| `purged`); `d:<device>` → its live slot |
| `q:{evt}:unreserved` | string | units free to reserve |
| `q:{evt}:resv` | string | units reserved by unheld leases |
| `q:{evt}:held` | string | units held by `core` (M4) |
| `q:{evt}:hist` | sorted set | score `served`, member `<tsMs>:<served>`, one per advancing tick, trimmed to `MISS_WINDOW` (newest kept; `cfg.histFloor` = served at the window start); gives `ratePerMin` and the time a `seq` was reached (`MISSED`) |
| `q:{evt}:inv` | sorted set | same shape, trimmed to 30 s (`cfg.invFloor`), for D-7 |
| `q:{evt}:claimed` | sorted set | slot scored by `seq`: claims of invitations still in the 30 s window (D-7) |
| `q:{evt}:frontier` | string | the JSON served by `getFrontier` |
| `q:{evt}:leader`, `q:{evt}:fence` | string | leader id (PX 3 s) and fencing counter |
| `q:{evt}:done` | hash | processed stream event ids (idempotency), trimmed with the stream |
| `q:{evt}:tick` | sharded pub/sub channel | frontier JSON every tick, for SSE and `core` catalog |
| `core:{evt}:stock`, `core:{evt}:sale` | string (JSON), written by `core` | stock snapshot and sale settings (D-1, D-2); read only |
| `core:{evt}:changed`, `core:incident` | sharded pub/sub / key | nudges from `core`; TTL freeze state (D-11) |
| `q:events` | set | live event ids (controllers scan it) |
| `rl:{scope}:{id}` | string | rate limits |
| `stream:booking`, `stream:booking:dlq` | stream | events from `core` |

All `q:{evt}:*` keys expire at sale end + 1 day. `queue` never writes `core:*` keys, and `core` never writes `q:*` keys.

### Lua functions (library `rusuh_q`, loaded with `FUNCTION LOAD REPLACE` at start)

| Function | Does | Returns |
| --- | --- | --- |
| `rusuh_place_v1` / `rusuh_leave_v1` | Read a device's place with its turn end and missed flag (read-only) / end the place or the draw entry (`leaveQueue`) | place view / `OK` |
| `rusuh_join_v1` | Existing place → same `seq` (a missed place counts as over). Before opening → `SADD pre`, `devs = pre`. Open → `INCR seq`, `HSET devs`. With a user: `HSETNX accts`; another device already owns the account → refuse | `seq` \| `PRE` \| `CLOSED` \| `ACCOUNT_ELSEWHERE` |
| `rusuh_claim_v1` | Checks in order: closed; the device's place matches the token `seq` (else `EXPIRED`); `seq > served` → `NOT_YET`; a live lease for this device → return it again (idempotent); `seq` reached more than 5 min ago → `MISSED`; slots full → `FULL`; `unreserved = 0` → `NO_STOCK`. Then `r = min(3, unreserved)`, `DECRBY unreserved r`, `INCRBY resv r`, `ZADD active`, `HSET leases`, `devs = x:<seq>` | `OK slot r exp` or the code |
| `rusuh_release_v1` | Skips an id already in `done`. `held(n)`: return `resv − n` units, mark held, move the lease end to the payment deadline + 2 min. `paid`: drop the lease. `expired(n)`: `INCRBY unreserved n`, drop the lease. If the purge ran first (lease status `purged`), take back the n units it returned | `APPLIED` \| `DUPLICATE` |
| `rusuh_lead_v1` / `rusuh_resign_v1` | Take or renew `leader` (`PX 3000`); a new lease `INCR`s `fence`. Resign deletes the key only if it is ours | leading, fence, newly acquired |
| `rusuh_purge_v1` | Fenced. Up to 500 leases ended more than 30 s ago and not held: units back to `unreserved`, lease status `purged` (kept, for a late `booking.held`). Held leases past their deadline are left for `core` and counted for an alarm | count purged, count stuck |
| `rusuh_tick_v1` | Fenced (`ARGV fence == GET fence`). `advance = min(maxSlots − active − invitedPending, rateCap, stock room, seq − served)` with stock room `floor((unreserved − 3 × invitedPending) ÷ 3)`, but 1 while at least one unit is left beyond the pending invitations (near sell-out). `hist`, `inv`. State: `preopen` until `opensAt` and an empty draw list / `open` / `paused` (`unreserved = 0` while reserved or held remain, or `cfg.hold`) / `closed` (nothing left, cancelled or past `endsAt`). Writes `frontier`, `SPUBLISH tick` | frontier JSON, advance, state, seq, served, invitedPending |
| `rusuh_lottery_v1` | Fenced. `SPOP pre 1000`, then `INCR seq` + `HSET devs` for each, in random order | devices left in `pre` |
| `rusuh_reconcile_v1` | Fenced. Reads `core:{evt}:stock` (declared key). Skips when the snapshot is older than 30 s. Computes `max(0, available − resv)`; corrects `unreserved` only when the same drift shows on two runs in a row (a snapshot can lag an event in flight) | drift, applied, snapshot age |
| `rusuh_ratelimit_v1` | `INCR` + `PEXPIRE` on first hit | count, ttl |

### Endpoints

All public paths sit under `/v1`, return `application/problem+json` errors with the `ErrorCode` enum, and pass request
id, OpenTelemetry, panic recovery, a 16 KB body limit and an origin allowlist for CORS (no `*`).

| Operation | Auth | Behaviour | Errors |
| --- | --- | --- | --- |
| `registerDevice` `POST /devices` | Turnstile (web) or attestation (mobile) | Verify (2 s timeout), mint a device token (30 days, `sub` = random device id, `plat`) | 400, 429 per IP / subnet |
| `joinQueue` `POST /queue/{evt}/join` | device token (+ optional bearer) | `rusuh_join_v1`; mint a queue token (`evt`, `dev`, `seq`, `exp` = sale end + 1 day) | 409 `ACCOUNT_HAS_PLACE_ELSEWHERE`, 410 `QUEUE_CLOSED`, 429 |
| `getMyPlace` `GET /queue/{evt}/me` | device + queue token | After the lottery: return `seq` and a new queue token | 401 |
| `getFrontier` `GET /queue/{evt}/frontier` | none | Return `q:{evt}:frontier` as is; `Cache-Control: public, max-age=1`; 250 ms in-process cache per task | — |
| `streamQueue` `GET /queue/{evt}/stream` | device + queue token | See Stream below | 401, 409 `TOO_FAR`, 410, 503 `STREAM_FULL` + `Retry-After` |
| `claimTurn` `POST /queue/{evt}/claim` | device + queue token | `rusuh_claim_v1`; mint an admission token (`aud: core`, `evt`, `dev`, `seq`, `slot`, `resv`, `exp` = +5 min); `loginRequired` when no account is attached | 409 `NOT_YET` / `FULL` (`Retry-After: 1–2`) / `NO_STOCK`, 410 `MISSED` / `EXPIRED` / `QUEUE_CLOSED` |
| `adminUpdateQueue` `PATCH /admin/events/{evt}/queue` | admin JWT, private listener only | Update `queue.event_queue_config` and `q:{evt}:cfg`; audit log with `reason` | 400, 401, 403 |
| `GET /.well-known/jwks.json` | none | Public keys for `core` (D-3) | — |
| `GET /healthz`, `GET /readyz` | none, internal | Readiness fails while draining or when Valkey is unreachable | — |

### Admission controller

Runs in every task; per live event exactly one leader does the work.

1. Every second: `SET q:{evt}:leader <task> NX PX 3000`, or renew it if already ours. On a new lease, `INCR fence` and
   keep the value.
2. Before opening: nothing but the frontier (`preopen`). At `opensAt`: `rusuh_lottery_v1` each tick until `pre` is
   empty, then normal ticks.
3. Each tick: `rusuh_purge_v1` → `rusuh_tick_v1` (writes and `SPUBLISH`es the frontier). Reconcile on a
   `core:{evt}:changed` nudge and at least every 10 s. Reload `core:{evt}:sale` on a nudge and every 30 s. Every
   second: upsert `queue.checkpoint (event_id, seq, served, at)`.
4. On any pub/sub reconnect: re-read every `core:*` key before trusting nudges again.
5. Measure tick lag; skip, never queue, a tick that runs late. A stale fence makes every script refuse, so an old
   leader after a pause cannot write.
6. On `SIGTERM` the leader deletes its leader key, so another task takes over within a second.
7. **TTL freeze** (D-11): while `core:incident` is frozen, skip the purge; when it lifts, add the frozen time to
   every unheld lease end.
8. **After Valkey loses data** (failover): pause, restore `seq = checkpoint.seq + 5,000` and `served` from the
   checkpoint, and accept a device's signed queue token on join to restore its place. Stretch goal; at least test
   the pause.

### Stream (ADR-0015)

- One `SSUBSCRIBE q:{evt}:tick` per event per task; a hub fans each message out to that event's local streams.
- Connect: verify tokens, rate limit per device, `seq − served ≤ streamFrom` (else 409 `TOO_FAR`), at most 5,000
  streams per task (else 503 `STREAM_FULL`).
- Headers `Content-Type: text/event-stream`, `Cache-Control: no-store, no-transform`, `X-Accel-Buffering: no`; send
  `retry: 3000` and the current frontier at once.
- Each tick: `event: frontier`, `id: <ts>`. When `seq ≤ served`: `event: turn` with `QueueTurn`, then close.
  On `closed`: the last frontier, then close. `: hb` every 15 s. Close after 10 min ±20%.
- Slow client: a buffered channel of 4 per stream; drop the stream when full, never block the hub.
- The server's `WriteTimeout` must not apply to this route (use `http.ResponseController` per write deadline).
- Drain on `SIGTERM`: stop accepting, close open streams spread over 30 s, then exit
  (`terminationGracePeriodSeconds: 45` on the lab, ECS `stopTimeout` 45 s).

### Consumer (ADR-0013)

- `XGROUP CREATE stream:booking queue $ MKSTREAM` at start; `XREADGROUP GROUP queue <task> COUNT 100 BLOCK 1000`.
- For each entry: validate against the JSON Schema (D-5), `rusuh_release_v1`, then `XACK`.
- Every 10 s: `XAUTOCLAIM` entries idle over 30 s. After 5 deliveries: copy to `stream:booking:dlq`, log a row in
  `queue.consumer_dlq_log`, `XACK`, raise the metric that alerts.
- Until `core`'s relay exists, a small `cmd/fakecore` publishes test events with the same schema.

### Configuration (env)

`HTTP_ADDR`, `ADMIN_ADDR`, `VALKEY_ADDR`, `VALKEY_PASSWORD`, `DATABASE_URL` (role `queue_rw`, `queue` schema
only), `SIGNING_KEYS` (Ed25519 PEMs with `kid`, current first), `CORE_JWKS_URL`,
`TURNSTILE_SECRET`, `ATTESTOR=sandbox|real`, `CORS_ORIGINS`, `STREAM_MAX_PER_TASK=5000`, `STREAM_LIFETIME=10m`,
`HEARTBEAT=15s`, `TICK=1s`, `RECONCILE_EVERY=10s`, `SNAPSHOT_MAX_AGE=30s`, `PURGE_GRACE=30s`, `ORDER_WINDOW=5m`,
`MISS_WINDOW=60s` (MISSED, D-7 invitations), `RATE_WINDOW=5m` (`ratePerMin`, history kept), `RATE_PRIOR_ORDER_TIME=6m` (warm-up speed guess, 0 = off), `RL_JOIN_PER_DEVICE=30`, `RL_CLAIM_PER_DEVICE=60`, `TRUSTED_PROXY_CIDRS`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
`GOMEMLIMIT`.

### Metrics and alerts (NFR-09)

- **Queue (per event, from the leader):** `queue_length` (`seq − served`), `queue_served`, `admit_rate_per_min`,
  `active_slots`, `units_unreserved`, `units_reserved`, `units_held`, `queue_state`, `tick_duration_seconds`,
  `tick_lag_seconds`, `leader_changes_total`, `reconcile_drift_units`, `stock_snapshot_age_seconds`, `purged_total`, `stuck_held_leases`.
- **API:** `join_total{result}`, `claim_total{result}`, `register_total{platform,result}`, request p95 per route,
  `lua_duration_seconds{fn}`.
- **Stream:** `sse_streams_open`, `sse_rejects_total{reason}`, `sse_closed_total{reason}`, `sse_dropped_slow_total`.
- **Consumer:** `consumer_lag_seconds`, `consumer_pending`, `dlq_entries`.
- **Alerts** (lab: Grafana → Telegram/Discord; AWS: CloudWatch): no tick for 3 s, no leader for 5 s, consumer lag
  over 30 s, any DLQ entry, reconcile drift on 3 runs in a row, stock snapshot older than 30 s, stuck held leases, Valkey latency over 50 ms p99.
- Logs: JSON with an allowlist; tokens and device ids are never logged in full.

## Milestones and tasks

Each milestone ends with a **Verify** step. Dates are working days.

### M0 — Skeleton (done 24 Oct, ahead of Mon 26 Oct)

- [x] Scaffold with justgo (hexagonal, std router, postgres, observer); modules `device`, `queue`, `stream`,
      `controller`, `consumer`, `admin`; the 8 public endpoints and JWKS added with `justgo gen module … handler`,
      each returning 501 `NOT_IMPLEMENTED` until its milestone.
- [x] `api/types.gen.go` with `oapi-codegen` (types only); `pkg/problem` (RFC 9457 + `ErrorCode`).
- [x] Config (Valkey, shutdown timeout), observer logs and traces, `/healthz`, `/readyz` (PostgreSQL + Valkey, 503
      while draining), graceful shutdown, server timeouts without a write timeout (SSE).
- [x] `docker-compose.yml` (PostgreSQL 16, Valkey 8 AOF off, app, Prism 4 mock); goose migrations for the `queue`
      schema (`event_queue_config`, `checkpoint` with fence guard, `consumer_dlq_log`); sqlc for 3 modules.
- [x] `.github/workflows/ci.yml` and `make ci`; `.golangci.yml`; `AGENTS.md` + Claude skill from `justgo agents`.
- [x] Push the repo to the org (31 Oct): skeleton on `main`, M1–M3 code on `feature/queue-admission`.
- [x] Restructure to the `boilerplate-golang` domain layout (31 Oct).
- [ ] Pin the OpenAPI spec to a `rusuh-documentation` commit once it is committed.

**Verified locally (24 Oct):** gofmt, vet, golangci-lint 0 issues, govulncheck no vulnerabilities, race tests pass;
migrations up, reset and up again; every route answers (health 200, stubs 501 problem+json, unknown 404, wrong method
405); `/readyz` 503 with Valkey stopped and 200 after; SIGTERM exits cleanly; amd64 and arm64 images build
(`nonroot`, ~30 MB) and the amd64 image passes `/readyz` against the compose stack; Prism mock serves
`/queue/{eventId}/frontier` and `/claim` from the spec (paths without `/v1`). Multi-platform in one build needs the
CI buildx builder; the local Docker driver builds one platform at a time.

### M1 — Tokens and devices (done 24 Oct, ahead of Tue 27 – Wed 28 Oct)

- [x] `pkg/token`: Ed25519 key ring (`SIGNING_KEYS` = `kid=base64(seed)`, signing key first), `typ` per kind
      (`device+jwt`, `queue+jwt`, `admission+jwt`) so kinds cannot be swapped, `kid`, 30 s leeway, EdDSA only.
- [x] JWKS at `GET /.well-known/jwks.json`; contract for Backend 2 in `rusuh-documentation/api/tokens.md`
      (verification steps, unique `slot` per booking against replay, rotation).
- [x] `registerDevice`: rate limit per IP and per /24 (or /48) **before** Turnstile, Turnstile siteverify
      (2 s timeout, optional hostname check), sandbox attestor (D-6); `w_` random ids for web, `m_` ids derived
      from the install id for mobile (same place after an app restart). 400 / 429 / 503 problem responses.
- [x] Valkey function library `lua/rusuh_q.lua` (embedded, `FUNCTION LOAD REPLACE` at start) with
      `rusuh_ratelimit_v1`; `pkg/ratelimit`; `pkg/clientip` (header trusted only from proxy CIDRs).
- [x] CORS allowlist (`CORS_ORIGINS`, no wildcard; foreign origins get no CORS headers).
- [x] Event JSON Schema (D-5): `rusuh-documentation/api/events/booking.v1.schema.json`.
- [x] Spec: `getJwks`, `Jwks` schema, 503 on `registerDevice`, error codes `INTERNAL_ERROR` and
      `VERIFICATION_UNAVAILABLE`.
- [x] Startup refuses to run outside development without `SIGNING_KEYS` or `TURNSTILE_SECRET`.
- [ ] Send `api/tokens.md`, the event schema and D-1 … D-11 to Backend 2 and agree them (**by Wed 28 Oct**).

**Verified (24 Oct):**
- Token tests: wrong key with the same `kid`, `alg: none`, HS256 signed with the public key, expired beyond the
  leeway, wrong kind, wrong audience, unknown `kid`, tampered payload, rotation (old key still verifies, removed key
  fails).
- Integration (Testcontainers Valkey 8, HTTP through the module): a bad Turnstile token gets 400; the 11th request a
  minute from one IP gets 429 with `Retry-After`; a spoofed `CF-Connecting-IP` from an untrusted peer does not dodge
  the limit; a device token verifies with nothing but the published JWKS (as `core` will); bad bodies get 400.
- Limiter: exactly 50 of 200 concurrent hits allowed; a key without TTL is repaired.
- Live: registration through Cloudflare's real siteverify with the test secret → 201; `FUNCTION LIST` shows
  `rusuh_q`; CORS preflight allowed for `CORS_ORIGINS` only.
- `make ci`: lint 0, govulncheck clean (after `moby/go-archive` 0.3.0 for testcontainers), race tests pass.

### M2 — Join, place, frontier (done 24 Oct, ahead of Thu 29 – Fri 30 Oct)

- [x] `rusuh_join_v1`: one place per device per event (idempotent), one per account (`ACCOUNT_ELSEWHERE`), a place
      bound to one account (`DEVICE_OTHER_ACCOUNT`), draw list before opening (`PRE`), back of the queue after a used
      place, keys inherit the event's expiry. `joinQueue` + queue token (`queue+jwt`, sale end + 1 day).
- [x] Account attach (D-4, D-10): optional `Authorization: Bearer`, verified with core's JWKS (`CORE_JWKS_URL`,
      `pkg/remotejwks`: cached 5 min, unknown `kid` refetched at most once a minute); a bad token is a 401, never a
      silent guest. Contract in `rusuh-documentation/api/tokens.md`.
- [x] `getFrontier` from `q:{evt}:frontier`, `Cache-Control: public, max-age=1` (also on its 404), 250 ms in-process
      cache with singleflight.
- [x] `getMyPlace` (seq after the draw, 410 `EXPIRED` for a used place, queue token bound to device and event).
- [x] Per-device join limit (30/min). No per-IP limit on join: carriers put many users behind one CGNAT address.
- [x] Event bootstrap in the controller (`rusuh_bootstrap_v1`): discovers `core:events`, reads `core:{evt}:sale` and
      `core:{evt}:stock` (`pkg/coredocs`, validated), settings from the admin module (defaults 2,000 / 50 / 2,000),
      creates cfg, counters, `unreserved` and the first frontier; cancelled or zero stock → `closed`.
      Contract in `rusuh-documentation/api/shared-keys.md`.
- [x] Sharded pub/sub (`pkg/valkeyx.Subscribe`): `core:events:changed` and `core:{evt}:changed`, resubscribe with
      backoff, re-read on every (re)subscribe; 30 s timer as backstop.
- [x] Checkpoint writer (`queue.checkpoint`, every second, fence 0 until M3's leader).
- [x] Spec: 401/404 on join, 404/410 on me, 404 on frontier.

**Verified (24 Oct):**
- Join script on real Valkey: 10,000 concurrent joins from one device → seq 1 only; 10,000 devices → 1…10,000 with
  no gap or repeat; account rules, draw list, rejoin after a used place, key expiry.
- HTTP: join → me → frontier; 401 for no/forged device token, bad bearer, bearer without `amr=otp` or for another
  audience, queue token of another device or event; 404 unknown event; 410 closed; 409 second device for an account;
  `"seq": null` while waiting for the draw; 429 on the 31st join a minute.
- **Join latency: 2,000 joins/s for 3 s → p50 1.9 ms, p95 2.95 ms, p99 4.1 ms, 0 failures** (race build: p95 60 ms).
- Controller on real Valkey + PostgreSQL: a published event is set up within a second of the nudge (timer off);
  cancel closes it via the per-event nudge; saved settings win; a bad sale document creates nothing; checkpoint rows.
- Live on the compose stack: core documents + `SPUBLISH` → queue created; register → join (seq 1) → me → frontier
  (200, cached 1 s) → checkpoint `evt_demo|1|0`.
- `make ci` green; full suite passed 3 runs in a row.
- Test harness: one Valkey and one PostgreSQL per test binary, flushed per test; a container that WSL Docker leaves
  unreachable is replaced; `go test -p 2`.

### M3 — Controller and claim (done 26 Oct, ahead of Mon 2 – Wed 4 Nov)

- [x] Leader election with fencing (`rusuh_lead_v1`, `rusuh_resign_v1` on SIGTERM); tick loop on its own goroutine
      (a slow sync never delays admission), lag measured, a tick starting more than half a period late skipped;
      `tick_duration_seconds`, `tick_lag_seconds`, `ticks_skipped_total`, `leader_changes_total`, `purged_total`
      on the global meter (exported from M6).
- [x] `rusuh_tick_v1`: advance rule with near-sell-out and D-7 (`q:{evt}:claimed` gives the claims of invitations
      still in the 30 s window), states, `hist`, `ratePerMin`, frontier, `SPUBLISH q:{evt}:tick`. Stock room also
      subtracts 3 units per pending invitation, so near sell-out does not invite users who would get `NO_STOCK`.
- [x] `rusuh_claim_v1` + admission token (`exp` = lease end in Valkey time, `aud: rusuh-core`); a retried claim
      returns the same slot; `loginRequired` for guests; 60 claims a minute per device; `rusuh_purge_v1`.
- [x] Checkpoint by the leader only, with its fence, every second.
- [x] Every timer in the scripts uses Valkey's `TIME` (FR-19); the windows come from config, so tests run the
      5-minute rules in seconds.
- [x] Table tests for the advance rule (14 cases: empty queue, rate cap, full slots, 1–2 units left, paused by
      stock or operator, sold out, cancelled, preopen).
- [x] Spec: 401/404/429 on `claimTurn`, retry and clock notes; `tokens.md` (same slot on retry);
      `shared-keys.md` (frontier states for `core`'s Home labels).

**Verified (26 Oct):**
- Claim race on real Valkey: 1,000 concurrent claims for 2 units → exactly one lease of 2 units, 999 `NO_STOCK`;
  with 10 slots → exactly 10 leases (30 units), 990 `FULL`.
- Two leaders: the old fence's tick and purge are refused and change nothing; a stale checkpoint (lower fence)
  never overwrites a newer one. Two controllers on one event: one leader, fence 1, no flapping; the leader stops
  → the other leads within a tick, fence 2, checkpoint fence 2.
- Lease expiry (order window 0.8 s, grace 0.2 s in the test): units back, old place `EXPIRED`, next user admitted,
  rejoin at the back.
- `MISSED` after the miss window, both from the history and below the trimmed floor.
- FR-05: join → claim 200 within one tick; the admission token verifies as `core` will (`typ`, `aud`, `evt`,
  `slot`, `resv`, `exp` = `expiresAt`), and not for queue's own audience.
- D-7: 5 slots → 5 invited, then 0 while they are pending; claims and aged-out invitations free the room.
- Live on the compose stack (order window 6 s, grace 2 s): 4 units, two devices → 3 + 1 units, retry returns the
  same slot, frontier `paused` → back to `open` with 4 units after the purge, old place 410 `EXPIRED`, rejoin at
  seq 3, checkpoint `evt_live|3|3|1`; SIGTERM removes the leader key.
- `make ci` green (lint 0, govulncheck clean); 24 integration tests + store and service tests pass.
- Fixed on the way: a nil tracer provider panicked at shutdown with `OTEL_EXPORTER_TYPE=none` (M0).

### HiFi contract update (26 Oct)

`rusuh-documentation/analysis/hifi-api-review.md`; contract 0.3.0-draft.
- [x] 60 s turn window (D-12), `turnExpiresAt` in `QueuePlace` and `QueueTurn`, `getMyPlace` 410 `MISSED`, join
      after a missed/used/left place → back of the queue.
- [x] `leaveQueue` (`DELETE /v1/queue/{eventId}/me`).
- [x] Frontier `reason` (`stock`, `operator`, `sold_out`, `cancelled`, `ended`) and `opensAt`; `QueuePlace.served`,
      `opensAt`, `serverNow`.
- [ ] Send the `core` changes (EventSummary, Category, Booking, PaymentView, `method`, PDF) and the conflicts
      H-1…H-6 to Backend 2 and the frontend team.

**Verified:** store tests (missed → new place, account free on another device, leave open and pre-open, frontier
reasons in the 14 advance-rule cases) and HTTP tests (MISSED on me, rejoin seq, `turnExpiresAt`, leave 204 ×2);
`make ci` green.

### M4 — Consumer, release, reconcile (Thu 5 – Fri 6 Nov)

- [x] Consumer group, `rusuh_release_v1`, retry of idle entries (`XPENDING` + `XCLAIM`, which also gives the delivery
      count), dead-letter (1 Nov).
- [x] `rusuh_reconcile_v1` on nudge and every 10 s from `core:{evt}:stock`; stale-snapshot guard (1 Nov).
- [ ] TTL freeze handling (D-11): deferred until `core` builds `adminTtlFreeze`.
- [ ] `cmd/fakecore` for tests until `core` is ready: publishes `stream:booking` events, writes `core:{evt}:stock` / `sale` and nudges. Then the joint test with Backend 2.

**Verify:** the same event delivered 5 times changes state once; `held` after a purge gives the right `unreserved`;
a poison event lands in the DLQ after 5 tries and alerts; reconcile corrects an injected drift on the second run after a nudge; a snapshot older than 30 s stops reconcile and alerts; dropping the pub/sub connection loses no correction (keys re-read).
**Property test** (`rapid`): random joins, claims, holds, pays, expiries and purges never make `unreserved` negative,
and `unreserved + resv == PostgreSQL available` once no event is in flight (reserved units still count as available in PostgreSQL; held ones do not).

### M5 — Stream, admin, lottery (Mon 9 – Tue 10 Nov)

- [ ] SSE hub and handler (limits, heartbeat, lifetime with jitter, slow client drop, drain).
- [ ] `adminUpdateQueue` on the private listener, audit log.
- [x] `rusuh_lottery_v1` and the opening sequence; `getMyPlace` and `claimTurn` after the draw (1 Nov).

**Verify:** through Cloudflare on the lab, events arrive one per second unbuffered and a stream survives 125 s on
heartbeats; `turn` arrives within 1 s of `served` passing `seq`; a pod killed mid-stream → clients reconnect or poll
with no lost place; `streamFrom = 0` → every connect gets 409; 1,000 pre-open devices get a random but complete order.

### M6 — Hardening, load, handover (Wed 11 – Thu 12 Nov)

- [ ] Security tests: forged and cross-device tokens, admission token used after `exp`, replayed claims, rate limits,
      CORS from a foreign origin.
- [ ] k6 on the lab (`../testing-platform`): `queue-join-spike`, `frontier-poll`, `sse-front`, `claim-race`; record
      per-core throughput for AWS sizing.
- [ ] Dashboards and alerts from the metrics list; trigger each alert once.
- [ ] Runbooks: pause or close a queue, Valkey failover, consumer stuck or DLQ replay, leader not ticking, stream
      storm after a deploy, key rotation.
- [ ] README: run locally, config, how `core` verifies tokens.

**Verify:** the full flow with `core` on the lab (join → stream → claim → hold → pay → e-ticket); every alert fired in
a drill; runbooks reviewed by Backend 2.

### Priority if time runs short

1. **Must:** M0–M4 (device, join, frontier, controller, claim, purge, consumer). Polling alone meets FR-07.
2. **Should:** SSE (M5), admin switches, lottery.
3. **Stretch:** real mobile attestation, restore from queue token after Valkey data loss, push at the turn (FR-27).

## Hand-offs

| To | What | By |
| --- | --- | --- |
| Backend 2 (`core`) | Admission token claims, JWKS URL, error codes `core` must return on a bad token | Wed 28 Oct |
| Backend 2 | Access-token JWKS (D-4); relay writing D-5 events; `core-worker` writing `core:{evt}:stock` + nudge and admin writing `core:{evt}:sale` / `core:incident` (D-1, D-2, D-11); app re-join after login (D-10) | Mon 2 Nov |
| Backend 2 | Catalog reading `q:{evt}:frontier` for Sold out (D-9) | Thu 5 Nov |
| Web 1, Mobile | Prism mock URL, frontier and stream behaviour, Turnstile test keys, claim result handling | Mon 26 Oct |
| Web 1, Mobile | Lab URL `https://rusuh-q.bingbing.my.id` | when the lab reaches Phase 6 |
| DevOps | CloudFront behaviours for `/frontier` and `/stream` (ADR-0015) | before staging |

## Testing summary

| Level | Tool | Covers |
| --- | --- | --- |
| Unit | `go test`, table tests | advance rule, ETA, token claims, config |
| Lua | Testcontainers Valkey 8, `rapid` | every function, races, idempotency, invariants |
| Integration | Testcontainers Valkey + PostgreSQL | controller + checkpoint + reconcile, consumer + DLQ |
| Contract | `kin-openapi` response validation against `openapi.yaml` | every operation's status codes and bodies |
| Security | Go tests in `test/security` | forged tokens, IDOR on places, rate limits, CORS |
| Stream | `httptest` + a Go SSE load client | heartbeat, lifetime, cap, drain, slow clients |
| Load | k6 from `../testing-platform` on another host | join spike, polling, 2,000+ streams, claim race |

## Risks

| Risk | Mitigation |
| --- | --- |
| 14 days for a lot of L-size work, plus the lab | Priority order above; build on docker-compose locally and use the lab from M5 |
| `core` contracts late (D-1, D-4, D-5) | `cmd/fakecore` writes the same keys, events and nudges; agree by 28 Oct |
| A pub/sub message is lost | Only key + nudge state goes over pub/sub; timers and reconnect re-reads recover it; deltas stay on the stream |
| Lua bugs oversell or lose units | Every function behind race and property tests; reconcile as the backstop; PostgreSQL `CHECK` as the last guard |
| Hot key on one event at 15,000 joins/s | One shard per busy event; measure Valkey ops/s in the spike test |
| SSE behaves differently through CloudFront | Proven on Cloudflare in the lab; re-test in AWS staging; `streamFrom = 0` |
| H1 feedback repeats (security, transactions, alerting, runbooks) | Security tests, idempotent scripts, alerts and runbooks are tasks in M1, M4 and M6, not extras |

## Doc follow-ups (in `rusuh-documentation`, uncommitted)

- Done 24 Oct: `GET /.well-known/jwks.json` in `openapi.yaml` (D-3).
- Done 24 Oct: `api/events/booking.v1.schema.json` (D-5) and `api/tokens.md`.
- Update the Valkey key table in the Solution Architecture for the hash layout (D-8) and the `core:*` keys
  (contract now in `api/shared-keys.md`).
- ADR-0016 (proposed): key + nudge over sharded pub/sub between services (D-1, D-2, D-9, D-11); amends ADR-0013.
- Solution Architecture: a "How `core` and `queue` interact" section (the four links above); live doc and PDF too.
- Customer journey LG15: account attach by re-join, not by `core` writing `q:*` (D-10).
- Solution Architecture technology table: queue service is Go 1.26 + std `net/http` (not `chi`), scaffolded with justgo.
- Fixed 24 Oct: `Frontier.ts` description in `openapi.yaml` was unquoted and parsed as an extra key.
