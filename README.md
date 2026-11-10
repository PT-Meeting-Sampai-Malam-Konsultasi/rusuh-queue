# rusuh-queue

Go `queue` service for Rusuh Booking Tiket: device identity, waiting room, admission controller, claim, SSE stream
and the `stream:booking` consumer. Design: `../rusuh-documentation` (ADR-0003, 0004, 0011, 0013, 0015).

- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md): scope per project item, design, milestones M0–M6 to 13 Nov.
- [AGENTS.md](AGENTS.md): rules for people and AI agents changing this code. The full guideline lives in
  `../rusuh-documentation/ai-guideline/`.

## Architecture

Domain-based hexagonal architecture (Ports & Adapters), the same layout as the team's `boilerplate-golang`:

```text
internal/<domain>/
├── port/
│   ├── inbound/        I<Domain>Usecase: what the domain offers (driving port)
│   └── outbound/       I<Name> interfaces: what the domain needs (driven ports)
├── domain/
│   ├── entity/         business objects, config, errors
│   └── dto/            data passed through the outbound ports
├── application/
│   └── usecase/        implements the inbound port (init.go holds the struct and New<Domain>)
└── infrastructure/
    ├── http/           handlers (init.go, <domain>.go, router.go)
    ├── valkey/         Valkey adapter (Lua functions)
    └── postgres/       PostgreSQL adapter (sqlc in sqlcgen/)
```

| Domain | Does | Routes |
| --- | --- | --- |
| `device` | Device token (Turnstile / attestation), JWKS | `POST /v1/devices`, `GET /.well-known/jwks.json` |
| `queue` | Join, my place, leave, frontier, claim | `/v1/queue/{eventId}/join`, `/me`, `/frontier`, `/claim` |
| `controller` | Leader election, 1 s tick, purge, checkpoint (background) | none |
| `admin` | Queue settings per event | `PATCH /admin/events/{eventId}/queue` (501 until M5) |
| `stream` | SSE near the front | `GET /v1/queue/{eventId}/stream` (501 until M5) |

`internal/delivery` is the composition root (builds clients, wires adapters → usecase → handler), `cmd/main.go` the
entrypoint, `internal/config` the env config, `_mock/<domain>/port/{inbound,outbound}` the generated mocks, `pkg/`
shared libraries (tokens, rate limit, Valkey, problem details, health, …) and `lua/rusuh_q.lua` the Valkey function
library.

## Run locally

```bash
cp example.env .env
make deps-up        # PostgreSQL 16 + Valkey 8 in Docker
make migrate-up     # goose migrations for the queue schema
make run            # http://localhost:8080/healthz, /readyz
```

### Valkey configuration

Local Docker Valkey uses `VALKEY_TLS_ENABLED=false` and an empty `VALKEY_PASSWORD`.
For ECS with AWS ElastiCache, set `VALKEY_TLS_ENABLED=true` and inject
`VALKEY_PASSWORD` from AWS Secrets Manager. TLS requires version 1.2 or newer and
uses normal certificate verification; the service does not bypass certificate checks.

Web and mobile can build against the contract with the mock (it serves the spec's response examples):

```bash
make mock-api       # Prism on http://localhost:4010 (spec paths, no /v1 prefix), reads ../rusuh-documentation/api
```

## Develop

| Task | Command |
| --- | --- |
| Regenerate API types, sqlc, mocks | `make gen` (or `gen-api`, `sqlc`, `mock`) |
| New table | add `db/migrations/0000N_<name>.sql` (goose), queries in `db/queries/<domain>/`, point `sqlc.yaml` at them, `make sqlc` |
| Everything CI runs | `make ci` |
| Coverage (gate: 90%) | `make test-cover` then `make cover` |
| Images | `make image` (buildx, amd64 + arm64) |

Requires Go 1.27 (the `toolchain` line downloads go1.27.3), Docker (integration tests start Valkey and PostgreSQL
containers), `sqlc`, `golangci-lint` and `mockgen`.
