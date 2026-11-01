# rusuh-queue — Agent Guide

Read the shared guideline first: `../rusuh-documentation/ai-guideline/` (start at its `README.md`). It holds the
project structure steering, the queue service rules, the git / issue workflow and the API tooling. This file is the
short version for this repo.

## Layout (boilerplate-golang, domain hexagonal)

- One folder per domain in `internal/<domain>/`: `port/inbound`, `port/outbound`, `domain/{entity,dto}`,
  `application/usecase`, `infrastructure/{http,valkey,postgres}`.
- Interfaces are I-prefixed: `I<Domain>Usecase` (inbound), `IQueueStore`, `ISigner`, … (outbound).
- Usecases implement the inbound port and depend only on outbound ports; adapters implement outbound ports.
- `internal/delivery/` is the only place that wires domains together (`domains.go`). A domain never imports
  another domain's packages; it declares a narrow outbound port instead (see `controller` → `ISettings`).
- Constructors: `New<Domain>` in `application/usecase/init.go` and `infrastructure/http/init.go`; routes in
  `infrastructure/http/router.go` (`RegisterRoutes(mux, h)`).

## Rules

- **Plan first:** `IMPLEMENTATION_PLAN.md` holds scope, design and milestones. Behaviour comes from
  `../rusuh-documentation` (ADRs, `api/openapi.yaml`); the ADR wins.
- **API types:** `api/types.gen.go` is generated from the OpenAPI spec; never edit it, run `make gen-api`.
- **Mocks:** generated with `make mock` into `_mock/<domain>/port/{inbound,outbound}` (packages `mock_inbound`,
  `mock_outbound`); never hand-edit them.
- **Schemas** live in `db/migrations/` (goose); sqlc reads them. Queries in `db/queries/<domain>/`, generated code
  in `internal/<domain>/infrastructure/postgres/sqlcgen`. Add a migration, then `make sqlc`.
- **Errors:** always `pkg/problem` (RFC 9457 + `api.ErrorCode`). `problem.NotImplemented` only marks stubs and must
  be gone by 13 Nov.
- **Tools** (`goose`, `oapi-codegen`, `sqlc`, `govulncheck`) run through `go run …@version` from the Makefile; never
  as `tool` directives (their dependencies broke the OpenTelemetry versions once).
- **Integration tests** use `internal/testkit` (one shared Valkey / PostgreSQL per test binary, flushed per test;
  never `t.Parallel` with it; tests skip without Docker). They wire domains through `internal/delivery`.
- **Key names** come only from `pkg/qkeys`; documents core writes are parsed with `pkg/coredocs`.
- **Valkey scripts** live in `lua/rusuh_q.lua` (embedded, loaded at start). Keep the previous `_vN` of a function
  until every task runs the new one, unless the change stays compatible both ways (optional trailing ARGV, extra
  output fields). Time comes from `now_ms()` (Valkey `TIME`), never from an argument; durations are arguments from
  config. A key a script creates gets the event's expiry with `ensure_expiry`. Controller writes are fenced.
- **Style:** `../rusuh-documentation/ai-guideline/go-coding-style.md`: comments only for the why, return values
  instead of pointers, no allocations on hot paths without need, Effective Go.
- **Commits:** `[<issue-code>] | david | <message>`, e.g. `[FR-09-Q1] | david | add the wait range to the frontier`.

## Before you finish

`make ci` (gofmt, golangci-lint, govulncheck, race tests, build), and `gofmt -w` on every Go file you changed.
