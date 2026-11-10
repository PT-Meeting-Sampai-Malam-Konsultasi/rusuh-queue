.PHONY: run build test test-cover cover clean docker-up docker-down mock sqlc lint vuln fmt-check gen-api gen deps-up migrate-up migrate-down migrate-status mock-api image ci

# Tools run with `go run …@version` so their dependencies never mix with the service's go.mod
GOOSE        := go run github.com/pressly/goose/v3/cmd/goose@v3.29.0
OAPI_CODEGEN := go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.1
OPENAPI_SPEC ?= ../rusuh-documentation/api/openapi.yaml
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/rusuh?sslmode=disable
IMAGE        ?= ghcr.io/pt-meeting-sampai-malam-konsultasi/rusuh-queue
COVER_MIN    ?= 90
COVER_SKIP   := /rusuh-queue/(api|cmd|_mock|internal/testkit)/|/sqlcgen/
TAG          ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

run:
	go run cmd/main.go

build:
	go build -o bin/rusuh-queue cmd/main.go

test:
	# -p 2: WSL Docker port forwarding struggles with many test containers at once
	go test -race -p 2 ./...

unit-test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Gate on coverage.out from `make test-cover`, leaving out generated code, entry points and test helpers
cover:
	@grep -vE '$(COVER_SKIP)' coverage.out > coverage.src.out
	@go tool cover -func=coverage.src.out | awk -v min=$(COVER_MIN) '/^total:/ { sub(/%/, "", $$3); printf "total %.1f%% (minimum %d%%)\n", $$3, min; exit ($$3 < min) }'

test-cover:
	go test -race -p 2 -coverprofile=coverage.out ./...

clean:
	rm -rf bin/

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

deps-up:
	docker compose up -d db valkey

mock:
	go generate ./internal/...

sqlc:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate

gen-api:
	$(OAPI_CODEGEN) -config api/oapi-codegen.yaml $(OPENAPI_SPEC)

gen: gen-api sqlc mock

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt -w ." && exit 1)

migrate-up:
	$(GOOSE) -dir db/migrations postgres "$(DATABASE_URL)" up

migrate-down:
	$(GOOSE) -dir db/migrations postgres "$(DATABASE_URL)" down

migrate-status:
	$(GOOSE) -dir db/migrations postgres "$(DATABASE_URL)" status

mock-api:
	docker compose --profile mock up -d mock-api

image:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE):$(TAG) .

ci: fmt-check lint vuln test-cover cover build
