# Multi-arch build (linux/amd64 for the lab, linux/arm64 for ECS Fargate Graviton):
#   docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/<org>/rusuh-queue:<sha> .
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Cross-compile natively on the build host instead of emulating the target
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-w -s" -o /rusuh-queue ./cmd

# Run stage: no shell, non-root. Configuration comes from the environment only;
# .env is never copied into the image.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /rusuh-queue /rusuh-queue

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/rusuh-queue"]
