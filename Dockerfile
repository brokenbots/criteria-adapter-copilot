# syntax=docker/dockerfile:1

# Build stage: compile the Go adapter binary with no CGO.
FROM golang:1.26.6-alpine AS builder

WORKDIR /src

# Copy module manifests and fetch dependencies first for layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source tree and build.
COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /bin/criteria-adapter-copilot .

# Final stage: small image with node/npm so the `copilot` CLI can be installed.
# The adapter shells out to `copilot` at runtime; install the CLI globally here.
FROM node:22-alpine

RUN apk add --no-cache ca-certificates git \
    && npm install -g @github/copilot \
    && rm -rf /root/.npm/_cacache

# Create a non-root user for the adapter.
RUN addgroup -S criteria && adduser -S criteria -G criteria
USER criteria

WORKDIR /app

COPY --from=builder /bin/criteria-adapter-copilot /usr/local/bin/criteria-adapter-copilot

# The copilot CLI is on PATH via the global npm install above.
ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT ["/usr/local/bin/criteria-adapter-copilot"]
