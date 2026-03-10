# Stage 1: Build
FROM golang:1.26-alpine AS builder

ARG VERSION=dev

RUN apk add --no-cache git make

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o bin/naladb ./cmd/naladb

# Stage 2: Runtime
FROM alpine:3.23

LABEL org.opencontainers.image.title="NalaDB"
LABEL org.opencontainers.image.description="Temporal key-value graph database with causal ordering"
LABEL org.opencontainers.image.url="https://github.com/thatscalaguy/naladb"
LABEL org.opencontainers.image.source="https://github.com/thatscalaguy/naladb"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S naladb && adduser -S -G naladb naladb \
    && mkdir -p /data && chown naladb:naladb /data

COPY --from=builder /src/bin/naladb /usr/local/bin/naladb

USER naladb
VOLUME /data
EXPOSE 7301 7400 9090
ENTRYPOINT ["naladb"]
