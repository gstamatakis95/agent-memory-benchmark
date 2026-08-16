# Deterministic mock embedder (tools/mockembedder/main.go is frozen).
# Build context is the repo root (docker-compose.yml).
FROM golang:1.25 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/mockembedder ./tools/mockembedder

FROM alpine:3.20
COPY --from=build /out/mockembedder /app/mockembedder
EXPOSE 9100
ENTRYPOINT ["/app/mockembedder"]
