# gRPC -> OpenAI-compatible embeddings bridge (Docker Model Runner / nomic).
# Build context is the repo root (docker-compose.yml).
FROM golang:1.25 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/nomicbridge ./tools/nomicbridge

FROM alpine:3.20
COPY --from=build /out/nomicbridge /app/nomicbridge
EXPOSE 9100
ENTRYPOINT ["/app/nomicbridge"]
