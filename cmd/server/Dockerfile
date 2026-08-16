# Server image: /app/server + /app/client + /app/migrate in ONE image —
# run.sh execs all three from the `server` compose service.
# Build context is the repo root (docker-compose.yml).
FROM golang:1.25 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# Cache the module graph as its own layer.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# datasets/ is optional (populated by scripts/download-dataset.sh before a
# rebuild for locomo/longmemeval runs); make sure the path exists so the
# final-stage COPY never fails.
RUN mkdir -p /src/datasets \
 && go build -o /out/server  ./cmd/server \
 && go build -o /out/client  ./cmd/client \
 && go build -o /out/migrate ./cmd/migrate

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/server /out/client /out/migrate /app/
COPY --from=build /src/testdata /app/testdata
COPY --from=build /src/datasets /app/datasets
EXPOSE 8081
# CMD, not ENTRYPOINT: `docker compose run server /app/migrate up` must
# replace the default process (with an ENTRYPOINT the run.sh commands would
# become arguments to /app/server instead).
CMD ["/app/server"]
