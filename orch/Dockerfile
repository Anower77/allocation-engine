# Single image that contains all three Go binaries (producer, consumer,
# worker). docker-compose picks which one to run per service via `command`.
FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache deps first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer && \
    CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer && \
    CGO_ENABLED=0 go build -o /out/worker   ./cmd/worker

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
# Default to the producer; overridden per service in docker-compose.yml.
ENTRYPOINT ["producer"]
