FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o history-consumer ./cmd/history-consumer

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/history-consumer /app/history-consumer

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_CALLS_TOPIC=ccm.calls
ENV HISTORY_PG_DSN=postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable

ENTRYPOINT ["/app/history-consumer"]

