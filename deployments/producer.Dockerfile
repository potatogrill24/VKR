FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o producer ./cmd/producer

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/producer /app/producer

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_CALLS_TOPIC=ccm.calls

ENTRYPOINT ["/app/producer"]

