FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o realtime-consumer ./cmd/realtime-consumer

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/realtime-consumer /app/realtime-consumer

ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_CALLS_TOPIC=ccm.calls
ENV REALTIME_HTTP_ADDR=":8080"

ENTRYPOINT ["/app/realtime-consumer"]

