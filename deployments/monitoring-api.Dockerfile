FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o monitoring-api ./cmd/monitoring-api

FROM alpine:3.19
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=builder /app/monitoring-api /app/monitoring-api

ENV MONITORING_PG_DSN=postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable
ENV MONITORING_HTTP_ADDR=":8081"

ENTRYPOINT ["/app/monitoring-api"]

