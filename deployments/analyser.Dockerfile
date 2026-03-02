FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o analyser ./cmd/analyser

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/analyser /app/analyser

ENV ANALYSER_PG_DSN=postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable
ENV ANALYSER_INTERVAL_MINUTES=120

ENTRYPOINT ["/app/analyser"]

