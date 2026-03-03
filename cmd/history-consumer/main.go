package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/contact-center-monitoring/internal/kafka"
	"github.com/example/contact-center-monitoring/internal/models"
	"github.com/example/contact-center-monitoring/internal/storage"
)

// history-consumer:
// - читает CallEvent из Kafka-топика ccm.calls
// - записывает их в таблицу calls в PostgreSQL

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := os.Getenv("HISTORY_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("history-consumer: cannot connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := storage.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("history-consumer: ensure schema error: %v", err)
	}

	topic := kafka.CallsTopicFromEnv()
	reader := kafka.NewReader(topic, "history-consumer")
	defer reader.Close()

	log.Printf("history-consumer: started, topic=%s", topic)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("history-consumer: read error: %v", err)
				time.Sleep(time.Second)
				continue
			}

			var evt models.CallEvent
			if err := json.Unmarshal(m.Value, &evt); err != nil {
				log.Printf("history-consumer: unmarshal error: %v", err)
				continue
			}

			if err := insertCall(ctx, pool, &evt); err != nil {
				log.Printf("history-consumer: insert error: %v", err)
				continue
			}
		}
	}()

	<-sigCh
	log.Println("history-consumer: shutting down")
}

func insertCall(ctx context.Context, pool *pgxpool.Pool, c *models.CallEvent) error {
	const q = `
INSERT INTO calls (
    call_id,
    agent_id,
    customer_id,
    queue,
    started_at,
    ended_at,
    status,
    wait_seconds,
    talk_seconds,
    wrapup_seconds,
    sentiment_score,
    sla_met
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12
)`

	_, err := pool.Exec(ctx, q,
		c.CallID,
		c.AgentID,
		c.CustomerID,
		c.Queue,
		c.StartedAt,
		c.EndedAt,
		c.Status,
		c.WaitSeconds,
		c.TalkSeconds,
		c.WrapUpSeconds,
		c.SentimentScore,
		c.SlaMet,
	)

	log.Printf("History-consumer: Calls inserted is database")
	return err
}

