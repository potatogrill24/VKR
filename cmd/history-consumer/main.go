// cmd/history-consumer/main.go
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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := os.Getenv("HISTORY_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	// Ждем PostgreSQL с retry (30 попыток)
	pool, err := storage.WaitForDB(ctx, dsn, 30)
	if err != nil {
		log.Fatalf("history-consumer: cannot connect to postgres after retries: %v", err)
	}
	defer pool.Close()

	// Создаем схему БД
	if err := storage.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("history-consumer: ensure schema error: %v", err)
	}

	topic := kafka.CallsTopicFromEnv()
	if topic == "" {
		topic = "ccm.calls" // значение по умолчанию
	}

	reader := kafka.NewReader(topic, "history-consumer")
	defer reader.Close()

	log.Printf("history-consumer: started, topic=%s", topic)

	// Канал для подсчета статистики вставок (опционально)
	insertCount := 0
	lastLogTime := time.Now()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
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

				insertCount++
				
				// Логируем статистику раз в минуту, а не после каждой вставки
				if time.Since(lastLogTime) > time.Minute {
					log.Printf("history-consumer: inserted %d calls in last minute", insertCount)
					insertCount = 0
					lastLogTime = time.Now()
				}
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
) ON CONFLICT (call_id) DO NOTHING` 

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

	return err
}