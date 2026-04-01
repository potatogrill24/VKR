// cmd/history-consumer/main.go
// History Consumer — читает события из Kafka и записывает в PostgreSQL
// для построения исторических отчётов и агрегации метрик.
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

	pool, err := storage.WaitForDB(ctx, dsn, 30)
	if err != nil {
		log.Fatalf("history-consumer: cannot connect to postgres after retries: %v", err)
	}
	defer pool.Close()

	if err := storage.CheckSchema(ctx, pool); err != nil {
		log.Printf("history-consumer: schema check warning: %v", err)
	}

	topic := kafka.CallsTopicFromEnv()
	reader := kafka.NewReader(topic, "history-consumer")
	defer reader.Close()

	log.Printf("history-consumer: started, topic=%s", topic)

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
    customer_phone,
    queue_id,
    call_type,
    started_at,
    answered_at,
    ended_at,
    status,
    disconnect_reason,
    wait_seconds,
    talk_seconds,
    hold_seconds,
    wrap_up_seconds,
    transfer_count,
    is_first_call_resolution,
    customer_rating,
    sentiment_score,
    sla_met,
    ivr_path,
    skill_used
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
) ON CONFLICT (call_id) DO NOTHING`

	_, err := pool.Exec(ctx, q,
		c.CallID,
		nullIfEmpty(c.AgentID),
		c.CustomerPhone,
		c.Queue,
		c.CallType,
		c.StartedAt,
		c.AnsweredAt,
		c.EndedAt,
		c.Status,
		c.DisconnectReason,
		c.WaitSeconds,
		c.TalkSeconds,
		c.HoldSeconds,
		c.WrapUpSeconds,
		c.TransferCount,
		c.IsFirstCallResolution,
		c.CustomerRating,
		c.SentimentScore,
		c.SlaMet,
		nullIfEmpty(c.IVRPath),
		nullIfEmpty(c.SkillUsed),
	)

	return err
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}