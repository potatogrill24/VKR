// cmd/analyser/main.go
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/contact-center-monitoring/internal/storage"
)

func main() {
	dsn := os.Getenv("ANALYSER_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	ctx := context.Background()

	// Ждем PostgreSQL с retry (30 попыток)
	pool, err := storage.WaitForDB(ctx, dsn, 30)
	if err != nil {
		log.Fatalf("analyser: cannot connect to postgres after retries: %v", err)
	}
	defer pool.Close()

	// Создаем схему БД
	if err := storage.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("analyser: ensure schema error: %v", err)
	}

	interval := 2 * time.Minute

	if v := os.Getenv("ANALYSER_INTERVAL_MINUTES"); v != "" {
		if m, err := time.ParseDuration(v + "m"); err == nil {
			interval = m
		}
	}

	log.Printf("analyser: started, interval=%s", interval)

	if err := runAggregation(ctx, pool); err != nil {
		log.Printf("analyser: initial aggregation error: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := runAggregation(ctx, pool); err != nil {
			log.Printf("analyser: aggregation error: %v", err)
		}
	}
}

func runAggregation(ctx context.Context, pool *pgxpool.Pool) error {
	log.Println("analyser: starting aggregation tick...")

	const q = `
WITH last_hour AS (
    SELECT *
    FROM calls
    WHERE started_at >= NOW() - INTERVAL '1 hour'
)
INSERT INTO global_metrics (name, value, time_window, calculated_at)
VALUES
    ('avg_wait_seconds', COALESCE((SELECT AVG(wait_seconds)::FLOAT8 FROM last_hour), 0), '1h', NOW()),
    ('calls_count',      COALESCE((SELECT COUNT(*)::FLOAT8        FROM last_hour), 0), '1h', NOW()),
    ('avg_talk_seconds', COALESCE((SELECT AVG(talk_seconds)::FLOAT8 FROM last_hour), 0), '1h', NOW()),
    ('sla_percent',      COALESCE((SELECT AVG(CASE WHEN sla_met THEN 100 ELSE 0 END)::FLOAT8 FROM last_hour), 0), '1h', NOW()),
    ('abandoned_calls',  COALESCE((SELECT COUNT(*)::FLOAT8 FROM last_hour WHERE status = 'abandoned'), 0), '1h', NOW());
`

	_, err := pool.Exec(ctx, q)
	if err != nil {
		return err
	}

	log.Println("analyser: aggregation tick completed successfully")
	return nil
}