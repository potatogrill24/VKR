package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/contact-center-monitoring/internal/storage"
)

// В реальном проекте Analyser будет читать агрегированные данные из PostgreSQL
// (или напрямую из Kafka/Redis) и считать сложные метрики раз в N часов.
// Здесь реализован упрощенный каркас, который просто логирует запуск задачи.

func main() {
	dsn := os.Getenv("ANALYSER_PG_DSN")
	if dsn == "" {
		// по умолчанию ожидаем сервис postgres из docker-compose
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("analyser: cannot connect to postgres: %v", err)
	}
	defer pool.Close()

	interval := 2 * time.Hour
	if v := os.Getenv("ANALYSER_INTERVAL_MINUTES"); v != "" {
		if m, err := time.ParseDuration(v + "m"); err == nil {
			interval = m
		}
	}

	if err := storage.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("analyser: ensure schema error: %v", err)
	}

	log.Printf("analyser: started, interval=%s", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := runAggregation(ctx, pool); err != nil {
			log.Printf("analyser: aggregation error: %v", err)
		}
		<-ticker.C
	}
}

func runAggregation(ctx context.Context, pool *pgxpool.Pool) error {
	// Пример простой агрегации:
	// - среднее время ожидания по всем звонкам за последний час
	// - общее количество звонков за последний час

	const q = `
WITH last_hour AS (
    SELECT *
    FROM calls
    WHERE started_at >= NOW() - INTERVAL '1 hour'
)
INSERT INTO global_metrics (name, value, window, calculated_at)
VALUES
    ('avg_wait_seconds', COALESCE((SELECT AVG(wait_seconds)::FLOAT8 FROM last_hour), 0), '1h', NOW()),
    ('calls_count',      COALESCE((SELECT COUNT(*)::FLOAT8        FROM last_hour), 0), '1h', NOW());
`

	_, err := pool.Exec(ctx, q)
	if err != nil {
		return err
	}

	log.Println("analyser: aggregation tick completed")
	return nil
}

