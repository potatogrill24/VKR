// cmd/analyser/main.go
// Analyser Service — периодически агрегирует данные из таблицы calls
// и записывает глобальные метрики в global_metrics для отображения на дашборде.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/contact-center-monitoring/internal/storage"
)

func main() {
	dsn := os.Getenv("ANALYSER_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	ctx := context.Background()

	pool, err := storage.WaitForDB(ctx, dsn, 30)
	if err != nil {
		log.Fatalf("analyser: cannot connect to postgres after retries: %v", err)
	}
	defer pool.Close()

	if err := storage.CheckSchema(ctx, pool); err != nil {
		log.Printf("analyser: schema check warning: %v", err)
	}

	log.Println("analyser: started, interval=1m")

	// Начальная агрегация
	if err := runAggregation(ctx, pool); err != nil {
		log.Printf("analyser: initial aggregation error: %v", err)
	}

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if err := runAggregation(ctx, pool); err != nil {
			log.Printf("analyser: aggregation error: %v", err)
		}
	}
}

func runAggregation(ctx context.Context, pool *pgxpool.Pool) error {
	log.Println("analyser: starting aggregation...")

	// Используем транзакцию с уровнем REPEATABLE READ для защиты от фантомного чтения.
	// Это гарантирует, что все SELECT'ы внутри транзакции видят один и тот же снимок данных,
	// даже если history-consumer вставляет новые записи параллельно.
	txOptions := pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	}

	tx, err := pool.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Метрики за 2 минуты
	if err := aggregateGlobalTx(ctx, tx, "2 minutes", "2m"); err != nil {
		log.Printf("analyser: global 2m error: %v", err)
	}
	if err := aggregateByQueueTx(ctx, tx, "2 minutes", "2m"); err != nil {
		log.Printf("analyser: by queue 2m error: %v", err)
	}

	// Метрики за 10 минут
	if err := aggregateGlobalTx(ctx, tx, "10 minutes", "10m"); err != nil {
		log.Printf("analyser: global 10m error: %v", err)
	}
	if err := aggregateByQueueTx(ctx, tx, "10 minutes", "10m"); err != nil {
		log.Printf("analyser: by queue 10m error: %v", err)
	}
	if err := aggregateByStatusTx(ctx, tx, "10 minutes", "10m"); err != nil {
		log.Printf("analyser: by status 10m error: %v", err)
	}
	if err := aggregateTopAgentsTx(ctx, tx, "10 minutes", "10m"); err != nil {
		log.Printf("analyser: top agents 10m error: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Очистка старых метрик (вне транзакции — не критично)
	if _, err := pool.Exec(ctx, `DELETE FROM global_metrics WHERE calculated_at < NOW() - INTERVAL '1 hour'`); err != nil {
		log.Printf("analyser: cleanup error: %v", err)
	}

	log.Println("analyser: aggregation completed")
	return nil
}

func aggregateGlobalTx(ctx context.Context, tx pgx.Tx, interval, window string) error {
	q := `
INSERT INTO global_metrics (name, value, time_window, queue_id, calculated_at)
SELECT name, value, $1, NULL, NOW()
FROM (
    SELECT 'calls_count' AS name, COUNT(*)::FLOAT8 AS value FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'avg_wait_seconds', COALESCE(AVG(wait_seconds), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'avg_talk_seconds', COALESCE(AVG(talk_seconds), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'avg_handle_time', COALESCE(AVG(wait_seconds + talk_seconds + hold_seconds + wrap_up_seconds), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'sla_percent', COALESCE(AVG(CASE WHEN sla_met THEN 100.0 ELSE 0.0 END), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'abandonment_rate', 
        CASE WHEN COUNT(*) > 0 
            THEN (COUNT(*) FILTER (WHERE status = 'abandoned'))::FLOAT8 / COUNT(*)::FLOAT8 * 100 
            ELSE 0 
        END
    FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'transfer_rate',
        CASE WHEN COUNT(*) > 0
            THEN (COUNT(*) FILTER (WHERE status = 'transferred'))::FLOAT8 / COUNT(*)::FLOAT8 * 100
            ELSE 0
        END
    FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'fcr_rate',
        CASE WHEN COUNT(*) FILTER (WHERE status = 'completed') > 0
            THEN (COUNT(*) FILTER (WHERE is_first_call_resolution = true))::FLOAT8 / 
                 (COUNT(*) FILTER (WHERE status = 'completed'))::FLOAT8 * 100
            ELSE 0
        END
    FROM calls WHERE ended_at >= NOW() - $2::INTERVAL
    UNION ALL
    SELECT 'avg_customer_rating', COALESCE(AVG(customer_rating), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL AND customer_rating IS NOT NULL
    UNION ALL
    SELECT 'avg_sentiment', COALESCE(AVG(sentiment_score), 0) FROM calls WHERE ended_at >= NOW() - $2::INTERVAL AND sentiment_score IS NOT NULL
) AS metrics`

	_, err := tx.Exec(ctx, q, window, interval)
	return err
}

func aggregateByQueueTx(ctx context.Context, tx pgx.Tx, interval, window string) error {
	q := `
INSERT INTO global_metrics (name, value, time_window, queue_id, calculated_at)
SELECT 
    'calls_count' AS name,
    COUNT(*)::FLOAT8 AS value,
    $1 AS time_window,
    queue_id,
    NOW() AS calculated_at
FROM calls 
WHERE ended_at >= NOW() - $2::INTERVAL
GROUP BY queue_id

UNION ALL

SELECT 
    'avg_wait_seconds',
    COALESCE(AVG(wait_seconds), 0),
    $1,
    queue_id,
    NOW()
FROM calls 
WHERE ended_at >= NOW() - $2::INTERVAL
GROUP BY queue_id

UNION ALL

SELECT 
    'sla_percent',
    COALESCE(AVG(CASE WHEN sla_met THEN 100.0 ELSE 0.0 END), 0),
    $1,
    queue_id,
    NOW()
FROM calls 
WHERE ended_at >= NOW() - $2::INTERVAL
GROUP BY queue_id

UNION ALL

SELECT 
    'abandonment_rate',
    CASE WHEN COUNT(*) > 0 
        THEN (COUNT(*) FILTER (WHERE status = 'abandoned'))::FLOAT8 / COUNT(*)::FLOAT8 * 100 
        ELSE 0 
    END,
    $1,
    queue_id,
    NOW()
FROM calls 
WHERE ended_at >= NOW() - $2::INTERVAL
GROUP BY queue_id`

	_, err := tx.Exec(ctx, q, window, interval)
	return err
}

func aggregateByStatusTx(ctx context.Context, tx pgx.Tx, interval, window string) error {
	q := `
INSERT INTO global_metrics (name, value, time_window, queue_id, calculated_at)
SELECT 
    'status_' || status AS name,
    COUNT(*)::FLOAT8 AS value,
    $1 AS time_window,
    NULL AS queue_id,
    NOW() AS calculated_at
FROM calls 
WHERE ended_at >= NOW() - $2::INTERVAL 
GROUP BY status`

	_, err := tx.Exec(ctx, q, window, interval)
	return err
}

func aggregateTopAgentsTx(ctx context.Context, tx pgx.Tx, interval, window string) error {
	q := `
INSERT INTO global_metrics (name, value, time_window, queue_id, calculated_at)
SELECT 
    'agent_calls_' || c.agent_id AS name,
    COUNT(*)::FLOAT8 AS value,
    $1 AS time_window,
    NULL AS queue_id,
    NOW() AS calculated_at
FROM calls c
WHERE c.ended_at >= NOW() - $2::INTERVAL
    AND c.agent_id IS NOT NULL
    AND c.status IN ('completed', 'transferred')
GROUP BY c.agent_id`

	_, err := tx.Exec(ctx, q, window, interval)
	return err
}
