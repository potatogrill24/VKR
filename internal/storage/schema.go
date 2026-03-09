// internal/storage/schema.go
package storage

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WaitForDB пытается подключиться к БД с повторными попытками
func WaitForDB(ctx context.Context, dsn string, maxAttempts int) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error

	for i := 0; i < maxAttempts; i++ {
		pool, err = pgxpool.New(ctx, dsn)
		if err != nil {
			log.Printf("Failed to connect to DB (attempt %d/%d): %v", i+1, maxAttempts, err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Проверяем соединение
		err = pool.Ping(ctx)
		if err == nil {
			log.Println("Successfully connected to PostgreSQL")
			return pool, nil
		}

		log.Printf("Ping failed (attempt %d/%d): %v", i+1, maxAttempts, err)
		pool.Close()
		time.Sleep(2 * time.Second)
	}

	return nil, err
}

// EnsureSchema создает необходимые таблицы, если их нет
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	// Таблица для звонков (используется history-consumer)
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS calls (
			id BIGSERIAL PRIMARY KEY,
			call_id VARCHAR(50) UNIQUE NOT NULL,
			agent_id VARCHAR(50),
			customer_id VARCHAR(50),
			queue VARCHAR(50),
			started_at TIMESTAMPTZ,
			ended_at TIMESTAMPTZ,
			status VARCHAR(50),
			wait_seconds INTEGER,
			talk_seconds INTEGER,
			wrapup_seconds INTEGER,
			sentiment_score FLOAT,
			sla_met BOOLEAN,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}

	// Таблица для глобальных метрик (используется analyser)
	// ВНИМАНИЕ: используем time_window вместо window!
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS global_metrics (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			value FLOAT NOT NULL,
			time_window VARCHAR(10) NOT NULL,  -- переименовали с window на time_window
			calculated_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}

	// Индекс для быстрого поиска последних метрик
	_, err = pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_global_metrics_calculated_at 
		ON global_metrics(calculated_at DESC)
	`)
	if err != nil {
		return err
	}

	log.Println("Database schema ensured successfully")
	return nil
}