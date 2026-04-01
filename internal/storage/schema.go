// internal/storage/schema.go
// Пакет storage предоставляет утилиты для работы с PostgreSQL.
// Схема БД создаётся через init-скрипты в deployments/init-db/
package storage

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WaitForDB пытается подключиться к БД с повторными попытками.
// Используется сервисами для ожидания готовности PostgreSQL при старте.
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

// CheckSchema проверяет, что необходимые таблицы существуют
func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	tables := []string{"calls", "agents", "queues", "global_metrics"}

	for _, table := range tables {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_name = $1
			)
		`, table).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			log.Printf("WARNING: Table '%s' does not exist. Run init-db scripts.", table)
		}
	}

	log.Println("Database schema check completed")
	return nil
}
