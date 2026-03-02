package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureSchema создает минимально необходимую схему БД, если её ещё нет.
// Таблица calls хранит "сырые" события звонков.
// Таблица global_metrics хранит агрегированные метрики для Monitoring API.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS calls (
    id              BIGSERIAL PRIMARY KEY,
    call_id         TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    queue           TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL,
    wait_seconds    INTEGER NOT NULL,
    talk_seconds    INTEGER NOT NULL,
    wrapup_seconds  INTEGER NOT NULL,
    sentiment_score DOUBLE PRECISION NOT NULL,
    sla_met         BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls (started_at);
CREATE INDEX IF NOT EXISTS idx_calls_queue_started_at ON calls (queue, started_at);

CREATE TABLE IF NOT EXISTS global_metrics (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    value         DOUBLE PRECISION NOT NULL,
    window        TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

	_, err := pool.Exec(ctx, ddl)
	return err
}

