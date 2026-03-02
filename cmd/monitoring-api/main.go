package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Monitoring API:
// - HTTP JSON-эндпоинты для чтения агрегированных метрик из PostgreSQL
// - В будущей версии можно добавить проксирование realtime-метрик из Redis

type GlobalMetric struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Value     float64   `json:"value"`
	Window    string    `json:"window"`
	CalculatedAt time.Time `json:"calculated_at"`
}

func main() {
	dsn := os.Getenv("MONITORING_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("monitoring-api: cannot connect to postgres: %v", err)
	}
	defer pool.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() {
		_ = rdb.Close()
	}()

	http.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	http.HandleFunc("/api/metrics/global", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		rows, err := pool.Query(ctx, `
			SELECT id, name, value, window, calculated_at
			FROM global_metrics
			ORDER BY calculated_at DESC
			LIMIT 100
		`)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			log.Printf("monitoring-api: query error: %v", err)
			return
		}
		defer rows.Close()

		var result []GlobalMetric
		for rows.Next() {
			var m GlobalMetric
			if err := rows.Scan(&m.ID, &m.Name, &m.Value, &m.Window, &m.CalculatedAt); err != nil {
				http.Error(w, "db error", http.StatusInternalServerError)
				log.Printf("monitoring-api: scan error: %v", err)
				return
			}
			result = append(result, m)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("monitoring-api: encode error: %v", err)
		}
	})

	http.HandleFunc("/api/metrics/realtime", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		val, err := rdb.Get(ctx, "realtime:metrics").Result()
		if err != nil {
			if err == redis.Nil {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"free_agents": 0,
					"in_call":     0,
					"in_queue":    0,
				})
				return
			}
			http.Error(w, "redis error", http.StatusInternalServerError)
			log.Printf("monitoring-api: redis error: %v", err)
			return
		}

		w.Write([]byte(val))
	})

	addr := ":8081"
	if v := os.Getenv("MONITORING_HTTP_ADDR"); v != "" {
		addr = v
	}

	log.Printf("monitoring-api: listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
		log.Fatalf("monitoring-api: server error: %v", err)
	}
}

