// cmd/monitoring-api/main.go
// Monitoring API — HTTP сервер для фронтенда.
// Предоставляет эндпоинты для получения глобальных и realtime метрик.
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

	"github.com/example/contact-center-monitoring/internal/storage"
)

type GlobalMetric struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Value        float64   `json:"value"`
	TimeWindow   string    `json:"time_window"`
	Queue        *string   `json:"queue,omitempty"`
	CalculatedAt time.Time `json:"calculated_at"`
}

func main() {
	dsn := os.Getenv("MONITORING_PG_DSN")
	if dsn == "" {
		dsn = "postgres://ccm:ccm@postgres:5432/ccm?sslmode=disable"
	}

	ctx := context.Background()

	pool, err := storage.WaitForDB(ctx, dsn, 30)
	if err != nil {
		log.Fatalf("monitoring-api: cannot connect to postgres: %v", err)
	}
	defer pool.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/api/health", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))

	// Глобальные метрики (последние записи)
	mux.HandleFunc("/api/metrics/global", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleGlobalMetrics(w, r, pool)
	}))

	// Последние метрики по каждому типу (для дашборда)
	mux.HandleFunc("/api/metrics/latest", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleLatestMetrics(w, r, pool)
	}))

	// Метрики по очередям
	mux.HandleFunc("/api/metrics/queues", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleQueueMetrics(w, r, pool)
	}))

	// Realtime метрики из Redis
	mux.HandleFunc("/api/metrics/realtime", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleRealtimeMetrics(w, r, rdb)
	}))

	// Список операторов
	mux.HandleFunc("/api/agents", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleAgents(w, r, pool)
	}))

	// Список очередей
	mux.HandleFunc("/api/queues", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleQueues(w, r, pool)
	}))

	addr := ":8081"
	if v := os.Getenv("MONITORING_HTTP_ADDR"); v != "" {
		addr = v
	}

	log.Printf("monitoring-api: listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
		log.Fatalf("monitoring-api: server error: %v", err)
	}
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func handleGlobalMetrics(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := pool.Query(ctx, `
		SELECT id, name, value, time_window, queue, calculated_at
		FROM global_metrics
		WHERE queue IS NULL
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
		if err := rows.Scan(&m.ID, &m.Name, &m.Value, &m.TimeWindow, &m.Queue, &m.CalculatedAt); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			log.Printf("monitoring-api: scan error: %v", err)
			return
		}
		result = append(result, m)
	}

	json.NewEncoder(w).Encode(result)
}

func handleLatestMetrics(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем последние значения каждой метрики для каждого окна
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ON (name, time_window) 
			name, value, time_window, calculated_at
		FROM global_metrics
		WHERE queue IS NULL
		ORDER BY name, time_window, calculated_at DESC
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	result := make(map[string]map[string]interface{})
	for rows.Next() {
		var name, window string
		var value float64
		var calculatedAt time.Time
		if err := rows.Scan(&name, &value, &window, &calculatedAt); err != nil {
			continue
		}
		if result[window] == nil {
			result[window] = make(map[string]interface{})
		}
		result[window][name] = value
		result[window]["calculated_at"] = calculatedAt
	}

	json.NewEncoder(w).Encode(result)
}

func handleQueueMetrics(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем метрики по очередям для обоих окон (2m и 10m)
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ON (time_window, queue, name) 
			time_window, queue, name, value, calculated_at
		FROM global_metrics
		WHERE queue IS NOT NULL AND time_window IN ('2m', '10m')
		ORDER BY time_window, queue, name, calculated_at DESC
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	// Структура: { "2m": { "support": { "calls_count": 10, ... } }, "10m": { ... } }
	result := make(map[string]map[string]map[string]float64)
	for rows.Next() {
		var window, queue, name string
		var value float64
		var calculatedAt time.Time
		if err := rows.Scan(&window, &queue, &name, &value, &calculatedAt); err != nil {
			continue
		}
		if result[window] == nil {
			result[window] = make(map[string]map[string]float64)
		}
		if result[window][queue] == nil {
			result[window][queue] = make(map[string]float64)
		}
		result[window][queue][name] = value
	}

	json.NewEncoder(w).Encode(result)
}

func handleRealtimeMetrics(w http.ResponseWriter, r *http.Request, rdb *redis.Client) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	val, err := rdb.Get(ctx, "realtime:metrics").Result()
	if err != nil {
		if err == redis.Nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"agents_available": 0,
				"agents_in_call":   0,
				"agents_wrap_up":   0,
				"calls_in_queue":   0,
				"service_level":    0,
				"updated_at":       time.Now(),
			})
			return
		}
		http.Error(w, "redis error", http.StatusInternalServerError)
		log.Printf("monitoring-api: redis error: %v", err)
		return
	}

	w.Write([]byte(val))
}

func handleAgents(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := pool.Query(ctx, `
		SELECT agent_id, full_name, email, primary_queue, skills, hire_date, is_active
		FROM agents
		WHERE is_active = true
		ORDER BY full_name
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	type Agent struct {
		AgentID      string   `json:"agent_id"`
		FullName     string   `json:"full_name"`
		Email        string   `json:"email"`
		PrimaryQueue string   `json:"primary_queue"`
		Skills       []string `json:"skills"`
		HireDate     string   `json:"hire_date"`
		IsActive     bool     `json:"is_active"`
	}

	var result []Agent
	for rows.Next() {
		var a Agent
		var hireDate time.Time
		if err := rows.Scan(&a.AgentID, &a.FullName, &a.Email, &a.PrimaryQueue, &a.Skills, &hireDate, &a.IsActive); err != nil {
			log.Printf("monitoring-api: scan error: %v", err)
			continue
		}
		a.HireDate = hireDate.Format("2006-01-02")
		result = append(result, a)
	}

	json.NewEncoder(w).Encode(result)
}

func handleQueues(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := pool.Query(ctx, `
		SELECT queue_id, queue_name, description, sla_threshold_seconds, priority, is_active
		FROM queues
		WHERE is_active = true
		ORDER BY priority, queue_name
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	type Queue struct {
		QueueID             string `json:"queue_id"`
		QueueName           string `json:"queue_name"`
		Description         string `json:"description"`
		SLAThresholdSeconds int    `json:"sla_threshold_seconds"`
		Priority            int    `json:"priority"`
		IsActive            bool   `json:"is_active"`
	}

	var result []Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.QueueID, &q.QueueName, &q.Description, &q.SLAThresholdSeconds, &q.Priority, &q.IsActive); err != nil {
			log.Printf("monitoring-api: scan error: %v", err)
			continue
		}
		result = append(result, q)
	}

	json.NewEncoder(w).Encode(result)
}

