// cmd/monitoring-api/main.go
// Monitoring API — единая точка входа для фронтенда.
// Предоставляет HTTP эндпоинты и WebSocket для realtime метрик.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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

	// Список операторов
	mux.HandleFunc("/api/agents", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleAgents(w, r, pool)
	}))

	// Список очередей
	mux.HandleFunc("/api/queues", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleQueues(w, r, pool)
	}))

	// Распределение статусов звонков
	mux.HandleFunc("/api/stats/status-distribution", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleStatusDistribution(w, r, pool)
	}))

	// Топ операторов по метрикам
	mux.HandleFunc("/api/stats/top-agents", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		handleTopAgents(w, r, pool)
	}))

	// WebSocket для realtime метрик (единая точка входа)
	hub := NewHub()
	mux.HandleFunc("/ws/realtime", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, hub)
	})

	// Горутина для рассылки realtime метрик из Redis
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			val, err := rdb.Get(ctx, "realtime:metrics").Result()
			if err != nil {
				continue
			}

			var metrics map[string]interface{}
			if err := json.Unmarshal([]byte(val), &metrics); err != nil {
				continue
			}

			hub.Broadcast(metrics)
		}
	}()

	addr := ":8081"
	if v := os.Getenv("MONITORING_HTTP_ADDR"); v != "" {
		addr = v
	}

	log.Printf("monitoring-api: listening on %s (HTTP + WebSocket)", addr)
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
		SELECT id, name, value, time_window, queue_id, calculated_at
		FROM global_metrics
		WHERE queue_id IS NULL
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

	// Получаем все метрики для последнего calculated_at каждого окна
	// Это гарантирует, что все метрики одного окна из одного цикла агрегации
	rows, err := pool.Query(ctx, `
		WITH latest_calc AS (
			SELECT MAX(calculated_at) as calc_at, time_window
			FROM global_metrics
			WHERE queue_id IS NULL AND name = 'calls_count'
			GROUP BY time_window
		)
		SELECT gm.name, gm.value, gm.time_window, gm.calculated_at
		FROM global_metrics gm
		JOIN latest_calc lc ON gm.time_window = lc.time_window AND gm.calculated_at = lc.calc_at
		WHERE gm.queue_id IS NULL
		ORDER BY gm.time_window, gm.name
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

	// Если данных нет, возвращаем структуру с нулями для корректного отображения на фронтенде
	if len(result) == 0 {
		now := time.Now()
		defaultMetrics := map[string]interface{}{
			"calls_count":         0,
			"avg_wait_seconds":    0,
			"avg_talk_seconds":    0,
			"avg_handle_time":     0,
			"sla_percent":         0,
			"abandonment_rate":    0,
			"transfer_rate":       0,
			"fcr_rate":            0,
			"avg_customer_rating": 0,
			"avg_sentiment":       0,
			"calculated_at":       now,
		}
		result["2m"] = defaultMetrics
		result["10m"] = defaultMetrics
	}

	json.NewEncoder(w).Encode(result)
}

func handleQueueMetrics(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Читаем метрики по очередям, используя тот же calculated_at что и глобальные метрики
	// Это гарантирует консистентность: сумма по очередям = calls_count
	rows, err := pool.Query(ctx, `
		WITH latest_calc AS (
			SELECT MAX(calculated_at) as calc_at, time_window
			FROM global_metrics
			WHERE queue_id IS NULL AND name = 'calls_count' AND time_window IN ('2m', '10m')
			GROUP BY time_window
		)
		SELECT gm.time_window, gm.queue_id, gm.name, gm.value
		FROM global_metrics gm
		JOIN latest_calc lc ON gm.time_window = lc.time_window AND gm.calculated_at = lc.calc_at
		WHERE gm.queue_id IS NOT NULL
		ORDER BY gm.time_window, gm.queue_id, gm.name
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
		if err := rows.Scan(&window, &queue, &name, &value); err != nil {
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

func handleStatusDistribution(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Читаем из global_metrics для консистентности с другими метриками
	rows, err := pool.Query(ctx, `
		SELECT 
			REPLACE(name, 'status_', '') AS status,
			value::INT AS count
		FROM global_metrics
		WHERE name LIKE 'status_%' 
			AND time_window = '10m'
			AND queue_id IS NULL
			AND calculated_at = (
				SELECT MAX(calculated_at) 
				FROM global_metrics 
				WHERE name LIKE 'status_%' AND time_window = '10m'
			)
		ORDER BY value DESC
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	type StatusData struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}

	var result []StatusData
	for rows.Next() {
		var s StatusData
		if err := rows.Scan(&s.Status, &s.Count); err != nil {
			continue
		}
		result = append(result, s)
	}

	json.NewEncoder(w).Encode(result)
}

func handleTopAgents(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Читаем из global_metrics для консистентности, джойним с agents для имён
	rows, err := pool.Query(ctx, `
		SELECT 
			REPLACE(gm.name, 'agent_calls_', '') AS agent_id,
			a.full_name,
			gm.value::INT AS calls_count,
			0 AS avg_talk_time,
			0 AS sla_percent,
			0 AS avg_rating
		FROM global_metrics gm
		JOIN agents a ON REPLACE(gm.name, 'agent_calls_', '') = a.agent_id
		WHERE gm.name LIKE 'agent_calls_%'
			AND gm.time_window = '10m'
			AND gm.queue_id IS NULL
			AND gm.calculated_at = (
				SELECT MAX(calculated_at) 
				FROM global_metrics 
				WHERE name LIKE 'agent_calls_%' AND time_window = '10m'
			)
		ORDER BY gm.value DESC
		LIMIT 10
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Printf("monitoring-api: query error: %v", err)
		return
	}
	defer rows.Close()

	type AgentStats struct {
		AgentID     string  `json:"agent_id"`
		FullName    string  `json:"full_name"`
		CallsCount  int     `json:"calls_count"`
		AvgTalkTime float64 `json:"avg_talk_time"`
		SLAPercent  float64 `json:"sla_percent"`
		AvgRating   float64 `json:"avg_rating"`
	}

	var result []AgentStats
	for rows.Next() {
		var a AgentStats
		if err := rows.Scan(&a.AgentID, &a.FullName, &a.CallsCount, &a.AvgTalkTime, &a.SLAPercent, &a.AvgRating); err != nil {
			continue
		}
		result = append(result, a)
	}

	json.NewEncoder(w).Encode(result)
}

// WebSocket Hub для управления подключениями
type Hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[*websocket.Conn]struct{})}
}

func (h *Hub) Add(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	log.Printf("monitoring-api: WebSocket client connected, total=%d", len(h.clients))
}

func (h *Hub) Remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
	log.Printf("monitoring-api: WebSocket client disconnected, total=%d", len(h.clients))
}

func (h *Hub) Broadcast(msg interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			c.Close()
			delete(h.clients, c)
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func handleWebSocket(w http.ResponseWriter, r *http.Request, hub *Hub) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("monitoring-api: WebSocket upgrade error: %v", err)
		return
	}
	hub.Add(conn)
	defer hub.Remove(conn)

	for {
		if _, _, err := conn.NextReader(); err != nil {
			return
		}
	}
}