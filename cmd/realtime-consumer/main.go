// cmd/realtime-consumer/main.go
// Realtime Consumer — читает события из Kafka, вычисляет метрики реального времени,
// сохраняет в Redis и рассылает через WebSocket подключённым клиентам.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/example/contact-center-monitoring/internal/kafka"
	"github.com/example/contact-center-monitoring/internal/models"
)

// MetricsAggregator хранит состояние для вычисления realtime-метрик
type MetricsAggregator struct {
	mu sync.RWMutex

	// Скользящее окно событий за последние 5 минут
	recentCalls []callRecord

	// Состояние операторов с временем последнего события
	agentLastEvent map[string]agentEvent

	// Все известные операторы
	knownAgents map[string]bool
}

type callRecord struct {
	timestamp   time.Time
	status      string
	waitSeconds int
	talkSeconds int
	slaMet      bool
	agentID     string
}

type agentEvent struct {
	timestamp   time.Time
	status      string
	talkSeconds int
	wrapSeconds int
}

func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{
		recentCalls:    make([]callRecord, 0),
		agentLastEvent: make(map[string]agentEvent),
		knownAgents:    make(map[string]bool),
	}
}

func (a *MetricsAggregator) AddEvent(evt *models.CallEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()

	// Добавляем запись
	a.recentCalls = append(a.recentCalls, callRecord{
		timestamp:   now,
		status:      evt.Status,
		waitSeconds: evt.WaitSeconds,
		talkSeconds: evt.TalkSeconds,
		slaMet:      evt.SlaMet,
		agentID:     evt.AgentID,
	})

	// Запоминаем оператора и его последнее событие
	if evt.AgentID != "" {
		a.knownAgents[evt.AgentID] = true
		a.agentLastEvent[evt.AgentID] = agentEvent{
			timestamp:   now,
			status:      evt.Status,
			talkSeconds: evt.TalkSeconds,
			wrapSeconds: evt.WrapUpSeconds,
		}
	}

	// Удаляем старые записи (старше 5 минут)
	cutoff := now.Add(-5 * time.Minute)
	newCalls := make([]callRecord, 0, len(a.recentCalls))
	for _, c := range a.recentCalls {
		if c.timestamp.After(cutoff) {
			newCalls = append(newCalls, c)
		}
	}
	a.recentCalls = newCalls
}

func (a *MetricsAggregator) GetMetrics() models.RealtimeMetrics {
	a.mu.RLock()
	defer a.mu.RUnlock()

	now := time.Now()
	metrics := models.RealtimeMetrics{
		UpdatedAt: now,
	}

	// Подсчёт состояний операторов на основе времени последнего события
	// Логика: после завершения звонка оператор некоторое время "занят"
	// - talk_seconds секунд — "на линии" (симуляция)
	// - wrap_up_seconds секунд — "в обработке"
	// - после этого — "свободен"
	for agentID := range a.knownAgents {
		evt, exists := a.agentLastEvent[agentID]
		if !exists {
			metrics.AgentsAvailable++
			continue
		}

		// Сколько времени прошло с момента события
		elapsed := now.Sub(evt.timestamp)

		// Симулируем, что оператор был "на линии" talk_seconds
		// и "в обработке" wrap_seconds после этого
		talkDuration := time.Duration(evt.talkSeconds) * time.Second / 10 // сжимаем время для демо
		wrapDuration := time.Duration(evt.wrapSeconds) * time.Second / 10

		if evt.status == "abandoned" || evt.status == "voicemail" {
			// Эти звонки не обслуживались оператором
			metrics.AgentsAvailable++
		} else if elapsed < talkDuration {
			// Оператор ещё "на линии"
			metrics.AgentsInCall++
		} else if elapsed < talkDuration+wrapDuration {
			// Оператор в послеразговорной обработке
			metrics.AgentsWrapUp++
		} else {
			// Оператор свободен
			metrics.AgentsAvailable++
		}
	}

	// Метрики за последние 5 минут
	if len(a.recentCalls) > 0 {
		var totalWait, slaMetCount, abandonedCount int
		var maxWait int

		for _, c := range a.recentCalls {
			totalWait += c.waitSeconds
			if c.waitSeconds > maxWait {
				maxWait = c.waitSeconds
			}
			if c.slaMet {
				slaMetCount++
			}
			if c.status == "abandoned" {
				abandonedCount++
			}
		}

		metrics.AvgWaitSec = totalWait / len(a.recentCalls)
		metrics.LongestWaitSec = maxWait
		metrics.CallsPerMinute = len(a.recentCalls) / 5

		metrics.ServiceLevel = float64(slaMetCount) / float64(len(a.recentCalls)) * 100
		metrics.AbandonmentRate = float64(abandonedCount) / float64(len(a.recentCalls)) * 100
	}

	// Симуляция очереди: чем больше звонков и меньше свободных операторов, тем больше очередь
	if metrics.AgentsAvailable > 0 {
		metrics.CallsInQueue = metrics.CallsPerMinute / 2
	} else {
		metrics.CallsInQueue = metrics.CallsPerMinute
	}

	return metrics
}

func (a *MetricsAggregator) EventCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.recentCalls)
}

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
	log.Printf("realtime-consumer: client connected, total=%d", len(h.clients))
}

func (h *Hub) Remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
	log.Printf("realtime-consumer: client disconnected, total=%d", len(h.clients))
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

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	topic := kafka.CallsTopicFromEnv()
	reader := kafka.NewReader(topic, "realtime-consumer")
	defer reader.Close()

	hub := NewHub()
	aggregator := NewMetricsAggregator()

	log.Printf("realtime-consumer: connecting to Kafka topic=%s, group=realtime-consumer", topic)

	// Горутина для чтения событий из Kafka
	go func() {
		eventCount := 0
		lastLogTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					log.Printf("realtime-consumer: read error: %v", err)
					time.Sleep(time.Second)
					continue
				}

				var evt models.CallEvent
				if err := json.Unmarshal(m.Value, &evt); err != nil {
					log.Printf("realtime-consumer: unmarshal error: %v", err)
					continue
				}

				aggregator.AddEvent(&evt)
				eventCount++

				// Логируем каждые 10 секунд
				if time.Since(lastLogTime) > 10*time.Second {
					metrics := aggregator.GetMetrics()
					log.Printf("realtime-consumer: processed %d events, agents=%d/%d/%d, recentCalls=%d, SL=%.1f%%",
						eventCount,
						metrics.AgentsAvailable, metrics.AgentsInCall, metrics.AgentsWrapUp,
						aggregator.EventCount(),
						metrics.ServiceLevel)
					lastLogTime = time.Now()
				}
			}
		}
	}()

	// Горутина для периодической рассылки метрик
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics := aggregator.GetMetrics()

				// Сохраняем в Redis
				if err := saveMetricsToRedis(ctx, rdb, &metrics); err != nil {
					log.Printf("realtime-consumer: redis error: %v", err)
				}

				// Рассылаем через WebSocket
				if hub.ClientCount() > 0 {
					hub.Broadcast(metrics)
				}
			}
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/ws/realtime", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade error: %v", err)
			return
		}
		hub.Add(conn)
		defer hub.Remove(conn)

		// Сразу отправляем текущие метрики
		conn.WriteJSON(aggregator.GetMetrics())

		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	})

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"clients": hub.ClientCount(),
		})
	})

	addr := ":8080"
	if v := os.Getenv("REALTIME_HTTP_ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("realtime-consumer: http listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("realtime-consumer: shutting down...")
	srv.Shutdown(context.Background())
}

func saveMetricsToRedis(ctx context.Context, rdb *redis.Client, m *models.RealtimeMetrics) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, "realtime:metrics", data, 30*time.Second).Err()
}