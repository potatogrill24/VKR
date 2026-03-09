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

type RealtimeMetrics struct {
	FreeAgents int `json:"free_agents"`
	InCall     int `json:"in_call"`
	InQueue    int `json:"in_queue"`
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
}

func (h *Hub) Remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

func (h *Hub) Broadcast(msg interface{}) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if err := c.WriteJSON(msg); err != nil {
			_ = c.Close()
			delete(h.clients, c)
		}
	}
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
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() {
		_ = rdb.Close()
	}()

	topic := kafka.CallsTopicFromEnv()
	reader := kafka.NewReader(topic, "realtime-consumer")
	defer reader.Close()

	hub := NewHub()
	metrics := &RealtimeMetrics{}
	var mu sync.Mutex

	go func() {
		for {
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

			mu.Lock()
			if evt.Status == "completed" || evt.Status == "abandoned" {
				metrics.InCall--
				metrics.FreeAgents++
				metrics.InQueue += 10
			} else {
				metrics.InCall++
				if metrics.FreeAgents > 0 {
					metrics.FreeAgents--
				}
			}
			mu.Unlock()

			// сохраняем текущие метрики в Redis для использования другими сервисами
			if err := saveMetricsToRedis(ctx, rdb, metrics); err != nil {
				log.Printf("realtime-consumer: redis error: %v", err)
			}

			hub.Broadcast(metrics)
		}
	}()

	http.HandleFunc("/ws/realtime", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade error: %v", err)
			return
		}
		hub.Add(conn)
		defer hub.Remove(conn)

		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	})

	addr := ":8080"
	if v := os.Getenv("REALTIME_HTTP_ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{Addr: addr}

	go func() {
		log.Printf("realtime-consumer: http listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	_ = srv.Shutdown(context.Background())
}

func saveMetricsToRedis(ctx context.Context, rdb *redis.Client, m *RealtimeMetrics) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// ключ можно описать в дипломе как точку интеграции realtime-слоя
	return rdb.Set(ctx, "realtime:metrics", data, 30*time.Second).Err()
}