// cmd/realtime-consumer/main.go
// Realtime Consumer — читает события из Kafka, вычисляет метрики реального времени
// и сохраняет их в Redis для последующего чтения сервисом Monitoring API.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/example/contact-center-monitoring/internal/kafka"
	"github.com/example/contact-center-monitoring/internal/models"
)

// Общее количество операторов в контакт-центре
const TotalAgents = 20

// MetricsAggregator хранит состояние для вычисления realtime-метрик
type MetricsAggregator struct {
	mu sync.RWMutex

	// Скользящее окно событий за последние 5 минут
	recentCalls []callRecord

	// Состояние операторов с временем последнего события
	agentLastEvent map[string]agentEvent
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
	// Логика: начинаем с TotalAgents свободных операторов,
	// затем вычитаем тех, кто сейчас занят (на линии или в обработке)
	for _, evt := range a.agentLastEvent {
		// Сколько времени прошло с момента события
		elapsed := now.Sub(evt.timestamp)

		// Симулируем, что оператор был "на линии" talk_seconds
		// и "в обработке" wrap_seconds после этого
		talkDuration := time.Duration(evt.talkSeconds) * time.Second / 10 // сжимаем время для демо
		wrapDuration := time.Duration(evt.wrapSeconds) * time.Second / 10

		if evt.status == "abandoned" || evt.status == "voicemail" {
			// Эти звонки не обслуживались оператором — он свободен
			continue
		} else if elapsed < talkDuration {
			// Оператор ещё "на линии"
			metrics.AgentsInCall++
		} else if elapsed < talkDuration+wrapDuration {
			// Оператор в послеразговорной обработке
			metrics.AgentsWrapUp++
		}
		// Если elapsed >= talkDuration+wrapDuration, оператор свободен (не считаем)
	}

	// Свободные = всего - занятые - в обработке
	metrics.AgentsAvailable = TotalAgents - metrics.AgentsInCall - metrics.AgentsWrapUp
	if metrics.AgentsAvailable < 0 {
		metrics.AgentsAvailable = 0
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

	return metrics
}

func (a *MetricsAggregator) EventCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.recentCalls)
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

	aggregator := NewMetricsAggregator()

	log.Printf("realtime-consumer: started, topic=%s, redis=%s", topic, redisAddr)

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

	// Горутина для периодического сохранения метрик в Redis
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
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("realtime-consumer: shutting down...")
	cancel()
}

func saveMetricsToRedis(ctx context.Context, rdb *redis.Client, m *models.RealtimeMetrics) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, "realtime:metrics", data, 30*time.Second).Err()
}
