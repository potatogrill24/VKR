package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/example/contact-center-monitoring/internal/kafka"
	"github.com/example/contact-center-monitoring/internal/models"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := kafka.CallsTopicFromEnv()
	writer := kafka.NewWriter(topic)
	defer writer.Close()

	log.Printf("producer: starting, topic=%s, brokers=%s", topic, kafka.BrokersFromEnv())

	interval := 500 * time.Millisecond
	if v := os.Getenv("PRODUCER_INTERVAL_MS"); v != "" {
		if ms, err := time.ParseDuration(v + "ms"); err == nil {
			interval = ms
		}
	}

	queues := []string{"support", "sales", "billing"}
	agentIDs := []string{"agent-1", "agent-2", "agent-3", "agent-4"}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			event := randomCallEvent(t, queues, agentIDs)
			payload, err := json.Marshal(event)
			if err != nil {
				log.Printf("producer: marshal error: %v", err)
				continue
			}

			err = writer.WriteMessages(ctx, kafkamessage(payload))
			if err != nil {
				log.Printf("producer: write error: %v", err)
				continue
			}
			log.Printf("producer: sent call_id=%s queue=%s agent=%s status=%s sla=%v",
				event.CallID, event.Queue, event.AgentID, event.Status, event.SlaMet)
		}
	}
}

func randomCallEvent(now time.Time, queues, agentIDs []string) models.CallEvent {
	wait := rand.Intn(60)             // 0-59 сек ожидания
	talk := 30 + rand.Intn(600)       // 30-630 сек разговора
	wrap := 5 + rand.Intn(60)         // 5-65 сек послеразговорной обработки
	statuses := []string{"completed", "abandoned", "transferred"}
	status := statuses[rand.Intn(len(statuses))]

	start := now.Add(-time.Duration(wait+talk+wrap) * time.Second)
	end := now

	slaMet := wait <= 20 // SLA: ответ в течение 20 секунд

	return models.CallEvent{
		CallID:         uuid.NewString(),
		AgentID:        agentIDs[rand.Intn(len(agentIDs))],
		CustomerID:     uuid.NewString(),
		Queue:          queues[rand.Intn(len(queues))],
		StartedAt:      start,
		EndedAt:        end,
		Status:         status,
		WaitSeconds:    wait,
		TalkSeconds:    talk,
		WrapUpSeconds:  wrap,
		SentimentScore: rand.Float64()*2 - 1, // -1..1
		SlaMet:         slaMet,
	}
}

func kafkamessage(payload []byte) kafkago.Message {
	return kafkago.Message{
		Key:   []byte(uuid.NewString()),
		Value: payload,
		Time:  time.Now(),
	}
}

