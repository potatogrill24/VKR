// Producer Service — HTTP API для приёма событий от внешних систем и публикации их в Kafka.

package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/example/contact-center-monitoring/internal/kafka"
	"github.com/example/contact-center-monitoring/internal/models"
)

var (
	eventsReceived uint64
	eventsPublished uint64
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := kafka.CallsTopicFromEnv()
	writer := kafka.NewWriter(topic)
	defer writer.Close()

	log.Printf("producer: starting HTTP API, topic=%s, brokers=%s", topic, kafka.BrokersFromEnv())

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":           "ok",
			"events_received":  atomic.LoadUint64(&eventsReceived),
			"events_published": atomic.LoadUint64(&eventsPublished),
		})
	})

	// Приём одного события CallEvent
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var event models.CallEvent
		if err := json.Unmarshal(body, &event); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		atomic.AddUint64(&eventsReceived, 1)

		if err := publishEvent(ctx, writer, &event); err != nil {
			log.Printf("producer: publish error: %v", err)
			http.Error(w, "Failed to publish", http.StatusInternalServerError)
			return
		}

		atomic.AddUint64(&eventsPublished, 1)
		log.Printf("producer: published call_id=%s queue=%s agent=%s status=%s",
			event.CallID, event.Queue, event.AgentID, event.Status)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "accepted",
			"call_id": event.CallID,
		})
	})

	// Приём пакета событий CallEvent
	mux.HandleFunc("/api/events/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var events []models.CallEvent
		if err := json.Unmarshal(body, &events); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		atomic.AddUint64(&eventsReceived, uint64(len(events)))

		published := 0
		for _, event := range events {
			if err := publishEvent(ctx, writer, &event); err != nil {
				log.Printf("producer: batch publish error: %v", err)
				continue
			}
			published++
		}

		atomic.AddUint64(&eventsPublished, uint64(published))
		log.Printf("producer: batch published %d/%d events", published, len(events))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "accepted",
			"received":  len(events),
			"published": published,
		})
	})

	addr := ":8082"
	if v := os.Getenv("PRODUCER_HTTP_ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("producer: HTTP server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("producer: server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("producer: shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}

// функция публикации события CallEvent в Kafka
func publishEvent(ctx context.Context, writer *kafkago.Writer, event *models.CallEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafkago.Message{
		Key:   []byte(event.CallID),
		Value: payload,
		Time:  time.Now(),
	}

	if event.CallID == "" {
		msg.Key = []byte(uuid.NewString())
	}

	return writer.WriteMessages(ctx, msg)
}

