package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func NewWriter(topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(BrokersFromEnv()),
		Topic:        topic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}

func NewReader(topic, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{BrokersFromEnv()},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: time.Second,
	})
}

func CloseQuietly(ctx context.Context, c interface{ Close() error }) {
	_ = c.Close()
	_ = ctx
}

