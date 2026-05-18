package kafka

import (
	"os"
)

const (
	DefaultKafkaBrokers = "kafka:9092"
	DefaultCallsTopic   = "ccm.calls"
)

func BrokersFromEnv() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return DefaultKafkaBrokers
}

func CallsTopicFromEnv() string {
	if v := os.Getenv("KAFKA_CALLS_TOPIC"); v != "" {
		return v
	}
	return DefaultCallsTopic
}

func MetricsTopicFromEnv() string {
	if v := os.Getenv("KAFKA_METRICS_TOPIC"); v != "" {
		return v
	}
	return DefaultMetricsTopic
}

