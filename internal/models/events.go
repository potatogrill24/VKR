package models

import "time"

// CallEvent описывает одно событие звонка в контакт-центре.
type CallEvent struct {
	CallID         string    `json:"call_id"`
	AgentID        string    `json:"agent_id"`
	CustomerID     string    `json:"customer_id"`
	Queue          string    `json:"queue"`
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at"`
	Status         string    `json:"status"` // completed, abandoned, transferred
	WaitSeconds    int       `json:"wait_seconds"`
	TalkSeconds    int       `json:"talk_seconds"`
	WrapUpSeconds  int       `json:"wrap_up_seconds"`
	SentimentScore float64   `json:"sentiment_score"` // -1..1
	SlaMet         bool      `json:"sla_met"`
}

