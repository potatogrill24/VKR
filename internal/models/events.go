package models

import "time"

// CallEvent описывает одно событие звонка в контакт-центре.
type CallEvent struct {
	CallID        string `json:"call_id"`
	AgentID       string `json:"agent_id,omitempty"`
	CustomerPhone string `json:"customer_phone"`

	Queue     string `json:"queue"`
	CallType  string `json:"call_type"`
	SkillUsed string `json:"skill_used,omitempty"`
	IVRPath   string `json:"ivr_path,omitempty"`

	StartedAt  time.Time  `json:"started_at"`
	AnsweredAt *time.Time `json:"answered_at,omitempty"`
	EndedAt    time.Time  `json:"ended_at"`

	Status           string `json:"status"`            
	DisconnectReason string `json:"disconnect_reason"` 

	WaitSeconds   int `json:"wait_seconds"`
	TalkSeconds   int `json:"talk_seconds"`
	HoldSeconds   int `json:"hold_seconds"`
	WrapUpSeconds int `json:"wrap_up_seconds"`

	TransferCount         int  `json:"transfer_count"`
	IsFirstCallResolution bool `json:"is_first_call_resolution"`
	SlaMet                bool `json:"sla_met"`

	CustomerRating *int     `json:"customer_rating,omitempty"`
	SentimentScore *float64 `json:"sentiment_score,omitempty"` 
}

// Agent описывает оператора контактного центра
type Agent struct {
	AgentID      string   `json:"agent_id"`
	FullName     string   `json:"full_name"`
	Email        string   `json:"email"`
	PrimaryQueue string   `json:"primary_queue"`
	Skills       []string `json:"skills"`
	HireDate     string   `json:"hire_date"`
	IsActive     bool     `json:"is_active"`
}

// Queue описывает очередь контактного центра
type Queue struct {
	QueueID             string `json:"queue_id"`
	QueueName           string `json:"queue_name"`
	Description         string `json:"description"`
	SLAThresholdSeconds int    `json:"sla_threshold_seconds"`
	Priority            int    `json:"priority"`
	IsActive            bool   `json:"is_active"`
}

// RealtimeMetrics содержит метрики реального времени для дашборда
type RealtimeMetrics struct {
	AgentsAvailable int `json:"agents_available"`
	AgentsInCall    int `json:"agents_in_call"`
	AgentsWrapUp    int `json:"agents_wrap_up"`
	AgentsOffline   int `json:"agents_offline"`

	LongestWaitSec int `json:"longest_wait_sec"`
	AvgWaitSec     int `json:"avg_wait_sec"`
	CallsPerMinute int `json:"calls_per_minute"`

	ServiceLevel    float64 `json:"service_level"`    
	AbandonmentRate float64 `json:"abandonment_rate"`

	UpdatedAt time.Time `json:"updated_at"`
}

type GlobalMetric struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Value        float64   `json:"value"`
	TimeWindow   string    `json:"time_window"`
	Queue        *string   `json:"queue,omitempty"`
	CalculatedAt time.Time `json:"calculated_at"`
}

