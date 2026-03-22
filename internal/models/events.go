package models

import "time"

// CallEvent описывает одно событие звонка в контакт-центре.
// Это основная структура данных, которая приходит от внешних систем
// и обрабатывается платформой мониторинга.
type CallEvent struct {
	// Идентификаторы
	CallID        string `json:"call_id"`
	AgentID       string `json:"agent_id,omitempty"`
	CustomerPhone string `json:"customer_phone"`

	// Маршрутизация
	Queue     string `json:"queue"`
	CallType  string `json:"call_type"` // inbound, outbound, callback
	SkillUsed string `json:"skill_used,omitempty"`
	IVRPath   string `json:"ivr_path,omitempty"`

	// Временные метки
	StartedAt  time.Time  `json:"started_at"`
	AnsweredAt *time.Time `json:"answered_at,omitempty"`
	EndedAt    time.Time  `json:"ended_at"`

	// Статус и причина завершения
	Status           string `json:"status"`            // completed, abandoned, transferred, voicemail
	DisconnectReason string `json:"disconnect_reason"` // customer_hangup, agent_hangup, transfer, timeout

	// Длительности (в секундах)
	WaitSeconds   int `json:"wait_seconds"`
	TalkSeconds   int `json:"talk_seconds"`
	HoldSeconds   int `json:"hold_seconds"`
	WrapUpSeconds int `json:"wrap_up_seconds"`

	// Качество обслуживания
	TransferCount         int  `json:"transfer_count"`
	IsFirstCallResolution bool `json:"is_first_call_resolution"`
	SlaMet                bool `json:"sla_met"`

	// Обратная связь
	CustomerRating *int     `json:"customer_rating,omitempty"` // 1-5, nil если не оценил
	SentimentScore *float64 `json:"sentiment_score,omitempty"` // -1..1, анализ тональности
}

// Agent описывает оператора контакт-центра
type Agent struct {
	AgentID      string   `json:"agent_id"`
	FullName     string   `json:"full_name"`
	Email        string   `json:"email"`
	PrimaryQueue string   `json:"primary_queue"`
	Skills       []string `json:"skills"`
	HireDate     string   `json:"hire_date"`
	IsActive     bool     `json:"is_active"`
}

// Queue описывает очередь контакт-центра
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
	// Состояние операторов
	AgentsAvailable int `json:"agents_available"`
	AgentsInCall    int `json:"agents_in_call"`
	AgentsWrapUp    int `json:"agents_wrap_up"`
	AgentsOffline   int `json:"agents_offline"`

	// Состояние очередей
	CallsInQueue    int `json:"calls_in_queue"`
	LongestWaitSec  int `json:"longest_wait_sec"`
	AvgWaitSec      int `json:"avg_wait_sec"`
	CallsPerMinute  int `json:"calls_per_minute"`

	// Качество за последние 5 минут
	ServiceLevel    float64 `json:"service_level"`    // % отвеченных в SLA
	AbandonmentRate float64 `json:"abandonment_rate"` // % брошенных

	// Метаданные
	UpdatedAt time.Time `json:"updated_at"`
}

// GlobalMetric представляет агрегированную метрику из PostgreSQL
type GlobalMetric struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Value        float64   `json:"value"`
	TimeWindow   string    `json:"time_window"`
	Queue        *string   `json:"queue,omitempty"`
	CalculatedAt time.Time `json:"calculated_at"`
}

