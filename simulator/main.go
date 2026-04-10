// Генератор синтетических данных контактного центра. Отправляет события на HTTP API producer'а платформы.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// CallEvent — событие звонка для отправки на платформу
type CallEvent struct {
	CallID                string     `json:"call_id"`
	AgentID               string     `json:"agent_id,omitempty"`
	CustomerPhone         string     `json:"customer_phone"`
	Queue                 string     `json:"queue"`
	CallType              string     `json:"call_type"`
	SkillUsed             string     `json:"skill_used,omitempty"`
	IVRPath               string     `json:"ivr_path,omitempty"`
	StartedAt             time.Time  `json:"started_at"`
	AnsweredAt            *time.Time `json:"answered_at,omitempty"`
	EndedAt               time.Time  `json:"ended_at"`
	Status                string     `json:"status"`
	DisconnectReason      string     `json:"disconnect_reason"`
	WaitSeconds           int        `json:"wait_seconds"`
	TalkSeconds           int        `json:"talk_seconds"`
	HoldSeconds           int        `json:"hold_seconds"`
	WrapUpSeconds         int        `json:"wrap_up_seconds"`
	TransferCount         int        `json:"transfer_count"`
	IsFirstCallResolution bool       `json:"is_first_call_resolution"`
	SlaMet                bool       `json:"sla_met"`
	CustomerRating        *int       `json:"customer_rating,omitempty"`
	SentimentScore        *float64   `json:"sentiment_score,omitempty"`
}

// Agent представляет оператора
type Agent struct {
	ID           string
	Name         string
	PrimaryQueue string
	Skills       []string
	LastCallEnd  time.Time
	IsBusy       bool      
}

var (
	// Очереди контакт-центра с весами (вероятность звонка)
	queues = []struct {
		ID     string
		Weight int
	}{
		{"support", 35}, 
		{"sales", 25},      
		{"billing", 20},      
		{"tech-support", 15}, 
		{"vip", 5},           
	}

	// Типы звонков
	callTypes = []struct {
		Type   string
		Weight int
	}{
		{"inbound", 85},
		{"outbound", 10}, 
		{"callback", 5},
	}

	// Статусы звонков
	statuses = []struct {
		Status string
		Weight int
	}{
		{"completed", 82},   
		{"abandoned", 8},    
		{"transferred", 7},  
		{"voicemail", 3},   
	}

	disconnectReasons = map[string][]string{
		"completed":   {"customer_hangup", "agent_hangup"},
		"abandoned":   {"timeout", "customer_hangup"},
		"transferred": {"transfer"},
		"voicemail":   {"voicemail"},
	}

	ivrPaths = []string{"1", "1>1", "1>2", "2", "2>1", "3", "0"}
)

func main() {
	var (
		producerURL = flag.String("url", "http://localhost:8082/api/events", "URL producer API")
		intervalSec = flag.Int("interval", 7, "Средний интервал между звонками (секунды)")
		totalEvents = flag.Int("total", 0, "Общее количество событий (0 = бесконечно)")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	agents := initAgents()
	log.Printf("Инициализировано %d операторов", len(agents))

	client := &http.Client{Timeout: 5 * time.Second}

	log.Printf("Симулятор запущен: url=%s, interval=%d сек", *producerURL, *intervalSec)

	eventCount := 0
	for {
		if *totalEvents > 0 && eventCount >= *totalEvents {
			log.Printf("Отправлено %d событий, завершение", eventCount)
			break
		}

		interval := time.Duration(float64(*intervalSec) * (0.5 + rand.Float64()))
		time.Sleep(interval * time.Second)

		numCalls := 1 + rand.Intn(4)

		availableAgents := getAvailableAgents(agents)

		if numCalls > len(availableAgents) && len(availableAgents) > 0 {
			numCalls = len(availableAgents)
		}
		
		if len(availableAgents) == 0 {
			log.Println("Все операторы заняты, пропускаем цикл")
			continue
		}

		sentInBatch := 0
		for i := 0; i < numCalls && i < len(availableAgents); i++ {
			agent := availableAgents[i]

			event := generateRealisticCall(agent)

			updateAgentState(agent, event)

			if err := sendEvent(client, *producerURL, event); err != nil {
				log.Printf("Ошибка отправки: %v", err)
			} else {
				log.Printf("  [%d/%d] call_id=%s queue=%s agent=%s status=%s wait=%ds talk=%ds SLA=%v",
					i+1, numCalls, event.CallID[:8], event.Queue, event.AgentID, event.Status,
					event.WaitSeconds, event.TalkSeconds, event.SlaMet)
				sentInBatch++
			}
			eventCount++
		}
		
		if sentInBatch > 0 {
			log.Printf("Отправлено %d звонков в пакете, всего: %d", sentInBatch, eventCount)
		}
	}
}

func initAgents() []*Agent {
	agentsData := []struct {
		ID           string
		Name         string
		PrimaryQueue string
		Skills       []string
	}{
		{"agent-1", "Иван Петров", "support", []string{"support", "billing"}},
		{"agent-2", "Мария Иванова", "sales", []string{"sales", "vip"}},
		{"agent-3", "Алексей Сидоров", "billing", []string{"billing", "support"}},
		{"agent-4", "Елена Козлова", "sales", []string{"sales"}},
		{"agent-5", "Дмитрий Смирнов", "tech-support", []string{"tech-support"}},
		{"agent-6", "Анна Попова", "support", []string{"support"}},
		{"agent-7", "Сергей Васильев", "billing", []string{"billing"}},
		{"agent-8", "Ольга Морозова", "sales", []string{"sales", "vip"}},
		{"agent-9", "Павел Новиков", "tech-support", []string{"tech-support"}},
		{"agent-10", "Наталья Волкова", "support", []string{"support", "billing"}},
		{"agent-11", "Михаил Федоров", "billing", []string{"billing"}},
		{"agent-12", "Татьяна Соколова", "sales", []string{"sales"}},
		{"agent-13", "Андрей Михайлов", "tech-support", []string{"tech-support"}},
		{"agent-14", "Юлия Егорова", "support", []string{"support"}},
		{"agent-15", "Виктор Павлов", "billing", []string{"billing"}},
		{"agent-16", "Ксения Степанова", "sales", []string{"sales", "vip"}},
		{"agent-17", "Григорий Николаев", "tech-support", []string{"tech-support"}},
		{"agent-18", "Вероника Ковалева", "support", []string{"support"}},
		{"agent-19", "Илья Захаров", "billing", []string{"billing"}},
		{"agent-20", "София Медведева", "sales", []string{"sales"}},
	}

	agents := make([]*Agent, len(agentsData))
	for i, a := range agentsData {
		agents[i] = &Agent{
			ID:           a.ID,
			Name:         a.Name,
			PrimaryQueue: a.PrimaryQueue,
			Skills:       a.Skills,
			LastCallEnd:  time.Now().Add(-time.Hour),
			IsBusy:       false,
		}
	}
	return agents
}

func getAvailableAgents(agents []*Agent) []*Agent {
	now := time.Now()

	for _, a := range agents {
		if a.IsBusy && now.Sub(a.LastCallEnd) > 0 {
			a.IsBusy = false
		}
	}

	available := make([]*Agent, 0)
	for _, a := range agents {
		if !a.IsBusy {
			available = append(available, a)
		}
	}

	rand.Shuffle(len(available), func(i, j int) {
		available[i], available[j] = available[j], available[i]
	})

	return available
}

func updateAgentState(agent *Agent, event *CallEvent) {
	if event.Status == "abandoned" || event.Status == "voicemail" {
		return
	}

	busySeconds := (event.TalkSeconds + event.WrapUpSeconds) / 60
	if busySeconds < 3 {
		busySeconds = 3 
	}
	if busySeconds > 10 {
		busySeconds = 10 
	}
	agent.LastCallEnd = time.Now().Add(time.Duration(busySeconds) * time.Second)
	agent.IsBusy = true
}

func generateRealisticCall(agent *Agent) *CallEvent {
	now := time.Now()
	queue := selectQueue(agent)
	callType := selectCallType()
	status := selectStatus()

	waitSeconds := generateWaitTime(status)
	talkSeconds := generateTalkTime(status)
	holdSeconds := generateHoldTime(status, talkSeconds)
	wrapUpSeconds := generateWrapUpTime(status)

	totalDuration := waitSeconds + talkSeconds + wrapUpSeconds
	startedAt := now.Add(-time.Duration(totalDuration) * time.Second)
	endedAt := now

	var answeredAt *time.Time
	if status == "completed" || status == "transferred" {
		t := startedAt.Add(time.Duration(waitSeconds) * time.Second)
		answeredAt = &t
	}

	slaMet := waitSeconds <= 20 && (status == "completed" || status == "transferred")

	reasons := disconnectReasons[status]
	disconnectReason := reasons[rand.Intn(len(reasons))]

	transferCount := 0
	if status == "transferred" {
		transferCount = 1 + rand.Intn(2)
	}

	isFirstCallResolution := false
	if status == "completed" {
		isFirstCallResolution = rand.Float64() < 0.85
	}

	var customerRating *int
	if status == "completed" && rand.Float64() < 0.7 {
		r := rand.Float64()
		var rating int
		switch {
		case r < 0.02:
			rating = 1
		case r < 0.05:
			rating = 2
		case r < 0.20:
			rating = 3
		case r < 0.50:
			rating = 4
		default:
			rating = 5
		}
		customerRating = &rating
	}

	var sentimentScore *float64
	if status == "completed" || status == "transferred" {
		s := rand.NormFloat64()*0.2 + 0.4
		if s < -1 {
			s = -1
		}
		if s > 1 {
			s = 1
		}
		sentimentScore = &s
	}

	ivrPath := ""
	if callType == "inbound" {
		ivrPath = ivrPaths[rand.Intn(len(ivrPaths))]
	}

	agentID := ""
	skillUsed := ""
	if status != "abandoned" && status != "voicemail" {
		agentID = agent.ID
		if len(agent.Skills) > 0 {
			skillUsed = agent.Skills[rand.Intn(len(agent.Skills))]
		}
	}

	return &CallEvent{
		CallID:                uuid.NewString(),
		AgentID:               agentID,
		CustomerPhone:         generatePhone(),
		Queue:                 queue,
		CallType:              callType,
		SkillUsed:             skillUsed,
		IVRPath:               ivrPath,
		StartedAt:             startedAt,
		AnsweredAt:            answeredAt,
		EndedAt:               endedAt,
		Status:                status,
		DisconnectReason:      disconnectReason,
		WaitSeconds:           waitSeconds,
		TalkSeconds:           talkSeconds,
		HoldSeconds:           holdSeconds,
		WrapUpSeconds:         wrapUpSeconds,
		TransferCount:         transferCount,
		IsFirstCallResolution: isFirstCallResolution,
		SlaMet:                slaMet,
		CustomerRating:        customerRating,
		SentimentScore:        sentimentScore,
	}
}

func selectQueue(agent *Agent) string {
	if rand.Float64() < 0.7 {
		return agent.PrimaryQueue
	}
	if len(agent.Skills) > 0 {
		return agent.Skills[rand.Intn(len(agent.Skills))]
	}
	return agent.PrimaryQueue
}

func generateWaitTime(status string) int {
	if status == "abandoned" {
		return 30 + rand.Intn(60)
	}
	r := rand.Float64()
	switch {
	case r < 0.70:
		return 3 + rand.Intn(12) 
	case r < 0.90:
		return 15 + rand.Intn(10) 
	default:
		return 25 + rand.Intn(15)
	}
}

func generateTalkTime(status string) int {
	switch status {
	case "abandoned", "voicemail":
		return 0
	case "transferred":
		return 15 + rand.Intn(30)
	default:
		return 30 + rand.Intn(60)
	}
}

func generateHoldTime(status string, talkSeconds int) int {
	if status == "abandoned" || status == "voicemail" || talkSeconds == 0 {
		return 0
	}
	if rand.Float64() < 0.2 {
		return 10 + rand.Intn(40)
	}
	return 0
}

func generateWrapUpTime(status string) int {
	if status == "abandoned" || status == "voicemail" {
		return 0
	}
	return 10 + rand.Intn(20)
}

func generatePhone() string {
	return fmt.Sprintf("+7 (9%02d) %03d-**-**", rand.Intn(100), rand.Intn(1000))
}

func selectCallType() string {
	total := 0
	for _, item := range callTypes {
		total += item.Weight
	}
	r := rand.Intn(total)
	for _, item := range callTypes {
		r -= item.Weight
		if r < 0 {
			return item.Type
		}
	}
	return callTypes[len(callTypes)-1].Type
}

func selectStatus() string {
	total := 0
	for _, item := range statuses {
		total += item.Weight
	}
	r := rand.Intn(total)
	for _, item := range statuses {
		r -= item.Weight
		if r < 0 {
			return item.Status
		}
	}
	return statuses[len(statuses)-1].Status
}

func selectQueueWeighted() string {
	total := 0
	for _, item := range queues {
		total += item.Weight
	}
	r := rand.Intn(total)
	for _, item := range queues {
		r -= item.Weight
		if r < 0 {
			return item.ID
		}
	}
	return queues[len(queues)-1].ID
}

func sendEvent(client *http.Client, url string, event *CallEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return nil
}