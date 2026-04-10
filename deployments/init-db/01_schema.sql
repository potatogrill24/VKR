-- Таблица справочник операторов
CREATE TABLE IF NOT EXISTS agents (
    agent_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    primary_queue VARCHAR(50) NOT NULL,
    skills TEXT[] NOT NULL DEFAULT '{}',
    hire_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Таблица очередей (справочник)
CREATE TABLE IF NOT EXISTS queues (
    queue_id VARCHAR(50) PRIMARY KEY,
    queue_name VARCHAR(100) NOT NULL,
    description TEXT,
    sla_threshold_seconds INTEGER NOT NULL DEFAULT 20,
    priority INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Таблица звонков (основная таблица для аналитики)
CREATE TABLE IF NOT EXISTS calls (
    id BIGSERIAL PRIMARY KEY,
    call_id VARCHAR(50) UNIQUE NOT NULL,
    agent_id VARCHAR(50) REFERENCES agents(agent_id),
    customer_phone VARCHAR(20),
    queue_id VARCHAR(50) NOT NULL REFERENCES queues(queue_id),
    call_type VARCHAR(20) NOT NULL DEFAULT 'inbound',
    started_at TIMESTAMPTZ NOT NULL,
    answered_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL,
    disconnect_reason VARCHAR(30),
    wait_seconds INTEGER NOT NULL DEFAULT 0,
    talk_seconds INTEGER NOT NULL DEFAULT 0,
    hold_seconds INTEGER NOT NULL DEFAULT 0,
    wrap_up_seconds INTEGER NOT NULL DEFAULT 0,
    transfer_count INTEGER NOT NULL DEFAULT 0,
    is_first_call_resolution BOOLEAN,
    customer_rating INTEGER CHECK (customer_rating IS NULL OR (customer_rating >= 1 AND customer_rating <= 5)),
    sentiment_score FLOAT CHECK (sentiment_score IS NULL OR (sentiment_score >= -1 AND sentiment_score <= 1)),
    sla_met BOOLEAN NOT NULL DEFAULT false,
    ivr_path VARCHAR(200),
    skill_used VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT valid_call_type CHECK (call_type IN ('inbound', 'outbound', 'callback')),
    CONSTRAINT valid_status CHECK (status IN ('completed', 'abandoned', 'transferred', 'voicemail'))
);

-- Таблица глобальных метрик
CREATE TABLE IF NOT EXISTS global_metrics (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    value FLOAT NOT NULL,
    time_window VARCHAR(10) NOT NULL,
    queue_id VARCHAR(50) REFERENCES queues(queue_id),
    calculated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_agent_id ON calls(agent_id);
CREATE INDEX IF NOT EXISTS idx_calls_queue_id ON calls(queue_id);
CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status);
CREATE INDEX IF NOT EXISTS idx_calls_agent_started ON calls(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_queue_id_started ON calls(queue_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_global_metrics_calculated_at ON global_metrics(calculated_at DESC);
CREATE INDEX IF NOT EXISTS idx_global_metrics_name_queue_id ON global_metrics(name, queue_id);
