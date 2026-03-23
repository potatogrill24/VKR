# Contact Center Monitoring Platform

Платформа для **мониторинга качества работы контактного центра в реальном времени**.

Проект выполнен в микросервисной архитектуре с использованием:
- **Kafka** — брокер сообщений для потоковой обработки событий
- **PostgreSQL** — хранилище исторических данных и справочников
- **Redis** — кэш realtime-метрик
- **Go** — backend-сервисы
- **React** — frontend-приложение

## Архитектура

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ВНЕШНИЕ СИСТЕМЫ                                     │
│  ┌─────────────────┐                                                        │
│  │    Simulator    │  Генератор синтетических данных (запускается отдельно) │
│  │  (Go / Python)  │                                                        │
│  └────────┬────────┘                                                        │
└───────────│─────────────────────────────────────────────────────────────────┘
            │ HTTP POST /api/events
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ПЛАТФОРМА МОНИТОРИНГА                             │
│                                                                             │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────────────┐   │
│  │  Producer   │────────▶│    Kafka    │────────▶│  history-consumer   │   │
│  │ (HTTP API)  │         │  ccm.calls  │         │  (→ PostgreSQL)     │   │
│  │   :8082     │         │             │         └──────────┬──────────┘   │
│  └─────────────┘         │             │                    │              │
│                          │             │                    ▼              │
│                          │             │         ┌─────────────────────┐   │
│                          │             │         │     PostgreSQL      │   │
│                          │             │         │  calls, agents,     │   │
│                          │             │         │  queues, metrics    │   │
│                          │             │         └──────────┬──────────┘   │
│                          │             │                    │              │
│                          │             │         ┌──────────┴──────────┐   │
│                          │             │         │      Analyser       │   │
│                          │             │         │  (агрегация метрик) │   │
│                          │             │         └─────────────────────┘   │
│                          │             │                                   │
│                          │             │         ┌─────────────────────┐   │
│                          │             │────────▶│ realtime-consumer   │   │
│                          └─────────────┘         │  (→ Redis + WS)     │   │
│                                                  │      :8080          │   │
│                                                  └──────────┬──────────┘   │
│                                                             │              │
│                                                             ▼              │
│                                                  ┌─────────────────────┐   │
│                                                  │       Redis         │   │
│                                                  │  realtime:metrics   │   │
│                                                  └──────────┬──────────┘   │
│                                                             │              │
│  ┌─────────────────────────────────────────────────────────┴────────────┐  │
│  │                        monitoring-api :8081                          │  │
│  │  GET /api/metrics/global    — глобальные метрики из PostgreSQL       │  │
│  │  GET /api/metrics/latest    — последние значения метрик              │  │
│  │  GET /api/metrics/queues    — метрики по очередям                    │  │
│  │  GET /api/metrics/realtime  — realtime из Redis                      │  │
│  │  GET /api/agents            — список операторов                      │  │
│  │  GET /api/queues            — список очередей                        │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                      │                                     │
└──────────────────────────────────────│─────────────────────────────────────┘
                                       │
                                       ▼
                            ┌─────────────────────┐
                            │     Frontend        │
                            │   React + Vite      │
                            │      :3000          │
                            └─────────────────────┘
```

## Сервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| **producer** | 8082 | HTTP API для приёма событий от внешних систем и публикации в Kafka |
| **history-consumer** | — | Читает события из Kafka и записывает в PostgreSQL (таблица `calls`) |
| **realtime-consumer** | 8080 | Вычисляет realtime-метрики, сохраняет в Redis, раздаёт через WebSocket |
| **analyser** | — | Периодически агрегирует данные из `calls` в `global_metrics` |
| **monitoring-api** | 8081 | HTTP API для фронтенда: метрики, операторы, очереди |
| **frontend** | 3000 | React SPA с дашбордом мониторинга |

## Модель данных

### CallEvent (событие звонка)

```json
{
  "call_id": "uuid",
  "agent_id": "agent-1",
  "customer_phone": "+7 (9XX) XXX-**-**",
  "queue": "support",
  "call_type": "inbound",
  "started_at": "2024-01-15T10:30:00Z",
  "answered_at": "2024-01-15T10:30:15Z",
  "ended_at": "2024-01-15T10:35:00Z",
  "status": "completed",
  "disconnect_reason": "customer_hangup",
  "wait_seconds": 15,
  "talk_seconds": 285,
  "hold_seconds": 0,
  "wrap_up_seconds": 30,
  "transfer_count": 0,
  "is_first_call_resolution": true,
  "sla_met": true,
  "customer_rating": 5,
  "sentiment_score": 0.7,
  "ivr_path": "1>2",
  "skill_used": "support"
}
```

### Справочники

- **agents** — операторы контакт-центра (agent_id, full_name, email, primary_queue, skills)
- **queues** — очереди (queue_id, queue_name, sla_threshold_seconds, priority)

## Метрики

### Realtime-метрики (обновляются каждую секунду)

| Метрика | Описание |
|---------|----------|
| `agents_available` | Свободные операторы |
| `agents_in_call` | Операторы на линии |
| `agents_wrap_up` | Операторы в послеразговорной обработке |
| `calls_in_queue` | Звонки в очереди |
| `longest_wait_sec` | Максимальное время ожидания |
| `avg_wait_sec` | Среднее время ожидания |
| `service_level` | % звонков отвеченных в SLA |
| `abandonment_rate` | % брошенных звонков |

### Глобальные метрики (агрегируются каждые 2 минуты)

| Метрика | Окна | Описание |
|---------|------|----------|
| `calls_count` | 1h, 24h | Количество звонков |
| `avg_wait_seconds` | 1h, 24h | Среднее время ожидания |
| `avg_talk_seconds` | 1h, 24h | Среднее время разговора |
| `avg_handle_time` | 1h, 24h | Среднее время обработки (AHT) |
| `sla_percent` | 1h, 24h | % выполнения SLA |
| `abandonment_rate` | 1h, 24h | % брошенных звонков |
| `transfer_rate` | 1h, 24h | % переведённых звонков |
| `fcr_rate` | 1h, 24h | % решённых с первого раза (FCR) |
| `avg_customer_rating` | 1h, 24h | Средняя оценка клиентов |
| `avg_sentiment` | 1h, 24h | Средняя тональность разговоров |

## Запуск

### 1. Запуск платформы

```bash
cd deployments
docker-compose up --build -d
```

Будут подняты:
- Zookeeper + Kafka
- PostgreSQL (с автоматической инициализацией схемы и данных)
- Redis
- Все Go-сервисы
- Frontend

### 2. Запуск генератора данных (отдельно от платформы)

```bash
cd simulator
go run main.go
```

Генератор использует встроенный список операторов, никаких дополнительных файлов не требуется.

Параметры генератора (все опциональные):
- `-url` — URL producer API (по умолчанию `http://localhost:8082/api/events`)
- `-agents` — путь к файлу agents.csv (опционально, есть встроенные данные)
- `-interval` — интервал между событиями в мс (по умолчанию 500)
- `-burst` — режим пакетной отправки (1-10 событий за раз)
- `-dry-run` — только печатать JSON, не отправлять
- `-total` — общее количество событий (0 = бесконечно)

### 3. Доступ к сервисам

| Сервис | URL |
|--------|-----|
| Frontend | http://localhost:3000 |
| Monitoring API | http://localhost:8081 |
| Producer API | http://localhost:8082 |
| WebSocket | ws://localhost:8080/ws/realtime |
| PostgreSQL | localhost:5432 (ccm/ccm/ccm) |
| Redis | localhost:6379 |

## API Endpoints

### Producer API (8082)

```
POST /api/events        — отправить одно событие
POST /api/events/batch  — отправить массив событий
GET  /api/health        — статус сервиса
```

### Monitoring API (8081)

```
GET /api/health              — статус сервиса
GET /api/metrics/global      — история глобальных метрик
GET /api/metrics/latest      — последние значения метрик по окнам (2m, 10m)
GET /api/metrics/queues      — метрики по очередям (напрямую из таблицы calls)
GET /api/metrics/realtime    — realtime-метрики из Redis
GET /api/stats/status-distribution — распределение статусов (за 1 час)
GET /api/stats/top-agents    — топ операторов по звонкам (за 1 час)
GET /api/agents              — список операторов
GET /api/queues              — список очередей
```

### WebSocket (8080)

```
ws://localhost:8080/ws/realtime — подписка на realtime-метрики
```

## Структура проекта

```
├── cmd/                          # Go-сервисы
│   ├── producer/                 # HTTP API → Kafka
│   ├── history-consumer/         # Kafka → PostgreSQL
│   ├── realtime-consumer/        # Kafka → Redis + WebSocket
│   ├── analyser/                 # PostgreSQL агрегация
│   └── monitoring-api/           # HTTP API для фронтенда
├── internal/                     # Общие пакеты
│   ├── kafka/                    # Конфигурация Kafka
│   ├── models/                   # Модели данных
│   └── storage/                  # Работа с PostgreSQL
├── simulator/                    # Генератор синтетических данных (отдельный модуль)
├── frontend/                     # React приложение
├── deployments/                  # Docker конфигурация
│   ├── docker-compose.yml
│   ├── init-db/                  # SQL-скрипты инициализации БД
│   └── *.Dockerfile
├── data/                         # Справочные данные
│   └── agents.csv
├── go.mod
└── README.md
```

## Остановка

```bash
cd deployments
docker-compose down

# С удалением данных PostgreSQL:
docker-compose down -v
```
