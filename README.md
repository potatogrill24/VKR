# Contact Center Monitoring Platform

Платформа для **мониторинга качества работы контактного центра в реальном времени**.

Проект выполнен в микросервисной архитектуре с использованием:
- **Kafka** — брокер сообщений для потоковой обработки событий
- **PostgreSQL** — хранилище исторических данных и справочников
- **Redis** — кэш realtime-метрик
- **Go** — backend-сервисы
- **React** — frontend-приложение

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
  "talk_seconds": 65,
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
| `calls_count` | 2min,  10min | Количество звонков |
| `avg_wait_seconds` | 2min,  10min | Среднее время ожидания |
| `avg_talk_seconds` | 2min,  10min | Среднее время разговора |
| `avg_handle_time` | 2min,  10min | Среднее время обработки (AHT) |
| `sla_percent` | 2min,  10min | % выполнения SLA |
| `abandonment_rate` | 2min,  10min | % брошенных звонков |
| `transfer_rate` | 2min,  10min | % переведённых звонков |
| `fcr_rate` | 2min,  10min | % решённых с первого раза (FCR) |
| `avg_customer_rating` | 2min,  10min | Средняя оценка клиентов |
| `avg_sentiment` | 2min,  10min | Средняя тональность разговоров |

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

### 2. Запуск генератора данных (отдельно от платформы) и его функциональные возможности

```bash
cd simulator
go run main.go
```

Генератор использует встроенный список операторов, никаких дополнительных файлов не требуется.

Параметры генератора (все опциональные):
- `-url` — URL producer API (по умолчанию `http://localhost:8082/api/events`)
- `-interval` — интервал между событиями в секундах (по умолчанию 7)
- `-dry-run` — только печатать JSON, не отправлять
- `-total` — общее количество событий (0 = бесконечно)

### 3. Остановка платформы

```bash
cd deployments
docker-compose down

# С удалением данных PostgreSQL:
docker-compose down -v
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
├── go.mod
└── README.md
```