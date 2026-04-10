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
| **realtime-consumer** | — | Вычисляет realtime-метрики, сохраняет в Redis |
| **analyser** | — | Периодически агрегирует данные из `calls` в `global_metrics` |
| **monitoring-api** | 8081 | HTTP API, WebSocket для фронтенда: глобальные метрики, метрики в реальном времени, операторы, очереди |
| **frontend** | 3000 | React SPA с дашбордом мониторинга |

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
│   ├── realtime-consumer/        # Kafka → Redis
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