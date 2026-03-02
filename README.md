## Contact Center Monitoring (Real-Time)

Платформа для **анализа и мониторинга качества работы контактного центра в реальном времени**.
Проект выполнен в микросервисной архитектуре с использованием **Kafka** как брокера сообщений,
**PostgreSQL** как хранилища истории и **Redis** (зарезервирован под кэш и быстрые метрики).

### Архитектура (высокоуровнево)

- **producer**  
  Генерирует синтетические события звонков (`CallEvent`) и публикует их в Kafka-топик `ccm.calls`.

- **history-consumer**  
  Отдельный consumer Kafka, который читает `CallEvent` из топика `ccm.calls` и записывает
  их в таблицу `calls` в PostgreSQL (историческое хранилище).

- **realtime-consumer**  
  Подписывается на топик `ccm.calls`, считает простые метрики в реальном времени
  (например, количество свободных операторов, активных звонков) и отдает их:
  - по WebSocket (`/ws/realtime`) напрямую во frontend;
  - в Redis (ключ `realtime:metrics`), откуда их читает `monitoring-api`.

- **analyser**  
  Периодически (по умолчанию раз в 2 часа) запускает задачу агрегации:
  читает историю звонков из таблицы `calls` и сохраняет агрегированные метрики в таблицу
  `global_metrics` в PostgreSQL (например, среднее время ожидания и количество звонков за последний час).

- **monitoring-api**  
  HTTP API для фронтенда. Считывает агрегированные метрики из PostgreSQL и realtime‑метрики из Redis
  и отдает их в формате JSON:
  - `GET /api/metrics/global` — агрегаты из таблицы `global_metrics`;
  - `GET /api/metrics/realtime` — последние realtime‑метрики из Redis.

- **frontend (React)**  
  Одностраничное приложение на React + Vite (директория `frontend`), которое:
  - по WebSocket получает realtime‑метрики из `realtime-consumer`;
  - по HTTP (`monitoring-api`) получает глобальные метрики и отображает их в виде таблицы/карточек.

Все backend‑сервисы написаны на **Go**.

### Основные технологии

- Язык: **Go 1.22**
- Брокер сообщений: **Apache Kafka**
- База данных: **PostgreSQL**
- Кэш/быстрые метрики: **Redis**
- Контейнеризация: **Docker**, **docker-compose**

### Запуск бэкенда через docker-compose

1. Перейти в директорию с `docker-compose.yml`:

```bash
cd deployments
```

2. Собрать и запустить всю платформу (Kafka + Postgres + Redis + все Go‑сервисы):

```bash
docker-compose up --build
```

Будут подняты контейнеры:

- `zookeeper` и `kafka` — инфраструктура для брокера сообщений;
- `postgres` — база данных `ccm`;
- `redis` — кэш и хранилище realtime‑метрик;
- `producer` — сервис генерации и отправки событий в Kafka;
- `history-consumer` — пишет события из Kafka в таблицу `calls` в PostgreSQL;
- `realtime-consumer` — читает события из Kafka и отдает метрики по WebSocket и в Redis (ключ `realtime:metrics`);
- `analyser` — периодический сервис агрегации метрик в PostgreSQL (записывает в `global_metrics`);
- `monitoring-api` — HTTP API для фронтенда на порту `8081`;
- `frontend` — React‑приложение, отдаваемое через nginx на порту `3000`.

Для остановки всех контейнеров:

```bash
docker-compose down
```

После запуска платформа доступна по адресам:

- UI мониторинга: `http://localhost:3000`
- Monitoring API: `http://localhost:8081`
- WebSocket realtime: `ws://localhost:8080/ws/realtime`

### Доступные эндпоинты

- **Realtime WebSocket**  
  `ws://localhost:8080/ws/realtime`  
  Подписка на простые метрики реального времени (пример: количество свободных операторов).

- **Monitoring API**
  - `GET http://localhost:8081/api/health` — проверка живости сервиса.
  - `GET http://localhost:8081/api/metrics/global` — чтение последних глобальных метрик
    (агрегатов, рассчитанных сервисом `analyser`, структура таблицы `global_metrics` задается в БД).
  - `GET http://localhost:8081/api/metrics/realtime` — чтение последних realtime‑метрик из Redis
    (тот же формат, что и по WebSocket).

- **PostgreSQL**
  - хост: `localhost`, порт: `5432`
  - БД/пользователь/пароль: `ccm / ccm / ccm`

- **Redis**
  - хост: `localhost`, порт: `6379`