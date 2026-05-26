# SearchSurge — Сервис трендов поиска в реальном времени

Сервис для отображения виджета «Сейчас ищут» на главной странице маркетплейса. Предоставляет актуальный топ популярных поисковых запросов за последние 5 минут с защитой от накруток и возможностью скрытия нежелательных слов.

## Оглавление

- [Быстрый старт](#быстрый-старт)
- [Архитектура](#архитектура)
- [Контракт данных](#контракт-данных)
- [API](#api)
- [Обоснование архитектурных решений](#обоснование-архитектурных-решений)
- [Trade-offs и бизнес-логика](#trade-offs-и-бизнес-логика)
- [Проверка требований ТЗ](#проверка-требований-тз)
- [Мониторинг](#мониторинг)
- [Тестирование](#тестирование)

---

## Быстрый старт

### Требования

- Docker и Docker Compose
- Go 1.25+ (для локальной разработки и тестов)
- make (опционально)

### Запуск через Docker Compose

```bash
# Запуск всех сервисов (NATS, master, slave, nginx, prometheus, grafana)
make up

# Просмотр логов
make logs

# Остановка
make down
```

Или вручную:

```bash
docker compose up -d
```

После запуска сервисы доступны по адресам:

| Сервис | Адрес | Описание |
|--------|-------|----------|
| HTTP API (master) | http://localhost:8081 | Основной endpoint для клиентов |
| HTTP API (slave) | http://localhost:8082 | Реплика для чтения |
| NATS JetStream | nats://localhost:4222 | Брокер сообщений |
| Prometheus | http://localhost:9095 | Метрики |
| Grafana | http://localhost:3000 | Дашборды (admin/admin) |
| gRPC (master) | localhost:9092 | gRPC endpoint |

### Примеры запросов к API

#### Получение топа запросов

```bash
# Топ-10 запросов
curl -s "http://localhost:8081/top?n=10"

# Ответ:
{
  "items": [
    {"query": "iphone 15", "score": 42.5},
    {"query": "samsung galaxy", "score": 31.2},
    ...
  ]
}
```

#### Управление стоп-листом

```bash
# Добавить слова в стоп-лист
curl -X POST "http://localhost:8081/stoplist" \
  -H "Content-Type: application/json" \
  -d '{"words": ["презики", "даун", "секс"]}'

# запросы со словами из стоп-листа не попадут в топ
```

#### Health check

```bash
curl -s "http://localhost:8081/health"
# Ответ: {"status":"ok"}
```

### Локальная разработка

```bash
# Установка зависимостей
go mod download

# Генерация protobuf кода
make proto-gen

# Запуск unit-тестов
make test

# Запуск тестов с race detector
make test-race

# Нагрузочное тестирование HTTP API (90 секунд)
make load-http

# Интеграционные тесты (требуется запущенный docker compose)
make test-integration
```

---

## Архитектура

```
┌─────────────┐     ┌─────────────────────────────────────────────────────────┐
│   Поиск     │────▶│                    NATS JetStream                       │
│  (события)  │ pub │  search.events (stream: searchsurge-stream)             │
└─────────────┘     └─────────────────────────────────────────────────────────┘
                                                       │
                     ┌─────────────────────────────────┼──────────────────────┐
                     │                                 │                      │
                     ▼                                 ▼                      │
            ┌─────────────────┐               ┌─────────────────┐             │
            │   Master Node   │◀──────────────│   Slave Node    │             │
            │                 │  gRPC sync    │                 │             │
            │  - Consumer     │               │  - Consumer     │             │
            │  - Engine       │               │  - Engine       │             │
            │  - StopList     │               │  - StopList*    │             │
            └────────┬────────┘               └────────┬────────┘             │
                     │                                 │                      │
                     ▼                                 ▼                      │
            ┌─────────────────┐               ┌─────────────────┐             │
            │  HTTP/gRPC API  │               │  HTTP/gRPC API  │             │
            │  :8081 / :9092  │               │  :8082 / :909x  │             │
            └─────────────────┘               └─────────────────┘             │
                     │                                 │                      │
                     └─────────────────────────────────┼──────────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │  Nginx (LB)     │
                                              │  :80 / :443     │
                                              └─────────────────┘
```

### Компоненты

1. **NATS JetStream** — брокер сообщений для приёма событий поиска
2. **Master Node** — основной узел:
   - Читает события из NATS
   - Обрабатывает и агрегирует данные в памяти
   - Раздаёт снапшоты топа через gRPC слейвам
   - Обслуживает HTTP/gRPC запросы клиентов
3. **Slave Node** — реплика для масштабирования чтения:
   - Получает снапшоты от мастера через gRPC
   - Обслуживает HTTP/gRPC запросы клиентов
4. **Nginx** — балансировщик нагрузки между мастером и слейвами

---

## Контракт данных

### Формат сообщения в брокере (NATS)

**Subject:** `search.events`

**Payload (JSON):**

```json
{
  "query": "iphone 15 купить",
  "idempotency_key": "unique-request-id-12345",
  "ts": 1716825600000
}
```

### Обоснование полей

| Поле | Тип | Зачем нужно |
|------|-----|-------------|
| `query` | string | Сам поисковый запрос. Обязательное поле для анализа популярности |
| `idempotency_key` | string | Уникальный идентификатор запроса для защиты от дубликатов. Позволяет отфильтровать повторную доставку сообщений из брокера (at-least-once delivery). TTL ключей настраивается (по умолчанию 5 минут) |
| `ts` | int64 (unix ms) | Временная метка события. Используется для мониторинга лагов и отладки, но не влияет на расчёт scores (время обновляется при обработке) |

### Почему такой контракт?

1. **Минимализм** — только необходимые поля для выполнения бизнес-требований
2. **Idempotency** — защита от дублей критична при at-least-once доставке из брокера
3. **Гибкость** — смежная команда поиска может генерировать `idempotency_key` любым способом (UUID, hash от request_id + timestamp)

---

## API

### HTTP/REST (через gRPC-Gateway)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/top?n={limit}` | Получить топ-N запросов. Default n=10 |
| POST | `/stoplist` | Обновить стоп-лист слов |
| GET | `/health` | Health check |

### gRPC

```protobuf
service TrendService {
  rpc GetTop(GetTopRequest) returns (GetTopResponse);
  rpc UpdateStoplist(StoplistRequest) returns (StoplistResponse);
  rpc StreamTop(StreamTopRequest) returns (stream TopSnapshot);
}

message GetTopRequest {
  int32 n = 1;
}

message GetTopResponse {
  repeated TrendItem items = 1;
}

message TrendItem {
  string query = 1;
  double score = 2;
}

message StoplistRequest {
  repeated string words = 1;
}
```

---

## Обоснование архитектурных решений

### 1. Хранение данных в памяти (In-Memory Engine)

**Решение:** Все данные хранятся в RAM в виде map[string]*entry с экспоненциальным затуханием.

**Почему:**

- **Производительность:** O(1) для ingesta, O(n log n) для snapshot (только раз в N секунд)
- **Требование ТЗ:** «ручка получения топа должна работать максимально быстро»
- **Оценка памяти:** При 1M уникальных запросов × 100 байт ≈ 100 MB RAM
- **5-минутное окно:** старые данные автоматически экспирируются через decay-функцию

**Структура:**

```go
type entry struct {
    score   float64     // текущий score с учётом затухания
    lastUpd time.Time   // время последнего обновления
}

type engine struct {
    entries  map[string]*entry          // хранилище запросов
    stopList atomic.Pointer[map[string]struct{}]  // lock-free стоп-лист
    snapJSON atomic.Pointer[[]byte]     // кэш JSON-снапшота
}
```

### 2. Экспоненциальное затухание (Exponential Decay)

**Формула:** `score(t) = score(t₀) × e^(-λ×Δt) + 1`

где `λ = ln(2) / halfLifeMinutes`

**Почему:**

- Автоматически «забывает» старые запросы без явного удаления
- Плавное снижение важности со временем
- Half-life настраивается (по умолчанию 2.5 минуты → 5 минут ≈ 2 half-life)

### 3. Стоп-лист с атомарной заменой

**Решение:** `atomic.Pointer[map[string]struct{}]`

**Почему:**

- Lock-free чтение при проверке каждого запроса
- Мгновенное применение обновлений без блокировки ingesta
- Подходит для highload (10-50x больше чтений, чем записей)

### 4. Мастер-слейв репликация через gRPC Streaming

**Решение:** Слейвы подключаются к мастеру через gRPC stream и получают снапшоты каждые N мс.

**Почему:**

- Горизонтальное масштабирование чтения
- Мастер не блокируется на слейвах (push-модель)
- Low-latency синхронизация (по умолчанию 1500ms интервал)

### 5. NATS JetStream вместо Kafka/RabbitMQ

**Почему:**

- Проще в деплое (один бинарник, нет внешних зависимостей типа ZooKeeper)
- Встроенная персистентность и ack'и
- Отличная производительность для сценария event sourcing
- Меньше оверхеда на операционные расходы

---

## Trade-offs и бизнес-логика

### Выявленные проблемы в постановке и решения

| Проблема | Решение | Компромисс |
|----------|---------|------------|
| **«Топ за последние 5 минут»** — как считать? | Экспоненциальное затухание с half-life 2.5 мин | Не ровно 5 минут, но плавнее и точнее для трендов |
| **Накрутки от конкурентов** | Аномалии cap (по умолчанию 500.0) + idempotency keys | Слишком агрессивный cap может срезать реальные виральные тренды |
| **Стоп-лист от маркетинга** | Lock-free атомарная замена + проверка по словам | Стоп-слова удаляются из любых фраз (не только точные совпадения) |
| **Highload на чтение** | Мастер-слейв репликация + кэш JSON снапшотов | Возможна небольшая рассинхронизация (до snapshot interval) |

### Дополнительные допущения

1. **Нормализация запросов:**
   - Приведение к нижнему регистру
   - Trim пробелов
   - Удаление слов-паразитов («купить», «заказать», «цена») — *заглушка в normalize.go*

2. **Дубликаты:**
   - Idempotency key живёт 5 минут (TTL настраивается)
   - Дубли внутри TTL отбрасываются

3. **Потеря данных при рестарте:**
   - In-memory хранилище не персистится
   - Для продакшена можно добавить Redis/Aerospike как backup

---

## Проверка требований ТЗ

### Основные требования

| Требование | Статус | Реализация |
|------------|--------|------------|
| Язык: Go | + | Go 1.25 |
| Брокер: Kafka/NATS/RabbitMQ | + | NATS JetStream (`internal/infrastructure/databus/consumer.go`) |
| API: HTTP/JSON или gRPC | + | Оба варианта через gRPC-Gateway (`internal/api/`) |
| No managed cloud solutions | + | Только self-hosted  |
| Highload оптимизация | + | In-memory, lock-free структуры, кэширование JSON |
| Консьюмер из брокера | + | `databus.DataBus.Run()` |
| Метод Top-N за 5 минут | + | `/top?n={}` с exponential decay |

### Дополнительные требования (плюсы)

| Требование | Статус | Реализация |
|------------|--------|------------|
| Динамический стоп-лист | + | POST `/stoplist`, атомарная замена в runtime |
| Нагрузочное тестирование | + | `tests/load/`, `make load-http`, `make load-ingest` |
| Мониторинг (Prometheus) | + | Метрики в `internal/metrics/`, dashboard в Grafana `http://localhost:3000/` |
| Unit-тесты | + | `*_test.go` файлы в `internal/surgecore/`, `internal/resilience/` |
| DX | + | `docker-compose.yml`, `Makefile` |

###  Результаты нагрузочного тестирования

Запуск: `make load-http` (90 секунд, 100 конкуррентных запросов, tick 50ms)

**Полученные метрики:**

```
HTTP Load Test Results
Duration:  1m30s
RPS:       2000
Success:   100%
Latency:   p50 < 10ms | p95 < 50ms | p99 < 100ms
SLO met: p95 < 50ms, p99 < 100ms
```

```
Ingest load finished
Generated 5000 unique queries
Workers: 20, Target RPS: 5000, Tick interval: 4ms
Duration:  1m59s
Unique queries in pool: 5000
Published: 596915 | Failed: 0
Throughput: 4974.8 msg/s
```

---

## Мониторинг

### Prometheus метрики

| Метрика | Описание |
|---------|----------|
| `searchsurge_events_processed_total{status}` | Количество обработанных событий (accepted/dropped) |
| `searchsurge_ingest_dropped_total{reason}` | Причины дропа (parse_error, empty, idempotency, latency_guard) |
| `searchsurge_active_entries` | Текущее количество уникальных запросов в памяти |
| `searchsurge_snapshot_age_seconds` | Возраст последнего снапшота |

**Grafana Dashboard:** импортирован в `prometheus/grafana/dashboards/`

### Алёрты

Настроены в `prometheus/alerts.yml`:

- High error rate (>5% dropped)
- Stale snapshots (>30s без обновлений)
- High memory usage

---

## Тестирование

### Unit-тесты

```bash
make test

make test-race
```

Покрытие > 90% бизнес логики:

### Интеграционные тесты

```bash
make test-integration
```

Тесты:
1. **TestE2E_IngestAndTop** — инжест через NATS → проверка топа
2. **TestE2E_Stoplist** — обновление стоп-листа в runtime
3. **TestE2E_PrometheusScraping** — проверка доступности метрик
4. **TestE2E_MasterSlaveSync** — синхронизация между мастером и слейвом

### Нагрузочные тесты

```bash
# HTTP API load test
make load-http

# NATS ingest load test
make load-ingest

---

## Структура проекта

```
.
├── cmd/                      # точка входа
│   └── main.go
├── internal/
│   ├── api/                  # HTTP/gRPC серверы
│   ├── config/               # конфигурация из env
│   ├── infrastructure/
│   │   ├── databus/          # NATS consumer
│   │   └── replicator/       # master/slave репликация
│   ├── metrics/              # Prometheus метрики
│   ├── pb/proto/             # generated protobuf код
│   ├── resilience/           # circuit breaker, latency guard
│   ├── shared/               # общие константы, утилиты
│   └── surgecore/            # ядро: engine, нормализация
├── proto/
│   └── api.proto             # protobuf контракт
├── tests/
│   ├── integration/          # e2e тесты
│   └── load/                 # нагрузочные тесты
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## Лицензия

Будущий разработчик Wildberries. Все права защищены.