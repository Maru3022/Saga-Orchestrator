# Saga Orchestrator

Центральный оркестратор распределённых транзакций fitness-платформы. Координирует многошаговый процесс создания пользователя (уведомление → личный кабинет → расчёт питания) по паттерну orchestration-based Saga: рассылает команды шагам через Kafka, отслеживает их ответы, делает retry с компенсацией при сбоях и следит за зависшими сагами по таймауту.

![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen?logo=springboot)
![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-2023.0.2-6DB33F?logo=spring)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-spring--kafka-black?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Flyway-336791?logo=postgresql)
![Redis](https://img.shields.io/badge/Redis-saga%20state-DC382D?logo=redis)
![Eureka](https://img.shields.io/badge/Discovery-Eureka%20Client-blue)

## Что делает сервис

- Запускает сагу `CREATE_PROGRAM` по событию `user.created`: последовательно отправляет команды на отправку уведомления, создание личного кабинета и расчёт КБЖУ, продвигая состояние `SagaInstance` через `STARTED → USER_CREATED → NOTIFICATION_SENT → CABINET_CREATED → COMPLETED`.
- Параллельно поддерживает второй, более общий механизм саги — динамическую цепочку `List<StepHandler>` (`UserStepHandler → NotificationStepHandler → TrainsStepHandler → NutritionStepHandler`), которая запускается через REST (`POST /saga/create-program`) и хранит состояние в Redis, а не в PostgreSQL.
- При неуспехе шага выполняет до `maxRetries` (по умолчанию 3) повторов того же шага, а после исчерпания ретраев — компенсацию в обратном порядке (`DELETE_CABINET → CANCEL_NOTIFICATION → DELETE_USER`).
- Гарантирует идемпотентность старта саги по `correlationId` (`existsByCorrelationId`), чтобы повторная доставка события `user.created` не создавала вторую parallel-сагу для того же пользователя.
- Фоновый монитор (`SagaTimeoutMonitor`, каждые 30 секунд) находит саги, зависшие дольше `saga.timeout-ms` (по умолчанию 120 секунд), и принудительно переводит их в `FAILED`/`COMPENSATING`.
- Регистрируется в Eureka и публикует health/metrics для Prometheus.

## Архитектура

```text
                         Kafka: user.created
                                |
                                v
                     +---------------------+
                     |  UserCreatedListener |
                     +----------+----------+
                                |
                                v
                     +----------------------+        PostgreSQL
                     |   SagaOrchestrator    |<-----> saga_instances
                     |  (event-driven path)  |        (Flyway-managed)
                     +----------+-----------+
                                |
        saga.notification.send | saga.cabinet.create | saga.nutrition.calculate
                                v
       +-------------------+  +----------------+  +----------------------+
       | Training_          |  | (Cabinet       |  | Training-Nutrition   |
       | Notification       |  |  service - вне |  |                      |
       +-------------------+  |  периметра)     |  +----------------------+
                                +----------------+
                                |
        saga.notification.response / saga.cabinet.response / saga.nutrition.response
                                v
                     +----------------------+
                     |   SagaOrchestrator    |
                     +----------------------+

  Параллельно: REST-путь
  POST /saga/create-program -> List<StepHandler> -> saga-*-command/-response -> Redis (SagaState, TTL 7д)
  SagaTimeoutMonitor (каждые 30с) -> сравнивает updatedAt в Redis с saga.timeout-ms -> failAndRollback()
```

## Архитектурные решения

### 1. Два независимых движка саги в одном сервисе

В кодовой базе сосуществуют две реализации одной и той же идеи, с разными моделями состояния:
- **Event-driven путь** (`SagaOrchestrator.startSaga(UserCreatedEvent)` + жёстко прописанная последовательность шагов в коде, сущность `SagaInstance`, хранится в PostgreSQL через Flyway-миграции `V1__create_saga_instances.sql`).
- **Generic путь** (`SagaOrchestrator.startSaga(String sagaType, Map payload)` + список `StepHandler` из `SagaHandlersConfig`, сущность `SagaState`, хранится в Redis с TTL 7 дней через `SagaStateRepository`).

Второй вариант компонуется из независимых `StepHandler`-реализаций (`UserStepHandler`, `NotificationStepHandler`, `TrainsStepHandler`, `NutritionStepHandler`), каждая из которых знает только свой шаг вперёд/назад (`processForward` / `processRollback`) и публикует команды в собственный топик (`saga-notification-command`, `saga-nutrition-command` и т.д.). `findNextHandler` просто берёт следующий элемент списка по индексу — порядок шагов задаётся порядком бинов в `SagaHandlersConfig`, а не явным графом, что делает цепочку легко расширяемой добавлением нового `StepHandler`, но не подходит для нелинейных сценариев (параллельные шаги, ветвление).

### 2. Компенсация в обратном порядке с явным маппингом "шаг отказа → что откатывать"

`startCompensation` в `SagaOrchestrator` использует `switch` по `SagaStep`, на котором сага упала, чтобы решить, какие именно компенсации нужны: если упал шаг `CALCULATE_NUTRITION` — откатываются и кабинет, и уведомление; если упал `CREATE_CABINET` — откатывается только уведомление; пользователь компенсируется (`DELETE_USER`) в любом случае последним шагом. Это явная, читаемая реализация Saga-компенсации без отдельного DSL: на каждый шаг — ровно один компенсирующий Kafka-топик (`*.compensate`), и порядок отправки зафиксирован в коде, а не выводится автоматически из истории шагов.

### 3. Обнаружение зависших саг таймаутом, а не только по ответу

Помимо обработки явных `FAILED`-ответов от шагов, `SagaTimeoutMonitor` каждые 30 секунд опрашивает Redis на предмет саг в статусе `IN_PROGRESS`, у которых `updatedAt` старше `saga.timeout-ms`. Это закрывает сценарий, когда шаг-сервис вообще не ответил (упал, потерял сообщение, sidecar недоступен) — без таймера такая сага осталась бы в `IN_PROGRESS` навсегда. Обнаруженные зависшие саги переводятся в `FAILED` через `failAndRollback`, что в этой ветке кода является терминальным состоянием без отдельного запуска компенсирующих топиков (в отличие от event-driven пути, где `handleFailure` после исчерпания ретраев явно инициирует `startCompensation`).

## API-эндпоинты

| Метод | Путь | Контроллер | Описание |
|---|---|---|---|
| POST | `/saga/create-program` | `SagaController` | Запускает сагу типа `CREATE_PROGRAM` через generic `StepHandler`-цепочку, возвращает `202 Accepted` с `SagaState` |
| GET | `/saga/{sagaId}` | `SagaController` | Возвращает текущее состояние саги из Redis по `sagaId`, `404` если не найдена |

### Kafka-топики (основные)

| Топик | Направление | Назначение |
|---|---|---|
| `saga.user.created` / `user.created` | consume | Триггер старта саги создания пользователя |
| `saga.notification.send`, `saga.cabinet.create`, `saga.nutrition.calculate` | produce | Команды шагам (event-driven путь) |
| `saga.notification.response`, `saga.cabinet.response`, `saga.nutrition.response` | consume | Ответы шагов (event-driven путь) |
| `saga-notification-command`, `saga-nutrition-command`, `saga-trains-command`, `saga-user-command` | produce | Команды шагам (generic путь) |
| `saga-notification-response`, `saga-nutrition-response`, `saga-trains-response`, `saga-user-response` | consume | Ответы шагов (generic путь) |
| `*.compensate` (`notification`, `cabinet`, `user`) | produce | Компенсирующие команды при откате саги |
| `*.DLT` (на все вышеперечисленные) | — | Dead Letter Topics для недоставленных/необработанных сообщений |

## Технологический стек

| Категория | Технологии |
|---|---|
| Язык / платформа | Java 17, Spring Boot 3.2.5, Spring Cloud 2023.0.2 |
| Данные | PostgreSQL + Flyway (`saga_instances`, `outbox_events`), Redis (состояние generic-саги, TTL 7 дней) |
| Messaging | Apache Kafka, Spring Kafka |
| Service discovery | Netflix Eureka Client |
| Observability | Spring Boot Actuator, Prometheus-метрики, health probes (liveness/readiness) |
| Тестирование | JUnit 5, Spring Kafka Test, H2, embedded-redis, Awaitility, JaCoCo (порог покрытия строк 50%, не блокирует сборку) |
| CI/CD | GitHub Actions: тесты с Redis-сервисом, Codecov, Trivy (FS + image), сборка и публикация образа в GHCR, деплой |
| Контейнеризация | Docker (multi-stage build, `eclipse-temurin:17-jre-alpine`, non-root пользователь, `HEALTHCHECK`) |
| Деплой | Kubernetes-манифест (`k8s/deployment.yaml`) |

## Локальный запуск

### Зависимости

JDK 17+, Maven, PostgreSQL, Redis, Kafka, Eureka Server (опционально для регистрации).

### Переменные окружения

```bash
POSTGRES_URL=jdbc:postgresql://localhost:5432/saga_orchestrator
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_HOST=localhost
REDIS_PORT=6379
EUREKA_URI=http://localhost:8761/eureka
```

### Сборка и тесты

```bash
mvn -B verify
```

### Запуск

```bash
mvn spring-boot:run
```

Сервис поднимется на `localhost:8090`, health/metrics — `/actuator/health`, `/actuator/prometheus`.

## Связанные репозитории

- [Training_Notification](https://github.com/Maru3022/Training_Notification) — шаг `NOTIFICATION` саги создания пользователя
- [Training-Nutrition](https://github.com/Maru3022/Training-Nutrition) — шаг `NUTRITION` саги, расчёт КБЖУ
- [Trains-Service](https://github.com/Maru3022/Trains-Service) — шаг `TRAINS` саги (generic путь)
- [Eureka-server](https://github.com/Maru3022/Eureka-server) — service discovery для всей платформы
