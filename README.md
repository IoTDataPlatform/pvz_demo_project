# PVZ IoT Data Platform

Демо-кейс, показывающий сценарий работы IoT-платформы для сети датчиков.  

Полный путь данных: **Устройства -> MQTT -> Kafka -> Flink -> Redis/Postgres -> Spring Boot API -> Web UI**

Эмулятор публикует телеметрию устройств (температура/влажность), координаты и состояние (online, RSSI/SNR, батарея). Пайплайн обогащает события, считает агрегаты и предоставляет данные в удобном виде для веб-приложения.

---


## Что показывает Web UI

- **Карта устройств** с текущими значениями и статусом (online/offline)
- **Детали устройства**: последние показания, качество связи, батарея
- **Графики температуры/влажности** за период (агрегация час/день/неделя)
- **Сводка за окно** + простая метрика “засухи” (streak низкой влажности)

<img width="1794" height="1053" alt="Screenshot From 2025-12-17 20-52-03" src="https://github.com/user-attachments/assets/37381f97-549e-477b-82f4-b6e3bd80ed76" />

---

## Структура репозитория

- `pvz-backend/` - **Spring Boot** REST API
  - читает текущие данные из **Redis**
  - читает историю и строит агрегации из **Postgres** (через JPA)
- `pvz-web/` — **Vite + React (TypeScript)** UI
  - карта + панель состояния + графики
- `scripts/` — всё для запуска демо-стека
  - `docker-compose.yml` — поднимает все сервисы
  - `Makefile` — команды для топиков Kafka, коннекторов, Flink jobs
  - `configs/*.json` — конфиги **Kafka Connect** коннекторов
  - `flink-sql/*.sql` — **Flink SQL** джобы (обогащение/агрегации)
  - `mqtt-emulator/` — Python-эмулятор устройств (MQTT)

---

## Инфраструктура и поток данных

### Поднимаются через `scripts/docker-compose.yml`:

**MQTT**
- `mqtt-broker` - брокер MQTT для приёма сообщений от устройств/эмулятора  
- `mqtt-emulator` - имитация датчиков: генерирует события `humidity/location/state`

**Event streaming**
- `kafka` - шина событий и буферизация потоков телеметрии
- `kafka-schema-registry` - хранение Avro-схем сообщений (контракты данных)

**Интеграции**
- `connect` (**Kafka Connect**) - коннекторы для “входа/выхода” данных:
  - **MQTT → Kafka** (ingest телеметрии)
  - **Kafka → Redis** (витрина текущего состояния/сводок)
  - **Kafka → Postgres** (история измерений)
  Конфиги коннекторов: `scripts/configs/*.json`

**Stream processing**
- `flink-jobmanager`, `flink-taskmanager` (**Apache Flink**) - обработка стрима SQL-джобами:
  - обогащение (temporal join: humidity + location/state)
  - расчёт сводки по окну
  - расчёт “засухи” (streak низкой влажности)
  SQL-джобы: `scripts/flink-sql/*.sql`

**Хранилища**
- `redis` - быстрый слой “текущих данных” для API/UI (состояние устройства, сводки, streak)
- `postgres-mqtt` - долговременная история измерений и агрегаты для графиков

**Приложения**
- `pvz-backend` - Spring Boot API: читает текущее из Redis, историю из Postgres
- `pvz-web` - UI (статический билд в nginx), ходит в backend API

---

### Поток данных

1. `mqtt-emulator` публикует MQTT-сообщения (humidity/location/state) в `mqtt-broker`.
2. Kafka Connect (MQTT Source) переносит их в Kafka-топики: `mqtt_humidity`, `mqtt_location`, `mqtt_state`.
3. Flink SQL:
   - собирает “полное состояние” устройства в `mqtt_enriched`
   - формирует поток истории измерений `device_measurements`
   - считает агрегаты/витрины (`mqtt_recent_summary`, `mqtt_low_humidity_streak`)
4. Kafka Connect sinks:
   - пишет `mqtt_enriched`/summary/streak в Redis (для быстрого чтения UI/API)
   - пишет историю измерений в Postgres (для графиков и запросов по времени)
5. `pvz-backend` отдаёт REST API, `pvz-web` визуализирует.

---
