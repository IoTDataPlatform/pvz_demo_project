CONNECT_HOST ?= localhost
CONNECT_PORT ?= 8083
CONNECT_URL  := http://$(CONNECT_HOST):$(CONNECT_PORT)

CONFIG ?= ./configs/mqtt-sensors-raw-source.json
CONNECTOR := $(basename $(notdir $(CONFIG:.json=)))

FLINK_JOBMANAGER ?= flink-jobmanager
FLINK_BIN := docker exec -i $(FLINK_JOBMANAGER) /opt/flink/bin

FLINK_SQL_MEASUREMENT_PATH ?= /opt/flink/sql/mqtt_measurement.sql
FLINK_SQL_ENRICHED_PATH ?= /opt/flink/sql/mqtt_enriched.sql
FLINK_SQL_RESENT_SUMMARY_PATH ?= /opt/flink/sql/mqtt_resent_summary.sql

KAFKA_BIN := docker compose exec -i kafka /usr/bin

.PHONY: kafka-topics-init \
        connector-up connector-down connect-connectors \
        mqtt-sensors-raw-source-up mqtt-sensors-raw-source-down \
        sensor-enriched-postgres-sink-up sensor-enriched-postgres-sink-down \
        sensor-enriched-redis-sink-up sensor-enriched-redis-sink-down \
        mqtt-analytics-redis-sink-up mqtt-analytics-redis-sink-down \
        flink-measurement-up flink-enriched-up flink-summary-up \
        flink-jobs-up flink-jobs-down flink-job-list \
        stack-up stack-down emulator-up emulator-down \
        connectors-up connectors-down \
        platform-start platform-stop

kafka-topics-init:
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_humidity_raw --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_temperature_raw --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_location_raw --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_state_raw --partitions 1 --replication-factor 1 || true

	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.humidity.v1 --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.temperature.v1 --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.location.v1 --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.state.v1 --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.parse.errors --partitions 1 --replication-factor 1 || true

	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.measurement.v1 --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.enriched.v1 --partitions 1 --replication-factor 1 || true

	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_recent_summary --partitions 1 --replication-factor 1 || true
	@$(KAFKA_BIN)/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_low_humidity_streak --partitions 1 --replication-factor 1 || true

	@echo "Kafka topics ready."

connector-up:
	@curl -sS -X POST "$(CONNECT_URL)/connectors" \
	  -H "Content-Type: application/json" \
	  --data @$(CONFIG) \
	  && echo "" || echo "Request failed"

connector-down:
	@curl -sS -X DELETE "$(CONNECT_URL)/connectors/$(CONNECTOR)" \
	  && echo "" || echo "Request failed"

connect-connectors:
	@curl -sS "$(CONNECT_URL)/connectors" && echo "" || echo "Request failed"

mqtt-sensors-raw-source-up:
	$(MAKE) connector-up CONFIG=./configs/mqtt-sensors-raw-source.json

mqtt-sensors-raw-source-down:
	$(MAKE) connector-down CONFIG=./configs/mqtt-sensors-raw-source.json

sensor-enriched-postgres-sink-up:
	$(MAKE) connector-up CONFIG=./configs/sensor-enriched-postgres-sink.json

sensor-enriched-postgres-sink-down:
	$(MAKE) connector-down CONFIG=./configs/sensor-enriched-postgres-sink.json

sensor-enriched-redis-sink-up:
	$(MAKE) connector-up CONFIG=./configs/sensor-enriched-redis-sink.json

sensor-enriched-redis-sink-down:
	$(MAKE) connector-down CONFIG=./configs/sensor-enriched-redis-sink.json

mqtt-analytics-redis-sink-up:
	$(MAKE) connector-up CONFIG=./configs/mqtt-analytics-redis-sink.json

mqtt-analytics-redis-sink-down:
	$(MAKE) connector-down CONFIG=./configs/mqtt-analytics-redis-sink.json

flink-measurement-up:
	@echo "Starting measurement job..."
	@$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_MEASUREMENT_PATH)

flink-enriched-up:
	@echo "Starting enriched job..."
	@$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_ENRICHED_PATH)

flink-summary-up:
	@echo "Starting summary job..."
	@$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_RESENT_SUMMARY_PATH)

flink-jobs-up: kafka-topics-init
	@$(MAKE) flink-measurement-up
	@$(MAKE) flink-enriched-up
	@$(MAKE) flink-summary-up

flink-jobs-down:
	@jobs=`$(FLINK_BIN)/flink list | awk '/RUNNING/ {print $$4}'`; \
	if [ -z "$$jobs" ]; then \
	  echo "No running Flink jobs found"; \
	else \
	  for job in $$jobs; do \
	    echo "Cancelling Flink job $$job..."; \
	    $(FLINK_BIN)/flink cancel $$job || echo "Failed to cancel $$job"; \
	  done; \
	  echo "Done."; \
	fi

flink-job-list:
	@$(FLINK_BIN)/flink list -a

stack-up:
	@docker compose up -d --build
	@echo "Base containers up."

stack-down:
	@docker compose down
	@echo "Stack down."

emulator-up:
	@docker compose up -d mqtt-emulator
	@echo "MQTT emulator started."

emulator-down:
	@docker compose rm -sf mqtt-emulator >/dev/null 2>&1 || true
	@echo "MQTT emulator stopped."

connectors-up:
	@$(MAKE) mqtt-sensors-raw-source-up
	@$(MAKE) sensor-enriched-postgres-sink-up
	@$(MAKE) sensor-enriched-redis-sink-up
	@$(MAKE) mqtt-analytics-redis-sink-up
	@echo "All connectors up."

connectors-down:
	@$(MAKE) mqtt-analytics-redis-sink-down || true
	@$(MAKE) sensor-enriched-redis-sink-down || true
	@$(MAKE) sensor-enriched-postgres-sink-down || true
	@$(MAKE) mqtt-sensors-raw-source-down || true
	@echo "All connectors down."

platform-start: stack-up
	@$(MAKE) emulator-down
	@$(MAKE) connectors-up
	@$(MAKE) flink-jobs-up
	@$(MAKE) emulator-up
	@echo "Platform started."

platform-stop:
	@$(MAKE) emulator-down
	@$(MAKE) connectors-down
	@$(MAKE) flink-jobs-down || true
	@$(MAKE) stack-down
	@echo "Platform stopped."