SHELL := /bin/bash

COMPOSE ?= docker compose
KUBECTL ?= kubectl
HELM ?= helm
CURL ?= curl

PROJECT_DIR := $(abspath .)
CHART_DIR ?= ./charts/pvz-infra
K8S_NAMESPACE ?= pvz
HELM_RELEASE ?= pvz

CONNECT_HOST ?= localhost
CONNECT_PORT ?= 8083
CONNECT_URL := http://$(CONNECT_HOST):$(CONNECT_PORT)

DOCKER_COMPOSE_BASE := -f docker-compose.yml
DOCKER_COMPOSE_HYBRID := -f docker-compose.hybrid.yml

OUTER_SERVICES := mqtt-broker mqttx-web pvz-backend pvz-web
OUTER_SERVICES_WITH_EMULATOR := $(OUTER_SERVICES) mqtt-emulator

CONFIG_DIR := ./configs
CONFIG_MQTT_RAW := $(CONFIG_DIR)/mqtt-sensors-raw-source.json
CONFIG_PG_SINK := $(CONFIG_DIR)/sensor-enriched-postgres-sink.json
CONFIG_REDIS_SINK := $(CONFIG_DIR)/sensor-enriched-redis-sink.json
CONFIG_ANALYTICS_REDIS := $(CONFIG_DIR)/mqtt-analytics-redis-sink.json

FLINK_SQL_MEASUREMENT_PATH ?= /opt/flink/sql/mqtt_measurement.sql
FLINK_SQL_ENRICHED_PATH ?= /opt/flink/sql/mqtt_enriched.sql
FLINK_SQL_RESENT_SUMMARY_PATH ?= /opt/flink/sql/mqtt_resent_summary.sql

KAFKA_POD_SELECTOR := app.kubernetes.io/instance=$(HELM_RELEASE),app.kubernetes.io/component=kafka
FLINK_JM_SELECTOR := app.kubernetes.io/instance=$(HELM_RELEASE),app.kubernetes.io/component=flink-jobmanager

KIND_CLUSTER ?= kind

.PHONY: \
  print-paths \
  image-build-sensor-normalizer image-build-backend image-build-web image-build-emulator image-build-all \
  kind-load-all \
  stack-up stack-down emulator-up emulator-down \
  connector-up connector-down connect-connectors \
  mqtt-sensors-raw-source-up mqtt-sensors-raw-source-down \
  sensor-enriched-postgres-sink-up sensor-enriched-postgres-sink-down \
  sensor-enriched-redis-sink-up sensor-enriched-redis-sink-down \
  mqtt-analytics-redis-sink-up mqtt-analytics-redis-sink-down \
  connectors-up connectors-down \
  kafka-topics-init \
  flink-measurement-up flink-enriched-up flink-summary-up flink-jobs-up flink-jobs-down flink-job-list \
  platform-start platform-stop \
  helm-lint helm-template \
  infra-k8s-up infra-k8s-down infra-k8s-status \
  wait-kafka-k8s wait-connect-k8s wait-postgres-k8s wait-flink-k8s \
  kafka-topics-init-k8s \
  connector-up-k8s connector-down-k8s connect-connectors-k8s \
  mqtt-sensors-raw-source-up-k8s mqtt-sensors-raw-source-down-k8s \
  sensor-enriched-postgres-sink-up-k8s sensor-enriched-postgres-sink-down-k8s \
  sensor-enriched-redis-sink-up-k8s sensor-enriched-redis-sink-down-k8s \
  mqtt-analytics-redis-sink-up-k8s mqtt-analytics-redis-sink-down-k8s \
  connectors-up-k8s connectors-down-k8s \
  flink-measurement-up-k8s flink-enriched-up-k8s flink-summary-up-k8s flink-jobs-up-k8s flink-jobs-down-k8s flink-job-list-k8s \
  outer-up-hybrid outer-down-hybrid emulator-up-hybrid emulator-down-hybrid \
  platform-hybrid-start platform-hybrid-stop

print-paths:
	@echo "PROJECT_DIR=$(PROJECT_DIR)"
	@echo "CHART_DIR=$(CHART_DIR)"
	@echo "K8S_NAMESPACE=$(K8S_NAMESPACE)"
	@echo "HELM_RELEASE=$(HELM_RELEASE)"
	@echo "CONNECT_URL=$(CONNECT_URL)"

# --------------------------------------------------
# image build
# --------------------------------------------------

image-build-sensor-normalizer:
	docker build -t sensor-normalizer:latest ../sensors-normalizer

image-build-backend:
	docker build -t pvz-backend:latest ../pvz-backend

image-build-web:
	docker build -t pvz-web:latest ../pvz-web

image-build-emulator:
	docker build -t mqtt-emulator:latest ./mqtt-emulator

image-build-all: image-build-sensor-normalizer image-build-backend image-build-web image-build-emulator

kind-load-all:
	kind load docker-image sensor-normalizer:latest --name $(KIND_CLUSTER)

# --------------------------------------------------
# docker-only mode
# --------------------------------------------------

stack-up:
	$(COMPOSE) $(DOCKER_COMPOSE_BASE) up -d --build

stack-down:
	$(COMPOSE) $(DOCKER_COMPOSE_BASE) down -v

emulator-up:
	$(COMPOSE) $(DOCKER_COMPOSE_BASE) up -d mqtt-emulator

emulator-down:
	$(COMPOSE) $(DOCKER_COMPOSE_BASE) rm -sf mqtt-emulator >/dev/null 2>&1 || true

connector-up:
	$(CURL) -fsS -X POST "$(CONNECT_URL)/connectors" \
	  -H "Content-Type: application/json" \
	  --data @"$(CONFIG)" || true
	@echo

connector-down:
	$(CURL) -fsS -X DELETE "$(CONNECT_URL)/connectors/$(CONNECTOR)" || true
	@echo

connect-connectors:
	$(CURL) -fsS "$(CONNECT_URL)/connectors" || true
	@echo

CONFIG ?= ./configs/mqtt-sensors-raw-source.json
CONNECTOR := $(basename $(notdir $(CONFIG:.json=)))

mqtt-sensors-raw-source-up:
	$(MAKE) connector-up CONFIG=$(CONFIG_MQTT_RAW)

mqtt-sensors-raw-source-down:
	$(MAKE) connector-down CONFIG=$(CONFIG_MQTT_RAW)

sensor-enriched-postgres-sink-up:
	$(MAKE) connector-up CONFIG=$(CONFIG_PG_SINK)

sensor-enriched-postgres-sink-down:
	$(MAKE) connector-down CONFIG=$(CONFIG_PG_SINK)

sensor-enriched-redis-sink-up:
	$(MAKE) connector-up CONFIG=$(CONFIG_REDIS_SINK)

sensor-enriched-redis-sink-down:
	$(MAKE) connector-down CONFIG=$(CONFIG_REDIS_SINK)

mqtt-analytics-redis-sink-up:
	$(MAKE) connector-up CONFIG=$(CONFIG_ANALYTICS_REDIS)

mqtt-analytics-redis-sink-down:
	$(MAKE) connector-down CONFIG=$(CONFIG_ANALYTICS_REDIS)

connectors-up:
	$(MAKE) mqtt-sensors-raw-source-up
	$(MAKE) sensor-enriched-postgres-sink-up
	$(MAKE) sensor-enriched-redis-sink-up
	$(MAKE) mqtt-analytics-redis-sink-up

connectors-down:
	$(MAKE) mqtt-analytics-redis-sink-down || true
	$(MAKE) sensor-enriched-redis-sink-down || true
	$(MAKE) sensor-enriched-postgres-sink-down || true
	$(MAKE) mqtt-sensors-raw-source-down || true

kafka-topics-init:
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_humidity_raw --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_temperature_raw --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_location_raw --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_state_raw --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.humidity.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.temperature.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.location.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.state.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.parse.errors --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.measurement.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic sensor.enriched.v1 --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_recent_summary --partitions 1 --replication-factor 1 || true
	docker compose exec -T kafka /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic mqtt_low_humidity_streak --partitions 1 --replication-factor 1 || true

FLINK_JOBMANAGER ?= flink-jobmanager
FLINK_BIN := docker exec -i $(FLINK_JOBMANAGER) /opt/flink/bin

flink-measurement-up:
	$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_MEASUREMENT_PATH)

flink-enriched-up:
	$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_ENRICHED_PATH)

flink-summary-up:
	$(FLINK_BIN)/sql-client.sh -f $(FLINK_SQL_RESENT_SUMMARY_PATH)

flink-jobs-up: kafka-topics-init
	$(MAKE) flink-measurement-up
	$(MAKE) flink-enriched-up
	$(MAKE) flink-summary-up

flink-jobs-down:
	@jobs=`$(FLINK_BIN)/flink list | awk '/RUNNING/ {print $$4}'`; \
	if [ -z "$$jobs" ]; then \
	  echo "No running Flink jobs found"; \
	else \
	  for job in $$jobs; do \
	    echo "Cancelling Flink job $$job"; \
	    $(FLINK_BIN)/flink cancel $$job || true; \
	  done; \
	fi

flink-job-list:
	$(FLINK_BIN)/flink list -a

platform-start: stack-up
	$(MAKE) emulator-down
	$(MAKE) connectors-up
	$(MAKE) flink-jobs-up
	$(MAKE) emulator-up

platform-stop:
	$(MAKE) emulator-down
	$(MAKE) connectors-down || true
	$(MAKE) flink-jobs-down || true
	$(MAKE) stack-down

# --------------------------------------------------
# helm / k8s
# --------------------------------------------------

helm-lint:
	$(HELM) lint $(CHART_DIR)

helm-template:
	$(HELM) template $(HELM_RELEASE) $(CHART_DIR)

infra-k8s-up:
	$(HELM) upgrade --install $(HELM_RELEASE) $(CHART_DIR) \
	  --namespace $(K8S_NAMESPACE) \
	  --create-namespace \
	  --set hostPaths.projectRoot="$(PROJECT_DIR)/.." \
	  --set hostPaths.connectPlugins="$(PROJECT_DIR)/connect-plugins" \
	  --set hostPaths.connectorConfigs="$(PROJECT_DIR)/configs" \
	  --set hostPaths.flinkLib="$(PROJECT_DIR)/flink-lib" \
	  --set hostPaths.flinkSql="$(PROJECT_DIR)/flink-sql" \
	  --set hostPaths.dbInit="$(PROJECT_DIR)/db-init" \
	  --wait

infra-k8s-down:
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(K8S_NAMESPACE) || true

infra-k8s-status:
	$(KUBECTL) -n $(K8S_NAMESPACE) get pods
	$(KUBECTL) -n $(K8S_NAMESPACE) get svc

wait-kafka-k8s:
	$(KUBECTL) -n $(K8S_NAMESPACE) rollout status deploy/$(HELM_RELEASE)-pvz-infra-kafka --timeout=300s

wait-connect-k8s:
	$(KUBECTL) -n $(K8S_NAMESPACE) rollout status deploy/$(HELM_RELEASE)-pvz-infra-connect --timeout=300s

wait-postgres-k8s:
	$(KUBECTL) -n $(K8S_NAMESPACE) rollout status deploy/$(HELM_RELEASE)-pvz-infra-postgres --timeout=300s

wait-flink-k8s:
	$(KUBECTL) -n $(K8S_NAMESPACE) rollout status deploy/$(HELM_RELEASE)-pvz-infra-flink-jobmanager --timeout=300s
	$(KUBECTL) -n $(K8S_NAMESPACE) rollout status deploy/$(HELM_RELEASE)-pvz-infra-flink-taskmanager --timeout=300s

kafka-topics-init-k8s:
	@KAFKA_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(KAFKA_POD_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_humidity_raw --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_temperature_raw --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_location_raw --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_state_raw --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.humidity.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.temperature.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.location.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.state.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.parse.errors --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.measurement.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic sensor.enriched.v1 --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_recent_summary --partitions 1 --replication-factor 1 || true; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$KAFKA_POD -- /usr/bin/kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic mqtt_low_humidity_streak --partitions 1 --replication-factor 1 || true

define apply_connector_config_k8s
	tmpfile=$$(mktemp); \
	cat "$(1)" \
	  | sed 's/mqtt-broker/host.docker.internal/g' \
	  | sed 's/\bredis\b/$(HELM_RELEASE)-pvz-infra-redis/g' \
	  | sed 's/\bpvz-db\b/$(HELM_RELEASE)-pvz-infra-postgres/g' \
	  | sed 's/kafka-schema-registry/$(HELM_RELEASE)-pvz-infra-schema-registry/g' \
	  > $$tmpfile; \
	$(CURL) -fsS -X POST "$(CONNECT_URL)/connectors" \
	  -H "Content-Type: application/json" \
	  --data @$$tmpfile || true; \
	echo; \
	rm -f $$tmpfile
endef

define delete_connector_config
	connector_name=$$(basename "$(1)" .json); \
	$(CURL) -fsS -X DELETE "$(CONNECT_URL)/connectors/$$connector_name" || true; \
	echo
endef

connector-up-k8s:
	$(call apply_connector_config_k8s,$(CONFIG))

connector-down-k8s:
	$(call delete_connector_config,$(CONFIG))

connect-connectors-k8s:
	$(CURL) -fsS "$(CONNECT_URL)/connectors" || true
	@echo

mqtt-sensors-raw-source-up-k8s:
	$(MAKE) connector-up-k8s CONFIG=$(CONFIG_MQTT_RAW)

mqtt-sensors-raw-source-down-k8s:
	$(MAKE) connector-down-k8s CONFIG=$(CONFIG_MQTT_RAW)

sensor-enriched-postgres-sink-up-k8s:
	$(MAKE) connector-up-k8s CONFIG=$(CONFIG_PG_SINK)

sensor-enriched-postgres-sink-down-k8s:
	$(MAKE) connector-down-k8s CONFIG=$(CONFIG_PG_SINK)

sensor-enriched-redis-sink-up-k8s:
	$(MAKE) connector-up-k8s CONFIG=$(CONFIG_REDIS_SINK)

sensor-enriched-redis-sink-down-k8s:
	$(MAKE) connector-down-k8s CONFIG=$(CONFIG_REDIS_SINK)

mqtt-analytics-redis-sink-up-k8s:
	$(MAKE) connector-up-k8s CONFIG=$(CONFIG_ANALYTICS_REDIS)

mqtt-analytics-redis-sink-down-k8s:
	$(MAKE) connector-down-k8s CONFIG=$(CONFIG_ANALYTICS_REDIS)

connectors-up-k8s:
	$(MAKE) mqtt-sensors-raw-source-up-k8s
	$(MAKE) sensor-enriched-postgres-sink-up-k8s
	$(MAKE) sensor-enriched-redis-sink-up-k8s
	$(MAKE) mqtt-analytics-redis-sink-up-k8s

connectors-down-k8s:
	$(MAKE) mqtt-analytics-redis-sink-down-k8s || true
	$(MAKE) sensor-enriched-redis-sink-down-k8s || true
	$(MAKE) sensor-enriched-postgres-sink-down-k8s || true
	$(MAKE) mqtt-sensors-raw-source-down-k8s || true

flink-measurement-up-k8s:
	@JM_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(FLINK_JM_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/sql-client.sh -f $(FLINK_SQL_MEASUREMENT_PATH)

flink-enriched-up-k8s:
	@JM_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(FLINK_JM_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/sql-client.sh -f $(FLINK_SQL_ENRICHED_PATH)

flink-summary-up-k8s:
	@JM_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(FLINK_JM_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/sql-client.sh -f $(FLINK_SQL_RESENT_SUMMARY_PATH)

flink-jobs-up-k8s: kafka-topics-init-k8s
	$(MAKE) flink-measurement-up-k8s
	$(MAKE) flink-enriched-up-k8s
	$(MAKE) flink-summary-up-k8s

flink-jobs-down-k8s:
	@JM_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(FLINK_JM_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	jobs=`$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/flink list | awk '/RUNNING/ {print $$4}'`; \
	if [ -z "$$jobs" ]; then \
	  echo "No running Flink jobs found"; \
	else \
	  for job in $$jobs; do \
	    echo "Cancelling Flink job $$job"; \
	    $(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/flink cancel $$job || true; \
	  done; \
	fi

flink-job-list-k8s:
	@JM_POD=`$(KUBECTL) -n $(K8S_NAMESPACE) get pod -l $(FLINK_JM_SELECTOR) -o jsonpath='{.items[0].metadata.name}'`; \
	$(KUBECTL) -n $(K8S_NAMESPACE) exec -i $$JM_POD -- /opt/flink/bin/flink list -a

# --------------------------------------------------
# hybrid mode
# --------------------------------------------------

outer-up-hybrid:
	$(COMPOSE) $(DOCKER_COMPOSE_HYBRID) up -d --build $(OUTER_SERVICES)

outer-down-hybrid:
	$(COMPOSE) $(DOCKER_COMPOSE_HYBRID) rm -sf $(OUTER_SERVICES_WITH_EMULATOR) >/dev/null 2>&1 || true

emulator-up-hybrid:
	$(COMPOSE) $(DOCKER_COMPOSE_HYBRID) up -d mqtt-emulator

emulator-down-hybrid:
	$(COMPOSE) $(DOCKER_COMPOSE_HYBRID) rm -sf mqtt-emulator >/dev/null 2>&1 || true

platform-hybrid-start: image-build-all kind-load-all infra-k8s-up outer-up-hybrid
	$(MAKE) emulator-down-hybrid || true
	$(MAKE) kafka-topics-init-k8s
	$(MAKE) connectors-up-k8s
	$(MAKE) flink-jobs-up-k8s
	$(MAKE) emulator-up-hybrid
	@echo "Hybrid platform started"
	@echo "Kafka UI:  http://localhost:8080"
	@echo "Backend:   http://localhost:8085"
	@echo "Frontend:  http://localhost:3000"
	@echo "MQTTX Web: http://localhost:8086"

platform-hybrid-stop:
	$(MAKE) emulator-down-hybrid || true
	$(MAKE) connectors-down-k8s || true
	$(MAKE) flink-jobs-down-k8s || true
	$(MAKE) outer-down-hybrid || true
	$(MAKE) infra-k8s-down || true
	@echo "Hybrid platform stopped"