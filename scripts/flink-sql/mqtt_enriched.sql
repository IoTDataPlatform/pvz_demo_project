SET 'execution.checkpointing.interval' = '10 s';

DROP TABLE IF EXISTS sensor_measurement;
DROP TABLE IF EXISTS sensor_location_dim;
DROP TABLE IF EXISTS sensor_state_dim;
DROP TABLE IF EXISTS sensor_enriched;

CREATE TABLE sensor_measurement
(
    device_id STRING,
    env STRING,
    tenant STRING,

    humidity DOUBLE,
    temperature DOUBLE,

    humidity_seq BIGINT,
    temperature_seq BIGINT,

    humidity_event_time STRING,
    temperature_event_time STRING,

    humidity_ingested_at BIGINT,
    temperature_ingested_at BIGINT,

    measurement_event_ts TIMESTAMP(3),
    measurement_processing_ts_ms BIGINT,

    WATERMARK FOR measurement_event_ts AS measurement_event_ts - INTERVAL '0.2' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.measurement.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-sensor-measurement',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE sensor_location_dim
(
    deviceId STRING,
    eventTime STRING,
    lat DOUBLE,
    lon DOUBLE,
    ingestedAt BIGINT,
    sourceTopic STRING,

    event_ts AS CAST(REPLACE(REPLACE(eventTime, 'T', ' '), 'Z', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '0.2' SECOND,
    PRIMARY KEY (deviceId) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'sensor.location.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'raw',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE sensor_state_dim
(
    deviceId STRING,
    eventTime STRING,
    rssi INT,
    snr DOUBLE,
    battery DOUBLE,
    online BOOLEAN,
    ingestedAt BIGINT,
    sourceTopic STRING,

    event_ts AS CAST(REPLACE(REPLACE(eventTime, 'T', ' '), 'Z', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '0.2' SECOND,
    PRIMARY KEY (deviceId) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'sensor.state.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'raw',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE sensor_enriched
(
    device_id STRING,
    env STRING,
    tenant STRING,

    lat DOUBLE,
    lon DOUBLE,
    humidity DOUBLE,
    temperature DOUBLE,

    humidity_seq BIGINT,
    temperature_seq BIGINT,

    humidity_event_time STRING,
    temperature_event_time STRING,
    state_event_time STRING,
    location_event_time STRING,

    humidity_ingested_at BIGINT,
    temperature_ingested_at BIGINT,
    state_ingested_at BIGINT,
    location_ingested_at BIGINT,

    measurement_event_ts TIMESTAMP(3),
    measurement_processing_ts_ms BIGINT,
    enriched_processing_ts_ms BIGINT,

    rssi INT,
    snr DOUBLE,
    battery DOUBLE,
    online BOOLEAN,

    redis_key STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.enriched.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );


BEGIN STATEMENT SET;

INSERT INTO sensor_enriched
SELECT
    m.device_id,
    m.env,
    m.tenant,

    l.lat,
    l.lon,
    m.humidity,
    m.temperature,

    m.humidity_seq,
    m.temperature_seq,

    m.humidity_event_time,
    m.temperature_event_time,
    s.eventTime AS state_event_time,
    l.eventTime AS location_event_time,

    m.humidity_ingested_at,
    m.temperature_ingested_at,
    s.ingestedAt AS state_ingested_at,
    l.ingestedAt AS location_ingested_at,

    m.measurement_event_ts,
    m.measurement_processing_ts_ms,
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT) AS enriched_processing_ts_ms,

    s.rssi,
    s.snr,
    s.battery,
    s.online,

    CONCAT('pvz:', m.env, ':', m.tenant, ':device:', m.device_id, ':state') AS redis_key
FROM sensor_measurement AS m
         LEFT JOIN sensor_location_dim FOR SYSTEM_TIME AS OF m.measurement_event_ts AS l
                   ON m.device_id = l.deviceId
         LEFT JOIN sensor_state_dim FOR SYSTEM_TIME AS OF m.measurement_event_ts AS s
                   ON m.device_id = s.deviceId;

END;