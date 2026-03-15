SET
'execution.checkpointing.interval' = '10 s';

DROP TABLE IF EXISTS sensor_humidity;
DROP TABLE IF EXISTS sensor_temperature;
DROP TABLE IF EXISTS sensor_measurement;

CREATE TABLE sensor_humidity
(
    deviceId    STRING,
    eventTime   STRING,
    humidity DOUBLE,
    seq         BIGINT,
    ingestedAt  BIGINT,
    sourceTopic STRING,

    event_ts AS CAST(REPLACE(REPLACE(eventTime, 'T', ' '), 'Z', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '0.2' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.humidity.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-sensor-humidity',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE sensor_temperature
(
    deviceId    STRING,
    eventTime   STRING,
    temperature DOUBLE,
    seq         BIGINT,
    ingestedAt  BIGINT,
    sourceTopic STRING,

    event_ts AS CAST(REPLACE(REPLACE(eventTime, 'T', ' '), 'Z', '') AS TIMESTAMP(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '0.2' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.temperature.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-sensor-temperature',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE sensor_measurement
(
    device_id                    STRING,
    env                          STRING,
    tenant                       STRING,

    humidity DOUBLE,
    temperature DOUBLE,

    humidity_seq                 BIGINT,
    temperature_seq              BIGINT,

    humidity_event_time          STRING,
    temperature_event_time       STRING,

    humidity_ingested_at         BIGINT,
    temperature_ingested_at      BIGINT,

    measurement_event_ts         TIMESTAMP(3),
    measurement_processing_ts_ms BIGINT,

    WATERMARK FOR measurement_event_ts AS measurement_event_ts - INTERVAL '0.2' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.measurement.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

INSERT INTO sensor_measurement
SELECT h.deviceId                              AS device_id,
       'prod'                                  AS env,
       'tenant-1'                              AS tenant,
       h.humidity                              AS humidity,
       t.temperature                           AS temperature,
       h.seq                                   AS humidity_seq,
       t.seq                                   AS temperature_seq,
       h.eventTime                             AS humidity_event_time,
       t.eventTime                             AS temperature_event_time,
       h.ingestedAt                            AS humidity_ingested_at,
       t.ingestedAt                            AS temperature_ingested_at,
       h.event_ts                              AS measurement_event_ts,
       CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT) AS measurement_processing_ts_ms
FROM sensor_humidity h
         JOIN sensor_temperature t
              ON h.deviceId = t.deviceId
                  AND h.seq = t.seq;