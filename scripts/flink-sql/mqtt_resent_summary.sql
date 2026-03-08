DROP TABLE IF EXISTS sensor_enriched;
DROP TABLE IF EXISTS mqtt_recent_summary;
DROP TABLE IF EXISTS mqtt_low_humidity_streak;

CREATE TABLE sensor_enriched
(
    device_id                    STRING,
    env                          STRING,
    tenant                       STRING,

    lat DOUBLE,
    lon DOUBLE,
    humidity DOUBLE,
    temperature DOUBLE,

    humidity_seq                 BIGINT,
    temperature_seq              BIGINT,

    humidity_event_time          STRING,
    temperature_event_time       STRING,
    state_event_time             STRING,
    location_event_time          STRING,

    humidity_ingested_at         BIGINT,
    temperature_ingested_at      BIGINT,
    state_ingested_at            BIGINT,
    location_ingested_at         BIGINT,

    measurement_event_ts         TIMESTAMP(3),
    measurement_processing_ts_ms BIGINT,
    enriched_processing_ts_ms    BIGINT,

    rssi                         INT,
    snr DOUBLE,
    battery DOUBLE,
    online                       BOOLEAN,

    redis_key                    STRING,

    WATERMARK FOR measurement_event_ts AS measurement_event_ts - INTERVAL '5' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'sensor.enriched.v1',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-sensor-recent-summary',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'avro-confluent',
      'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE mqtt_recent_summary
(
    redis_key                    STRING,
    env                          STRING,
    tenantId                     STRING,
    window_start                 TIMESTAMP(3),
    window_end                   TIMESTAMP(3),
    windowSeconds                BIGINT,
    totalDevices                 BIGINT,
    onlineDevices                BIGINT,
    offlineDevices               BIGINT,
    avgTemp DOUBLE,
    avgHumidity DOUBLE,
    minEventTs                   TIMESTAMP(3),
    maxEventTs                   TIMESTAMP(3),
    minMeasurementProcessingTsMs BIGINT,
    maxMeasurementProcessingTsMs BIGINT,
    minEnrichedProcessingTsMs    BIGINT,
    maxEnrichedProcessingTsMs    BIGINT,
    PRIMARY KEY (redis_key) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'mqtt_recent_summary',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'raw',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

CREATE TABLE mqtt_low_humidity_streak
(
    redis_key                         STRING,
    env                               STRING,
    tenantId                          STRING,
    device_id                         STRING,
    threshold DOUBLE,

    last_event_ts                     TIMESTAMP(3),
    last_ok_event_ts                  TIMESTAMP(3),
    streak_days DOUBLE,
    last_humidity DOUBLE,

    last_measurement_processing_ts_ms BIGINT,
    last_enriched_processing_ts_ms    BIGINT,
    last_humidity_ingested_at         BIGINT,
    PRIMARY KEY (redis_key) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'mqtt_low_humidity_streak',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'raw',
      'value.format' = 'avro-confluent',
      'value.avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
      );

BEGIN STATEMENT
SET;

INSERT INTO mqtt_recent_summary
SELECT CONCAT(env, ':', tenant)                                                 AS redis_key,
       env,
       tenant                                                                   AS tenantId,
       window_start,
       window_end,
       CAST(600 AS BIGINT)                                                      AS windowSeconds,
       COUNT(DISTINCT device_id)                                                AS totalDevices,
       COUNT(DISTINCT CASE WHEN COALESCE(online, FALSE) THEN device_id END)     AS onlineDevices,
       COUNT(DISTINCT CASE WHEN NOT COALESCE(online, FALSE) THEN device_id END) AS offlineDevices,
       AVG(temperature)                                                         AS avgTemp,
       AVG(humidity)                                                            AS avgHumidity,
       MIN(measurement_event_ts)                                                AS minEventTs,
       MAX(measurement_event_ts)                                                AS maxEventTs,
       MIN(measurement_processing_ts_ms)                                        AS minMeasurementProcessingTsMs,
       MAX(measurement_processing_ts_ms)                                        AS maxMeasurementProcessingTsMs,
       MIN(enriched_processing_ts_ms)                                           AS minEnrichedProcessingTsMs,
       MAX(enriched_processing_ts_ms)                                           AS maxEnrichedProcessingTsMs
FROM TABLE(
        HOP(
            TABLE sensor_enriched, DESCRIPTOR(measurement_event_ts), INTERVAL '10' SECOND,
                                                                     INTERVAL '10' MINUTE
        )
     )
GROUP BY env, tenant, window_start, window_end;

INSERT INTO mqtt_low_humidity_streak
WITH agg AS (SELECT env,
                    tenant,
                    device_id,
                    MAX(measurement_event_ts)                                     AS last_event_ts,
                    MAX(CASE WHEN humidity >= 50.0 THEN measurement_event_ts END) AS last_ok_event_ts,
                    MIN(measurement_event_ts)                                     AS first_event_ts
             FROM sensor_enriched
             GROUP BY env, tenant, device_id),
     latest AS (SELECT env,
                       tenant,
                       device_id,
                       measurement_event_ts,
                       humidity,
                       measurement_processing_ts_ms,
                       enriched_processing_ts_ms,
                       humidity_ingested_at
                FROM (SELECT env,
                             tenant,
                             device_id,
                             measurement_event_ts,
                             humidity,
                             measurement_processing_ts_ms,
                             enriched_processing_ts_ms,
                             humidity_ingested_at,
                             ROW_NUMBER() OVER (
                PARTITION BY env, tenant, device_id
                ORDER BY measurement_event_ts DESC
            ) AS rn
                      FROM sensor_enriched)
                WHERE rn = 1)
SELECT CONCAT('pvz:', a.env, ':', a.tenant, ':device:', a.device_id, ':humidity_low_streak') AS redis_key,
       a.env,
       a.tenant                                                                              AS tenantId,
       a.device_id,
       50.0                                                                                  AS threshold,
       a.last_event_ts,
       a.last_ok_event_ts,
       CAST(
               TIMESTAMPDIFF(
                   SECOND, COALESCE(a.last_ok_event_ts, a.first_event_ts),
                           a.last_event_ts
               ) AS DOUBLE
       ) / 86400.0                                                                           AS streak_days,
       l.humidity                                                                            AS last_humidity,
       l.measurement_processing_ts_ms                                                        AS last_measurement_processing_ts_ms,
       l.enriched_processing_ts_ms                                                           AS last_enriched_processing_ts_ms,
       l.humidity_ingested_at                                                                AS last_humidity_ingested_at
FROM agg a
         LEFT JOIN latest l
                   ON a.env = l.env
                       AND a.tenant = l.tenant
                       AND a.device_id = l.device_id
                       AND a.last_event_ts = l.measurement_event_ts;

END;