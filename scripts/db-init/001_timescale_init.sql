CREATE TABLE sensor_enriched
(
    id                           BIGINT GENERATED ALWAYS AS IDENTITY,
    device_id                    TEXT,
    env                          TEXT,
    tenant                       TEXT,

    lat                          DOUBLE PRECISION,
    lon                          DOUBLE PRECISION,
    humidity                     DOUBLE PRECISION,
    temperature                  DOUBLE PRECISION,

    humidity_seq                 BIGINT,
    temperature_seq              BIGINT,

    humidity_event_time          TEXT,
    temperature_event_time       TEXT,
    state_event_time             TEXT,
    location_event_time          TEXT,

    humidity_ingested_at         BIGINT,
    temperature_ingested_at      BIGINT,
    state_ingested_at            BIGINT,
    location_ingested_at         BIGINT,

    measurement_event_ts         TIMESTAMPTZ NOT NULL,
    measurement_processing_ts_ms BIGINT,
    enriched_processing_ts_ms    BIGINT,

    rssi                         INTEGER,
    snr                          DOUBLE PRECISION,
    battery                      DOUBLE PRECISION,
    online                       BOOLEAN,

    redis_key                    TEXT,

    postgres_inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('sensor_enriched', 'measurement_event_ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sensor_enriched_time
    ON sensor_enriched (measurement_event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_enriched_device_time
    ON sensor_enriched (device_id, measurement_event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_enriched_env_tenant_time
    ON sensor_enriched (env, tenant, measurement_event_ts DESC);