package iot.data.platform.devices.infra;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "sensor_enriched")
public class SensorEnrichedRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "device_id")
    private String deviceId;

    private String env;

    private String tenant;

    private Double lat;
    private Double lon;
    private Double humidity;
    private Double temperature;

    @Column(name = "humidity_seq")
    private Long humiditySeq;

    @Column(name = "temperature_seq")
    private Long temperatureSeq;

    @Column(name = "humidity_event_time")
    private String humidityEventTime;

    @Column(name = "temperature_event_time")
    private String temperatureEventTime;

    @Column(name = "state_event_time")
    private String stateEventTime;

    @Column(name = "location_event_time")
    private String locationEventTime;

    @Column(name = "humidity_ingested_at")
    private Long humidityIngestedAt;

    @Column(name = "temperature_ingested_at")
    private Long temperatureIngestedAt;

    @Column(name = "state_ingested_at")
    private Long stateIngestedAt;

    @Column(name = "location_ingested_at")
    private Long locationIngestedAt;

    @Column(name = "measurement_event_ts")
    private Instant measurementEventTs;

    @Column(name = "measurement_processing_ts_ms")
    private Long measurementProcessingTsMs;

    @Column(name = "enriched_processing_ts_ms")
    private Long enrichedProcessingTsMs;

    private Integer rssi;
    private Double snr;
    private Double battery;
    private Boolean online;

    @Column(name = "redis_key")
    private String redisKey;

    protected SensorEnrichedRecord() {
    }

    public String getEnv() {
        return env;
    }

    public String getTenant() {
        return tenant;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public Instant getMeasurementEventTs() {
        return measurementEventTs;
    }

    public Double getTemperature() {
        return temperature;
    }

    public Double getHumidity() {
        return humidity;
    }

    public Boolean getOnline() {
        return online;
    }
}