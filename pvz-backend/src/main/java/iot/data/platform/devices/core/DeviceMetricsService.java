package iot.data.platform.devices.core;

import iot.data.platform.devices.api.DeviceMetricsPointResponse;
import iot.data.platform.devices.api.DeviceMetricsResponse;
import iot.data.platform.devices.infra.SensorEnrichedRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class DeviceMetricsService {

    private final SensorEnrichedRepository repository;

    public DeviceMetricsService(SensorEnrichedRepository repository) {
        this.repository = repository;
    }

    public DeviceMetricsResponse getMetrics(
            String env,
            String tenantId,
            String deviceId,
            MetricsBucket bucket,
            Long fromTs,
            Long toTs
    ) {
        Instant now = Instant.now();
        Instant from;
        Instant to;

        if (fromTs != null && toTs != null && fromTs < toTs) {
            from = Instant.ofEpochSecond(fromTs);
            to = Instant.ofEpochSecond(toTs);
        } else {
            to = now;
            from = switch (bucket) {
                case HOUR -> now.minus(24, ChronoUnit.HOURS);
                case DAY -> now.minus(30, ChronoUnit.DAYS);
                case WEEK -> now.minus(26, ChronoUnit.WEEKS);
            };
        }

        List<AggregatedPoint> aggregated = repository.aggregateByTime(
                env,
                tenantId,
                deviceId,
                bucket.postgresUnit(),
                from,
                to
        );

        List<DeviceMetricsPointResponse> points = aggregated.stream()
                .map(p -> new DeviceMetricsPointResponse(
                        p.bucketStart().getEpochSecond(),
                        p.tAvg(),
                        p.hAvg()
                ))
                .toList();

        return new DeviceMetricsResponse(deviceId, bucket, points);
    }
}
