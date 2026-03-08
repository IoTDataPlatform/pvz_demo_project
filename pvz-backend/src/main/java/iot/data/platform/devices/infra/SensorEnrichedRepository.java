package iot.data.platform.devices.infra;

import iot.data.platform.devices.core.AggregatedPoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface SensorEnrichedRepository extends JpaRepository<SensorEnrichedRecord, Long> {

    @Query("""
            select new iot.data.platform.devices.core.AggregatedPoint(
                function('date_trunc', :bucket, m.measurementEventTs),
                avg(m.temperature),
                avg(m.humidity),
                count(m.deviceId),
                sum(case when m.online = true then 1 else 0 end),
                sum(case when m.online = false then 1 else 0 end)
            )
            from SensorEnrichedRecord m
            where m.env = :env
              and m.tenant = :tenantId
              and m.deviceId = :deviceId
              and m.measurementEventTs between :fromTs and :toTs
            group by 1
            order by 1
            """)
    List<AggregatedPoint> aggregateByTime(
            @Param("env") String env,
            @Param("tenantId") String tenantId,
            @Param("deviceId") String deviceId,
            @Param("bucket") String bucket,
            @Param("fromTs") Instant fromTs,
            @Param("toTs") Instant toTs
    );
}