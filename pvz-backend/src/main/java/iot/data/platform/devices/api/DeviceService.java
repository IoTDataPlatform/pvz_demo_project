package iot.data.platform.devices.api;

import iot.data.platform.devices.core.DeviceState;
import iot.data.platform.devices.infra.RedisDeviceRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DeviceService {
    private final RedisDeviceRepository redisDeviceRepository;

    public DeviceService(RedisDeviceRepository redisDeviceRepository) {
        this.redisDeviceRepository = redisDeviceRepository;
    }

    public List<DeviceState> getAllDevices(String env, String tenantId) {
        return redisDeviceRepository.findAllByTenant(env, tenantId);
    }

    public DeviceState getDevice(String env, String tenantId, String deviceId) {
        return redisDeviceRepository.findById(env, tenantId, deviceId);
    }

    public RecentSummaryResponse getRecentSummary(String env, String tenantId) {
        return redisDeviceRepository.findRecentSummary(env, tenantId);
    }

    public DroughtStreakResponse getDroughtStreak(String env, String tenantId, String deviceId) {
        return redisDeviceRepository.findDroughtStreak(env, tenantId, deviceId);
    }

    public DroughtSummaryResponse getDroughtSummary(String env, String tenantId) {
        return redisDeviceRepository.findDroughtSummary(env, tenantId);
    }

    public List<RecentDeviceSnapshotResponse> getRecentSnapshots(String env, String tenantId) {
        List<DeviceState> states = redisDeviceRepository.findAllByTenant(env, tenantId);
        List<RecentDeviceSnapshotResponse> result = new ArrayList<>();

        for (DeviceState s : states) {
            long lastSeen = 0L;

            if (s.measurementTsMs() != null) {
                lastSeen = Math.max(lastSeen, s.measurementTsMs());
            }
            if (s.stateIngestedAt() != null) {
                lastSeen = Math.max(lastSeen, s.stateIngestedAt());
            }
            if (s.enrichedProcessingTsMs() != null) {
                lastSeen = Math.max(lastSeen, s.enrichedProcessingTsMs());
            }

            result.add(new RecentDeviceSnapshotResponse(
                    s.deviceId(),
                    lastSeen,
                    s.temperature(),
                    s.humidity(),
                    s.online(),
                    s.rssi(),
                    s.snr(),
                    s.battery()
            ));
        }

        return result;
    }
}