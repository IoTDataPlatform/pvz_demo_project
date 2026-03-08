package iot.data.platform.devices.infra;

import iot.data.platform.devices.api.DroughtStreakResponse;
import iot.data.platform.devices.api.DroughtSummaryResponse;
import iot.data.platform.devices.api.RecentSummaryResponse;
import iot.data.platform.devices.core.DeviceState;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository
public class RedisDeviceRepository {
    private final RedisTemplate<String, String> redisTemplate;
    private final HashOperations<String, String, String> hashOps;

    public RedisDeviceRepository(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.hashOps = redisTemplate.opsForHash();
    }

    public List<DeviceState> findAllByTenant(String env, String tenantId) {
        String pattern = String.format("pvz:%s:%s:device:*:state", env, tenantId);
        Set<String> stateKeys = redisTemplate.keys(pattern);

        if (stateKeys == null || stateKeys.isEmpty()) {
            return List.of();
        }

        List<DeviceState> result = new ArrayList<>();
        for (String stateKey : stateKeys) {
            String deviceId = extractDeviceId(stateKey);
            if (deviceId == null) continue;

            DeviceState state = findById(env, tenantId, deviceId);
            if (state != null) {
                result.add(state);
            }
        }
        return result;
    }

    private String extractDeviceId(String stateKey) {
        int deviceIdx = stateKey.indexOf(":device:");
        int stateIdx = stateKey.lastIndexOf(":state");
        if (deviceIdx < 0 || stateIdx < 0 || stateIdx <= deviceIdx + 8) {
            return null;
        }
        return stateKey.substring(deviceIdx + 8, stateIdx);
    }

    public DeviceState findById(String env, String tenantId, String deviceId) {
        String stateKey = RedisKeys.deviceState(env, tenantId, deviceId);
        Map<String, String> map;
        try {
            map = hashOps.entries(stateKey);
        } catch (DataAccessException e) {
            return null;
        }
        if (map == null || map.isEmpty()) return null;

        return new DeviceState(
                deviceId,
                env,
                tenantId,
                parseDoubleOrNull(map.get("lat")),
                parseDoubleOrNull(map.get("lon")),
                parseDoubleOrNull(map.get("humidity")),
                parseDoubleOrNull(map.get("temperature")),
                parseTimestampMillisOrNull(map.get("measurement_event_ts")),
                parseIntOrNull(map.get("rssi")),
                parseDoubleOrNull(map.get("snr")),
                parseDoubleOrNull(map.get("battery")),
                parseBoolOrNull(map.get("online")),
                parseLongOrNull(map.get("state_ingested_at")),
                parseLongOrNull(map.get("enriched_processing_ts_ms"))
        );
    }

    public RecentSummaryResponse findRecentSummary(String env, String tenantId) {
        String redisKey = env + ":" + tenantId;

        Map<String, String> map;
        try {
            map = hashOps.entries(redisKey);
        } catch (DataAccessException e) {
            return emptySummary(env, tenantId);
        }

        return map == null || map.isEmpty()
                ? emptySummary(env, tenantId)
                : new RecentSummaryResponse(
                env,
                tenantId,
                600,
                parseInt(map.get("totalDevices")),
                parseInt(map.get("onlineDevices")),
                parseInt(map.get("offlineDevices")),
                parseDoubleOrNull(map.get("avgTemp")),
                parseDoubleOrNull(map.get("avgHumidity"))
        );
    }

    public DroughtStreakResponse findDroughtStreak(String env, String tenantId, String deviceId) {
        String key = RedisKeys.deviceHumidityLowStreak(env, tenantId, deviceId);

        Map<String, String> map;
        try {
            map = hashOps.entries(key);
        } catch (DataAccessException e) {
            return null;
        }
        if (map == null || map.isEmpty()) return null;

        Double threshold = parseDoubleOrNull(map.get("threshold"));
        Long lastTs = parseTimestampMillisOrNull(map.get("last_event_ts"));
        Long lastOkTs = parseTimestampMillisOrNull(map.get("last_ok_event_ts"));
        Double streakDays = parseDoubleOrNull(map.get("streak_days"));
        Double lastH = parseDoubleOrNull(map.get("last_humidity"));

        return new DroughtStreakResponse(
                env,
                tenantId,
                deviceId,
                threshold,
                lastTs,
                lastOkTs,
                streakDays,
                lastH
        );
    }

    public DroughtSummaryResponse findDroughtSummary(String env, String tenantId) {
        Set<String> keys = redisTemplate.keys(RedisKeys.deviceHumidityLowStreakPattern(env, tenantId));
        if (keys == null || keys.isEmpty()) {
            return new DroughtSummaryResponse(env, tenantId, null, 0, 0.0, null);
        }

        int inDrought = 0;
        double maxDays = 0.0;
        String maxDeviceId = null;
        Double threshold = null;

        for (String key : keys) {
            Map<String, String> map;
            try {
                map = hashOps.entries(key);
            } catch (DataAccessException e) {
                continue;
            }
            if (map == null || map.isEmpty()) continue;

            if (threshold == null) {
                threshold = parseDoubleOrNull(map.get("threshold"));
            }

            Double days = parseDoubleOrNull(map.get("streak_days"));
            if (days == null) continue;

            if (days > 0) inDrought++;

            if (days > maxDays) {
                maxDays = days;
                maxDeviceId = extractDeviceIdFromStreakKey(key);
            }
        }

        return new DroughtSummaryResponse(env, tenantId, threshold, inDrought, maxDays, maxDeviceId);
    }

    private String extractDeviceIdFromStreakKey(String streakKey) {
        int deviceIdx = streakKey.indexOf(":device:");
        int tailIdx = streakKey.lastIndexOf(":humidity_low_streak");
        if (deviceIdx < 0 || tailIdx < 0 || tailIdx <= deviceIdx + 8) return null;
        return streakKey.substring(deviceIdx + 8, tailIdx);
    }

    private RecentSummaryResponse emptySummary(String env, String tenantId) {
        return new RecentSummaryResponse(
                env,
                tenantId,
                600,
                0,
                0,
                0,
                null,
                null
        );
    }

    private int parseInt(String v) {
        if (v == null || v.isBlank()) return 0;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private Integer parseIntOrNull(String v) {
        if (v == null || v.isBlank()) return null;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Long parseLongOrNull(String v) {
        if (v == null || v.isBlank()) return null;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Double parseDoubleOrNull(String v) {
        if (v == null || v.isBlank()) return null;
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Boolean parseBoolOrNull(String v) {
        if (v == null || v.isBlank()) return null;
        return switch (v.toLowerCase()) {
            case "1", "true", "yes", "on" -> true;
            case "0", "false", "no", "off" -> false;
            default -> null;
        };
    }

    private Long parseTimestampMillisOrNull(String v) {
        if (v == null || v.isBlank()) return null;

        try {
            long n = Long.parseLong(v);
            return n < 10_000_000_000L ? n * 1000 : n;
        } catch (NumberFormatException ignored) {
        }

        try {
            return Instant.parse(v).toEpochMilli();
        } catch (Exception ignored) {
        }

        try {
            return LocalDateTime.parse(v).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception ignored) {
        }

        return null;
    }
}