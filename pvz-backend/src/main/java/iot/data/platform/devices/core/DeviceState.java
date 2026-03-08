package iot.data.platform.devices.core;

public record DeviceState(
        String deviceId,
        String env,
        String tenantId,
        Double lat,
        Double lon,
        Double humidity,
        Double temperature,
        Long measurementTsMs,
        Integer rssi,
        Double snr,
        Double battery,
        Boolean online,
        Long stateIngestedAt,
        Long enrichedProcessingTsMs
) {
}