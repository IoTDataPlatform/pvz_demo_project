package iot.data.platform.devices.api;

public record RecentDeviceSnapshotResponse(
        String deviceId,
        long lastSeenTs,
        Double t,
        Double h,
        Boolean online,
        Integer rssi,
        Double snr,
        Double bat
) {
}