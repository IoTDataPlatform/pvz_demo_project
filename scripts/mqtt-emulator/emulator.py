import os
import time
import json
import random
import itertools
import math
from datetime import datetime, timezone
import hashlib

import paho.mqtt.client as mqtt


def parse_devices() -> list[str]:
    devices_raw = os.getenv("DEVICES", "").strip()
    if devices_raw:
        return [x.strip() for x in devices_raw.split(",") if x.strip()]

    device_count = int(os.getenv("DEVICE_COUNT", "5"))
    if device_count <= 0:
        raise ValueError("DEVICE_COUNT must be > 0")

    device_prefix = os.getenv("DEVICE_PREFIX", "device-")
    width = max(3, len(str(device_count)))

    return [f"{device_prefix}{i:0{width}d}" for i in range(1, device_count + 1)]


def device_location(device: str):
    h = int(hashlib.sha1(device.encode()).hexdigest()[:8], 16)

    lat_range = 0.01
    lon_range = 0.02

    lat_seed = (h % 65536) / 65535.0
    lon_seed = ((h // 65536) % 65536) / 65535.0

    lat_off = (lat_seed - 0.5) * lat_range
    lon_off = (lon_seed - 0.5) * lon_range

    lat = BASE_LAT + lat_off
    lon = BASE_LON + lon_off

    lat += random.gauss(0, 0.0001)
    lon += random.gauss(0, 0.0001)

    return lat, lon


MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

ENVIRONMENTS = [x.strip() for x in os.getenv("ENVIRONMENTS", "dev").split(",") if x.strip()]
TENANTS = [x.strip() for x in os.getenv("TENANTS", "tenant-a").split(",") if x.strip()]

DEVICES = parse_devices()

DRYNESS_LEVEL = float(os.getenv("DRYNESS_LEVEL", "0.0"))

REALTIME_INTERVAL_SEC = float(os.getenv("REALTIME_INTERVAL_SEC", "60"))

BACKFILL_ENABLED = os.getenv("BACKFILL_ENABLED", "true").lower() in ("1", "true", "yes")
BACKFILL_FROM_YEAR = int(os.getenv("BACKFILL_FROM_YEAR", "2020"))
BACKFILL_INTERVAL_SEC = float(os.getenv("BACKFILL_INTERVAL_SEC", "60"))

CLIENT_ID = os.getenv("CLIENT_ID", "sensor-emulator")

BASE_LAT = float(os.getenv("BASE_LAT", "54.8433"))
BASE_LON = float(os.getenv("BASE_LON", "83.0931"))

USELESS_PAYLOAD_BYTES = int(os.getenv("USELESS_PAYLOAD_BYTES", "0"))

MONTHLY_HUMIDITY = [
    88,  # Jan
    86,  # Feb
    80,  # Mar
    68,  # Apr
    58,  # May
    64,  # Jun
    69,  # Jul
    70,  # Aug
    72,  # Sep
    76,  # Oct
    82,  # Nov
    85,  # Dec
]

print("MQTT_HOST:", MQTT_HOST)
print("MQTT_PORT:", MQTT_PORT)
print("ENVIRONMENTS:", ENVIRONMENTS)
print("TENANTS:", TENANTS)
print("DEVICES:", DEVICES)
print("REALTIME_INTERVAL_SEC:", REALTIME_INTERVAL_SEC)
print("BACKFILL_FROM_YEAR:", BACKFILL_FROM_YEAR)
print("BACKFILL_INTERVAL_SEC:", BACKFILL_INTERVAL_SEC)
print("BASE_LAT / BASE_LON:", BASE_LAT, BASE_LON)
print("USELESS_PAYLOAD_BYTES:", USELESS_PAYLOAD_BYTES)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _month_of_year(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.month


def make_sensor_topic(env: str, tenant: str, device: str, suffix: str) -> str:
    return f"{env}/{tenant}/sensors/{device}/{suffix}"


def make_device_topic(env: str, tenant: str, device: str, suffix: str) -> str:
    return f"{env}/{tenant}/devices/{device}/{suffix}"


def with_padding(payload: str) -> str:
    if USELESS_PAYLOAD_BYTES <= 0:
        return payload
    return payload + "," + ("X" * USELESS_PAYLOAD_BYTES)


class DeviceState:
    def __init__(self, device_id: str):
        self.id = device_id
        self.seq = 0
        self.humidity = 0.0
        self.temperature = 0.0
        self.rssi = -80
        self.snr = 0.0
        self.battery = 100.0
        self.online = True
        self.lat, self.lon = device_location(device_id)

    def update(self, ts_ms: int):
        self.seq += 1
        self.temperature = seasonal_temperature(ts_ms)
        self.humidity = seasonal_humidity(ts_ms)
        self.rssi = random.randint(-100, -50)
        self.snr = random.uniform(-10.0, 10.0)
        self.battery = max(0.0, min(100.0, self.battery - random.uniform(0.0, 0.05)))
        self.online = True
        self.lat, self.lon = device_location(self.id)

    def humidity_payload(self, ts_ms: int) -> str:
        return with_padding(
            f"{self.id},{iso_from_ms(ts_ms)},{self.humidity:.2f},{self.seq}"
        )

    def temperature_payload(self, ts_ms: int) -> str:
        return with_padding(
            f"{self.id},{iso_from_ms(ts_ms)},{self.temperature:.2f},{self.seq}"
        )

    def state_payload(self, ts_ms: int) -> str:
        return with_padding(
            f"{self.id},{iso_from_ms(ts_ms)},{self.rssi},{self.snr:.2f},{self.battery:.1f},{self.online}"
        )

    def location_payload(self, ts_ms: int) -> str:
        return with_padding(
            f"{self.id},{iso_from_ms(ts_ms)},{self.lat:.6f},{self.lon:.6f}"
        )


DEVICE_STATES = {device: DeviceState(device) for device in DEVICES}


def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Connected to MQTT broker, rc:", reason_code)
    topic_filter = "+/+/devices/+/command"
    client.subscribe(topic_filter, qos=0)
    print("Subscribed to commands:", topic_filter)


def on_message(client, userdata, msg):
    print("Command received:", msg.topic, msg.payload)
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        payload = {}

    parts = msg.topic.split("/")
    if len(parts) < 5:
        print("Unexpected topic format for command, skip")
        return

    env, tenant, kind, device, suffix = parts[0], parts[1], parts[2], parts[3], parts[4]

    cmd_id = payload.get("cmd_id", "unknown")
    ack_topic = make_device_topic(env, tenant, device, "ack")

    ack_payload = {
        "cmd_id": cmd_id,
        "ts": int(time.time() * 1000),
        "status": "ok",
        "details": "Command accepted by emulator"
    }

    client.publish(ack_topic, json.dumps(ack_payload), qos=0, retain=False)
    print("Sent ACK:", ack_topic, ack_payload)


client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
client.on_connect = on_connect
client.on_message = on_message


def connect_mqtt():
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            print("MQTT connect success")
            return
        except Exception as e:
            print("MQTT connect failed, retry in 3s:", e)
            time.sleep(3)


def _day_of_year(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.timetuple().tm_yday


def seasonal_temperature(ts_ms: int) -> float:
    day = _day_of_year(ts_ms)
    phase_shift_days = 200
    angle = 2.0 * math.pi * ((day - 1 - phase_shift_days) / 365.0)

    avg = 5.0
    amp = 25.0
    noise = random.gauss(0, 1.0)

    return avg + amp * math.sin(angle) + noise


def seasonal_humidity(ts_ms: int) -> float:
    month = _month_of_year(ts_ms)

    base_h = MONTHLY_HUMIDITY[month - 1]

    noise = random.gauss(0, 3.0)

    h = base_h + noise - DRYNESS_LEVEL
    return max(0.0, min(100.0, h))


def publish_for_timestamp(ts_ms: int):
    for env, tenant, device in itertools.product(ENVIRONMENTS, TENANTS, DEVICES):
        state = DEVICE_STATES[device]
        state.update(ts_ms)

        humidity_topic = make_sensor_topic(env, tenant, device, "humidity")
        client.publish(humidity_topic, state.humidity_payload(ts_ms), qos=0, retain=False)

        temperature_topic = make_sensor_topic(env, tenant, device, "temperature")
        client.publish(temperature_topic, state.temperature_payload(ts_ms), qos=0, retain=False)

        location_topic = make_sensor_topic(env, tenant, device, "location")
        client.publish(location_topic, state.location_payload(ts_ms), qos=0, retain=False)

        state_topic = make_sensor_topic(env, tenant, device, "state")
        client.publish(state_topic, state.state_payload(ts_ms), qos=0, retain=False)


def backfill_history():
    start_dt = datetime(BACKFILL_FROM_YEAR, 1, 1, tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(time.time() * 1000)

    step_ms = int(BACKFILL_INTERVAL_SEC * 1000)
    if step_ms <= 0:
        raise ValueError("BACKFILL_INTERVAL_SEC must be > 0")

    print(
        f"Starting backfill from {start_dt.isoformat()} to current time, "
        f"step = {BACKFILL_INTERVAL_SEC} sec"
    )

    ts_ms = start_ms
    batch = 0
    while ts_ms < end_ms:
        publish_for_timestamp(ts_ms)
        batch += 1
        if batch % 500 == 0:
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            print(f"Backfill progress: {dt.isoformat()} (batch {batch})")
        ts_ms += step_ms

    print("Backfill finished, total batches:", batch)


def publish_loop():
    if BACKFILL_ENABLED:
        backfill_history()
    else:
        print("Backfill disabled")

    print("Switching to realtime mode, interval =", REALTIME_INTERVAL_SEC, "sec")

    while True:
        now_ms = int(time.time() * 1000)
        publish_for_timestamp(now_ms)
        time.sleep(REALTIME_INTERVAL_SEC)


if __name__ == "__main__":
    connect_mqtt()
    client.loop_start()
    publish_loop()
