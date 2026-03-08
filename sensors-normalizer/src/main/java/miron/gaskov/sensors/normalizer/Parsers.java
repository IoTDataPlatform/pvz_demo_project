package miron.gaskov.sensors.normalizer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.charset.StandardCharsets;

public final class Parsers {

    private Parsers() {
    }

    public static final Schema HUMIDITY_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "HumidityReading",
          "namespace": "miron.gaskov.sensors.normalizer.avro",
          "fields": [
            { "name": "deviceId", "type": "string" },
            { "name": "eventTime", "type": "string" },
            { "name": "humidity", "type": "double" },
            { "name": "seq", "type": "long" },
            { "name": "ingestedAt", "type": "long" },
            { "name": "sourceTopic", "type": "string" }
          ]
        }
        """);

    public static final Schema TEMPERATURE_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "TemperatureReading",
          "namespace": "miron.gaskov.sensors.normalizer.avro",
          "fields": [
            { "name": "deviceId", "type": "string" },
            { "name": "eventTime", "type": "string" },
            { "name": "temperature", "type": "double" },
            { "name": "seq", "type": "long" },
            { "name": "ingestedAt", "type": "long" },
            { "name": "sourceTopic", "type": "string" }
          ]
        }
        """);

    public static final Schema LOCATION_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "LocationReading",
          "namespace": "miron.gaskov.sensors.normalizer.avro",
          "fields": [
            { "name": "deviceId", "type": "string" },
            { "name": "eventTime", "type": "string" },
            { "name": "lat", "type": "double" },
            { "name": "lon", "type": "double" },
            { "name": "ingestedAt", "type": "long" },
            { "name": "sourceTopic", "type": "string" }
          ]
        }
        """);

    public static final Schema STATE_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "DeviceStateReading",
          "namespace": "miron.gaskov.sensors.normalizer.avro",
          "fields": [
            { "name": "deviceId", "type": "string" },
            { "name": "eventTime", "type": "string" },
            { "name": "rssi", "type": "int" },
            { "name": "snr", "type": "double" },
            { "name": "battery", "type": "double" },
            { "name": "online", "type": "boolean" },
            { "name": "ingestedAt", "type": "long" },
            { "name": "sourceTopic", "type": "string" }
          ]
        }
        """);

    public static final Schema PARSE_ERROR_SCHEMA = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "ParseError",
          "namespace": "miron.gaskov.sensors.normalizer.avro",
          "fields": [
            { "name": "sourceTopic", "type": "string" },
            { "name": "rawPayload", "type": "string" },
            { "name": "errorMessage", "type": "string" },
            { "name": "failedAt", "type": "long" }
          ]
        }
        """);

    public static Result parseHumidity(byte[] bytes, String sourceTopic) {
        try {
            String raw = decode(bytes);
            String[] p = raw.split(",", -1);
            if (p.length != 4) {
                throw new IllegalArgumentException("Expected 4 fields, got " + p.length);
            }

            GenericRecord record = new GenericData.Record(HUMIDITY_SCHEMA);
            record.put("deviceId", p[0]);
            record.put("eventTime", p[1]);
            record.put("humidity", Double.parseDouble(p[2]));
            record.put("seq", Long.parseLong(p[3]));
            record.put("ingestedAt", System.currentTimeMillis());
            record.put("sourceTopic", sourceTopic);

            return Result.success(record);
        } catch (Exception e) {
            return Result.failure(error(bytes, sourceTopic, e));
        }
    }

    public static Result parseTemperature(byte[] bytes, String sourceTopic) {
        try {
            String raw = decode(bytes);
            String[] p = raw.split(",", -1);
            if (p.length != 4) {
                throw new IllegalArgumentException("Expected 4 fields, got " + p.length);
            }

            GenericRecord record = new GenericData.Record(TEMPERATURE_SCHEMA);
            record.put("deviceId", p[0]);
            record.put("eventTime", p[1]);
            record.put("temperature", Double.parseDouble(p[2]));
            record.put("seq", Long.parseLong(p[3]));
            record.put("ingestedAt", System.currentTimeMillis());
            record.put("sourceTopic", sourceTopic);

            return Result.success(record);
        } catch (Exception e) {
            return Result.failure(error(bytes, sourceTopic, e));
        }
    }

    public static Result parseLocation(byte[] bytes, String sourceTopic) {
        try {
            String raw = decode(bytes);
            String[] p = raw.split(",", -1);
            if (p.length != 4) {
                throw new IllegalArgumentException("Expected 4 fields, got " + p.length);
            }

            GenericRecord record = new GenericData.Record(LOCATION_SCHEMA);
            record.put("deviceId", p[0]);
            record.put("eventTime", p[1]);
            record.put("lat", Double.parseDouble(p[2]));
            record.put("lon", Double.parseDouble(p[3]));
            record.put("ingestedAt", System.currentTimeMillis());
            record.put("sourceTopic", sourceTopic);

            return Result.success(record);
        } catch (Exception e) {
            return Result.failure(error(bytes, sourceTopic, e));
        }
    }

    public static Result parseState(byte[] bytes, String sourceTopic) {
        try {
            String raw = decode(bytes);
            String[] p = raw.split(",", -1);
            if (p.length != 6) {
                throw new IllegalArgumentException("Expected 6 fields, got " + p.length);
            }

            GenericRecord record = new GenericData.Record(STATE_SCHEMA);
            record.put("deviceId", p[0]);
            record.put("eventTime", p[1]);
            record.put("rssi", Integer.parseInt(p[2]));
            record.put("snr", Double.parseDouble(p[3]));
            record.put("battery", Double.parseDouble(p[4]));
            record.put("online", Boolean.parseBoolean(p[5]));
            record.put("ingestedAt", System.currentTimeMillis());
            record.put("sourceTopic", sourceTopic);

            return Result.success(record);
        } catch (Exception e) {
            return Result.failure(error(bytes, sourceTopic, e));
        }
    }

    private static GenericRecord error(byte[] bytes, String sourceTopic, Exception e) {
        GenericRecord error = new GenericData.Record(PARSE_ERROR_SCHEMA);
        error.put("sourceTopic", sourceTopic);
        error.put("rawPayload", decode(bytes));
        error.put("errorMessage", e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        error.put("failedAt", System.currentTimeMillis());
        return error;
    }

    private static String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public record Result(GenericRecord value, GenericRecord error) {
        public static Result success(GenericRecord value) {
            return new Result(value, null);
        }

        public static Result failure(GenericRecord error) {
            return new Result(null, error);
        }

        public boolean isSuccess() {
            return value != null;
        }
    }
}