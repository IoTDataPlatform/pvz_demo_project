package miron.gaskov.sensors.normalizer;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SensorNormalizerApp {

    public static void main(String[] args) throws Exception {
        Properties appProps = loadApplicationProperties();

        String bootstrapServers = required(appProps, "bootstrap.servers");
        String schemaRegistryUrl = required(appProps, "schema.registry.url");
        String applicationId = required(appProps, "application.id");

        String humidityRaw = required(appProps, "topic.humidity.raw");
        String temperatureRaw = required(appProps, "topic.temperature.raw");
        String locationRaw = required(appProps, "topic.location.raw");
        String stateRaw = required(appProps, "topic.state.raw");

        String humidityOut = required(appProps, "topic.humidity.out");
        String temperatureOut = required(appProps, "topic.temperature.out");
        String locationOut = required(appProps, "topic.location.out");
        String stateOut = required(appProps, "topic.state.out");
        String parseErrors = required(appProps, "topic.parse.errors");

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        streamsProps.put("schema.registry.url", schemaRegistryUrl);
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        streamsProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, applicationId + "-client");

        Serde<String> stringSerde = Serdes.String();
        Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        GenericAvroSerde genericAvroSerde = avroSerde(schemaRegistryUrl);

        StreamsBuilder builder = new StreamsBuilder();

        processHumidity(builder, humidityRaw, humidityOut, parseErrors, stringSerde, byteArraySerde, genericAvroSerde);
        processTemperature(builder, temperatureRaw, temperatureOut, parseErrors, stringSerde, byteArraySerde, genericAvroSerde);
        processLocation(builder, locationRaw, locationOut, parseErrors, stringSerde, byteArraySerde, genericAvroSerde);
        processState(builder, stateRaw, stateOut, parseErrors, stringSerde, byteArraySerde, genericAvroSerde);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10))));
        streams.start();
    }

    private static void processHumidity(
            StreamsBuilder builder,
            String inputTopic,
            String outputTopic,
            String errorTopic,
            Serde<String> keySerde,
            Serde<byte[]> rawSerde,
            GenericAvroSerde avroSerde
    ) {
        KStream<String, Parsers.Result> parsed = builder
                .stream(inputTopic, Consumed.with(keySerde, rawSerde))
                .mapValues(value -> Parsers.parseHumidity(value, inputTopic));

        var branches = parsed.split(Named.as("humidity-"))
                .branch((k, v) -> v.isSuccess(), Branched.as("ok"))
                .defaultBranch(Branched.as("err"));

        branches.get("humidity-ok")
                .selectKey((k, v) -> v.value().get("deviceId").toString())
                .mapValues(Parsers.Result::value)
                .to(outputTopic, Produced.with(keySerde, avroSerde));

        branches.get("humidity-err")
                .map((k, v) -> KeyValue.pair("humidity-parse-error", v.error()))
                .to(errorTopic, Produced.with(keySerde, avroSerde));
    }

    private static void processTemperature(
            StreamsBuilder builder,
            String inputTopic,
            String outputTopic,
            String errorTopic,
            Serde<String> keySerde,
            Serde<byte[]> rawSerde,
            GenericAvroSerde avroSerde
    ) {
        KStream<String, Parsers.Result> parsed = builder
                .stream(inputTopic, Consumed.with(keySerde, rawSerde))
                .mapValues(value -> Parsers.parseTemperature(value, inputTopic));

        var branches = parsed.split(Named.as("temperature-"))
                .branch((k, v) -> v.isSuccess(), Branched.as("ok"))
                .defaultBranch(Branched.as("err"));

        branches.get("temperature-ok")
                .selectKey((k, v) -> v.value().get("deviceId").toString())
                .mapValues(Parsers.Result::value)
                .to(outputTopic, Produced.with(keySerde, avroSerde));

        branches.get("temperature-err")
                .map((k, v) -> KeyValue.pair("temperature-parse-error", v.error()))
                .to(errorTopic, Produced.with(keySerde, avroSerde));
    }

    private static void processLocation(
            StreamsBuilder builder,
            String inputTopic,
            String outputTopic,
            String errorTopic,
            Serde<String> keySerde,
            Serde<byte[]> rawSerde,
            GenericAvroSerde avroSerde
    ) {
        KStream<String, Parsers.Result> parsed = builder
                .stream(inputTopic, Consumed.with(keySerde, rawSerde))
                .mapValues(value -> Parsers.parseLocation(value, inputTopic));

        var branches = parsed.split(Named.as("location-"))
                .branch((k, v) -> v.isSuccess(), Branched.as("ok"))
                .defaultBranch(Branched.as("err"));

        branches.get("location-ok")
                .selectKey((k, v) -> v.value().get("deviceId").toString())
                .mapValues(Parsers.Result::value)
                .to(outputTopic, Produced.with(keySerde, avroSerde));

        branches.get("location-err")
                .map((k, v) -> KeyValue.pair("location-parse-error", v.error()))
                .to(errorTopic, Produced.with(keySerde, avroSerde));
    }

    private static void processState(
            StreamsBuilder builder,
            String inputTopic,
            String outputTopic,
            String errorTopic,
            Serde<String> keySerde,
            Serde<byte[]> rawSerde,
            GenericAvroSerde avroSerde
    ) {
        KStream<String, Parsers.Result> parsed = builder
                .stream(inputTopic, Consumed.with(keySerde, rawSerde))
                .mapValues(value -> Parsers.parseState(value, inputTopic));

        var branches = parsed.split(Named.as("state-"))
                .branch((k, v) -> v.isSuccess(), Branched.as("ok"))
                .defaultBranch(Branched.as("err"));

        branches.get("state-ok")
                .selectKey((k, v) -> v.value().get("deviceId").toString())
                .mapValues(Parsers.Result::value)
                .to(outputTopic, Produced.with(keySerde, avroSerde));

        branches.get("state-err")
                .map((k, v) -> KeyValue.pair("state-parse-error", v.error()))
                .to(errorTopic, Produced.with(keySerde, avroSerde));
    }

    private static GenericAvroSerde avroSerde(String schemaRegistryUrl) {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        serde.configure(config, false);
        return serde;
    }

    private static Properties loadApplicationProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = SensorNormalizerApp.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        }
        return props;
    }

    private static String required(Properties props, String key) {
        String envKey = key.toUpperCase().replace('.', '_');
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }
        return value;
    }
}