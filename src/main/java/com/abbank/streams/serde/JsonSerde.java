package com.abbank.streams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic JSON Serde for Kafka Streams state stores and value types.
 *
 * <p>Usage: {@code new JsonSerde<>(MyClass.class)}
 */
public class JsonSerde<T> implements Serde<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSerde.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private final Class<T> targetType;

    public JsonSerde(final Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                LOG.error("Serialisation failed for type {} on topic {}", targetType.getSimpleName(), topic, e);
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return null;
            try {
                return MAPPER.readValue(bytes, targetType);
            } catch (Exception e) {
                LOG.warn("Deserialisation failed for type {} on topic {} — skipping record",
                        targetType.getSimpleName(), topic, e);
                return null;
            }
        };
    }
}
