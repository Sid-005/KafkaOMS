package com.kafkaoms.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Custom Kafka Deserializer that converts JSON bytes → Java objects.
 *
 * The reverse of JsonSerializer:
 * Flow: Kafka topic → bytes → JSON string → JsonDeserializer → Order object
 *
 * IMPORTANT: The consumer needs to know WHICH class to deserialize into.
 * You configure this with:
 *   props.put("value.deserializer.class", Order.class.getName());
 *
 * Or by passing the target class in the constructor.
 *
 * @param <T> The type of object to deserialize into
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonDeserializer.class);
    private final ObjectMapper mapper = JsonMapper.instance();
    private Class<T> targetType;

    /** No-arg constructor — required by Kafka. Type is set via configure(). */
    public JsonDeserializer() {}

    /** Convenience constructor for when you know the type at creation time. */
    public JsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetType == null) {
            String className = (String) configs.get("value.deserializer.class");
            if (className != null) {
                try {
                    targetType = (Class<T>) Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Cannot find deserializer target class: " + className, e);
                }
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return mapper.readValue(data, targetType);
        } catch (Exception e) {
            log.error("Failed to deserialize message from topic {}: {}", topic, e.getMessage());
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
