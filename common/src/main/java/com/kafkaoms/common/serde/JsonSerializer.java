package com.kafkaoms.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Kafka Serializer that converts Java objects → JSON bytes.
 *
 * HOW KAFKA SERIALIZATION WORKS (for my future self):
 * Kafka stores messages as raw bytes. When you send a Java object (like an Order),
 * Kafka needs to know HOW to convert it to bytes. That's what a Serializer does.
 *
 * Flow: Order object → JsonSerializer → JSON string → bytes → Kafka topic
 *
 * You configure this in the producer:
 *   props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
 *
 * @param <T> The type of object to serialize (Order, Fill, etc.)
 */
public class JsonSerializer<T> implements Serializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);
    private final ObjectMapper mapper = JsonMapper.instance();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            log.error("Failed to serialize object for topic {}: {}", topic, e.getMessage());
            throw new RuntimeException("Serialization failed", e);
        }
    }
}
