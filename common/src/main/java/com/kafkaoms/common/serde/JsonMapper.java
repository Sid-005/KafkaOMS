package com.kafkaoms.common.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared ObjectMapper configuration.
 *
 * ObjectMapper is Jackson's main class for converting Java objects ↔ JSON.
 *
 * We configure it once here so every service serializes/deserializes
 * consistently. Key settings:
 *   - JavaTimeModule: teaches Jackson how to handle java.time.Instant
 *   - WRITE_DATES_AS_TIMESTAMPS=false: outputs "2026-03-10T10:00:00Z"
 *     instead of a raw number like 1741600000
 */
public final class JsonMapper {

    private static final ObjectMapper INSTANCE;

    static {
        INSTANCE = new ObjectMapper();
        INSTANCE.registerModule(new JavaTimeModule());
        INSTANCE.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private JsonMapper() {}

    public static ObjectMapper instance() {
        return INSTANCE;
    }
}
