package com.kafkaoms.common.model;

import java.time.Instant;

/**
 * Represents a message that could not be processed by a service.
 *
 * Instead of dropping a bad message silently, any service that hits an
 * unrecoverable error publishes a DeadLetterEvent to the dead-letter topic.
 * This gives you:
 *   - A durable record that something went wrong (nothing is lost)
 *   - Enough context to diagnose the problem (which service, which topic, what error)
 *   - The ability to replay the original message once the bug is fixed
 *
 * HOW TO USE IT:
 *   If you see messages in the dead-letter topic in Kafka UI, check:
 *   1. sourceService — which service failed
 *   2. sourceTopic + partition + offset — find the original message in Kafka UI
 *   3. errorMessage — what went wrong
 *
 * @param eventId       Unique ID for this dead-letter entry
 * @param sourceTopic   The Kafka topic the original message came from
 * @param sourceService The service that failed to process the message
 * @param messageKey    The Kafka record key (e.g. symbol like "AAPL")
 * @param partition     Kafka partition the original message was on
 * @param offset        Kafka offset of the original message — use this to find it in Kafka UI
 * @param errorType     The exception class name (e.g. "NullPointerException")
 * @param errorMessage  The exception detail message
 * @param timestamp     When the failure occurred
 */
public record DeadLetterEvent(
        String eventId,
        String sourceTopic,
        String sourceService,
        String messageKey,
        int partition,
        long offset,
        String errorType,
        String errorMessage,
        Instant timestamp
) {}
