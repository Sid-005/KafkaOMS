package com.kafkaoms.common.model;

/**
 * The lifecycle status of an order as it flows through the system.
 *
 * State transitions:
 *   SUBMITTED → APPROVED → FILLED
 *   SUBMITTED → REJECTED (terminal state)
 *
 * Each transition is an event published to a Kafka topic.
 */
public enum OrderStatus {
    SUBMITTED,
    APPROVED,
    REJECTED,
    FILLED
}
