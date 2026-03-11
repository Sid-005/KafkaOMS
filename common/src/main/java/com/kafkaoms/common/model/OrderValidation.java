package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Risk validation result for an order.
 *
 * Published to: orders-approved OR orders-rejected
 * Consumed by:  execution-service (approved) / monitor-service (rejected)
 *
 * If status = REJECTED, reason = why (e.g., "Exceeds max order size of 100").
 */
public record OrderValidation(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("order_id") String orderId,
        OrderStatus status,
        String reason,          // null if approved, explanation if rejected
        Instant timestamp
) {}
