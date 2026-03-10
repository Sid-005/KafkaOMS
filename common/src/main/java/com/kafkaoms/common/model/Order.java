package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a submitted order — the first event in our pipeline.
 *
 * Published to: orders-submitted
 * Consumed by:  risk-service
 *
 * This is a Java "record" (introduced in Java 16). Records are immutable
 * data classes — perfect for events. The compiler auto-generates:
 *   - constructor
 *   - getters (eventId(), orderId(), etc.)
 *   - equals(), hashCode(), toString()
 *
 * Example JSON:
 * {
 *   "event_id": "evt_001",
 *   "order_id": "ord_1001",
 *   "strategy_id": "sma_cross_v1",
 *   "symbol": "AAPL",
 *   "side": "BUY",
 *   "quantity": 10,
 *   "order_type": "MARKET",
 *   "timestamp": "2026-03-10T10:00:00Z"
 * }
 */
public record Order(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("strategy_id") String strategyId,
        String symbol,
        Side side,
        int quantity,
        @JsonProperty("order_type") OrderType orderType,
        Instant timestamp
) {}
