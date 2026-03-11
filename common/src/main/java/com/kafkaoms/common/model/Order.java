package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a submitted order (in Java Record) — the first event in the pipeline.
 *
 * Published to: orders-submitted
 * Consumed by:  risk-service
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
