package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a trade fill — the execution of an order at a specific price.
 *
 * Published to: fills
 * Consumed by:  portfolio-service, monitor-service
 *
 * In real markets, one order can produce multiple fills (partial fills).
 * For Phase 1, each order produces exactly one fill.
 *
 * The fill_price includes simulated slippage:
 *   - BUY: market_price + slippage
 *   - SELL: market_price - slippage
 *
 * The fee represents a fixed transaction cost (e.g., $1.00 per trade).
 */
public record Fill(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("fill_id") String fillId,
        @JsonProperty("order_id") String orderId,
        String symbol,
        Side side,
        @JsonProperty("filled_quantity") int filledQuantity,
        @JsonProperty("fill_price") double fillPrice,
        double fee,
        Instant timestamp
) {}
