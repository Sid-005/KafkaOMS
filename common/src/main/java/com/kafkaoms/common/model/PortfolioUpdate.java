package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a portfolio state update after a fill is processed.
 *
 * Published to: portfolio-updated
 * Consumed by:  monitor-service
 *
 * Key financial concepts:
 *   position     = how many shares you currently hold of this symbol
 *   avg_cost     = weighted average price you paid per share
 *   cash         = remaining cash after buys/sells
 *   realized_pnl = profit/loss from completed (closed) trades
 *   unrealized_pnl = paper profit/loss on open positions (based on current market price)
 */
public record PortfolioUpdate(
        @JsonProperty("event_id") String eventId,
        String symbol,
        int position,
        @JsonProperty("avg_cost") double avgCost,
        double cash,
        @JsonProperty("realized_pnl") double realizedPnl,
        @JsonProperty("unrealized_pnl") double unrealizedPnl,
        Instant timestamp
) {}
