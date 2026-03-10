package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a market data tick — a price update for a stock.
 *
 * Published to: market-data
 * Consumed by:  strategy-service, execution-service, portfolio-service
 *
 * In real markets, prices change thousands of times per second.
 * Our simulator publishes a new price every few seconds for each symbol,
 * with small random price movements to simulate volatility.
 */
public record MarketData(
        String symbol,
        double price,
        double volume,        // simulated trading volume
        Instant timestamp
) {}
