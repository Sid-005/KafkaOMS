package com.kafkaoms.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Represents a market data tick — a price update for a stock.
 *
 * Published to: market-data
 * Consumed by:  strategy-service, execution-service, portfolio-service
 *
 * Simulates real-time market data with random price movements and volume for each symbol.
 * Effectively, simulates volitility.
 */
public record MarketData(
        String symbol,
        double price,
        double volume,        // simulated trading volume
        Instant timestamp
) {}
