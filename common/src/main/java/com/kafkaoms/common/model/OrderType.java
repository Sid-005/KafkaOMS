package com.kafkaoms.common.model;

/**
 * The type of order.
 *
 * MARKET = execute immediately at the current market price.
 *          This is what we'll use in Phase 1.
 *
 * LIMIT  = only execute if the price is at or better than a specified price.
 *          We'll add this in Phase 3.
 */
public enum OrderType {
    MARKET,
    LIMIT
}
