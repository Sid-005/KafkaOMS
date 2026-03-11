package com.kafkaoms.common.model;

/**
 * Order Types
 *
 * MARKET = execute immediately at the current market price.
 *          Used Phase 1.
 *
 * LIMIT  = only execute if the price is at or better than a specified price.
 *          Added in Phase 3.
 */
public enum OrderType {
    MARKET,
    LIMIT
}
