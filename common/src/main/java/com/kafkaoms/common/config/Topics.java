package com.kafkaoms.common.config;

/**
 * Central registry of all Kafka topic names.
 */
public final class Topics {

    private Topics() {} // Prevent instantiation

    public static final String MARKET_DATA       = "market-data";
    public static final String ORDERS_SUBMITTED  = "orders-submitted";
    public static final String ORDERS_APPROVED   = "orders-approved";
    public static final String ORDERS_REJECTED   = "orders-rejected";
    public static final String FILLS             = "fills";
    public static final String PORTFOLIO_UPDATED = "portfolio-updated";
    public static final String RISK_ALERTS       = "risk-alerts";
}
