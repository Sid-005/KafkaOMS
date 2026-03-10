package com.kafkaoms.common.config;

/**
 * Central registry of all Kafka topic names.
 *
 * WHY a constants class?
 * If you hardcode "orders-submitted" in 5 different services and then
 * rename the topic, you'd have to find and fix all 5. With constants,
 * you change it in ONE place.
 *
 * These topic names match what we create in docker-compose.yml → kafka-init.
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
