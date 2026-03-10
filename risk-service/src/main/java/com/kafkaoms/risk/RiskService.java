package com.kafkaoms.risk;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.model.*;
import com.kafkaoms.common.serde.JsonDeserializer;
import com.kafkaoms.common.serde.JsonSerializer;
import com.kafkaoms.common.util.IdGenerator;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RISK SERVICE
 * ============
 * The "gatekeeper" of the pipeline. Every submitted order passes through here
 * before it can be executed.
 *
 * Consumes: orders-submitted
 * Produces: orders-approved OR orders-rejected
 *
 * RISK RULES (Phase 1):
 *   1. Max order size: no single order > 100 shares
 *   2. Max position per symbol: no more than 500 shares of any one stock
 *   3. Long-only: no selling shares you don't own (no short selling)
 *
 * WHY RISK CHECKS MATTER:
 * In real trading, risk checks prevent catastrophic losses. A bug in the
 * strategy could submit an order for 1,000,000 shares — the risk engine
 * catches that before real money is at stake.
 *
 * KEY KAFKA CONCEPT — IDEMPOTENCY:
 * We track processed event IDs in a Set. If Kafka delivers the same message
 * twice (which CAN happen), we detect the duplicate and skip it. This prevents
 * the same order from being approved/rejected twice.
 *
 * In production, you'd store processed IDs in a database, not in memory.
 */
public class RiskService {

    private static final Logger log = LoggerFactory.getLogger(RiskService.class);

    // --- Risk limits ---
    private static final int MAX_ORDER_SIZE = 100;
    private static final int MAX_POSITION_PER_SYMBOL = 500;

    // Track current positions (symbol → quantity held)
    // In production, this would come from a database
    private static final Map<String, Integer> positions = new ConcurrentHashMap<>();

    // Track processed event IDs for idempotency
    private static final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        // Consumer config
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "risk-service");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProps.put("value.deserializer.class", Order.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Producer config
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, OrderValidation> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(List.of(Topics.ORDERS_SUBMITTED));
            log.info("Risk Service started. Validating incoming orders...");

            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, Order> record : records) {
                    Order order = record.value();

                    // --- Idempotency check ---
                    if (processedEventIds.contains(order.eventId())) {
                        log.warn("Duplicate event detected, skipping: {}", order.eventId());
                        continue;
                    }
                    processedEventIds.add(order.eventId());

                    // --- Run risk checks ---
                    String rejectionReason = validateOrder(order);

                    if (rejectionReason == null) {
                        // APPROVED — publish to orders-approved
                        OrderValidation approval = new OrderValidation(
                                IdGenerator.newEventId(),
                                order.orderId(),
                                OrderStatus.APPROVED,
                                null,
                                Instant.now()
                        );

                        producer.send(new ProducerRecord<>(Topics.ORDERS_APPROVED, order.symbol(), approval));

                        // Update our local position tracking
                        int delta = order.side() == Side.BUY ? order.quantity() : -order.quantity();
                        positions.merge(order.symbol(), delta, Integer::sum);

                        log.info("✅ APPROVED: {} {} {} x{}",
                                order.orderId(), order.side(), order.symbol(), order.quantity());
                    } else {
                        // REJECTED — publish to orders-rejected
                        OrderValidation rejection = new OrderValidation(
                                IdGenerator.newEventId(),
                                order.orderId(),
                                OrderStatus.REJECTED,
                                rejectionReason,
                                Instant.now()
                        );

                        producer.send(new ProducerRecord<>(Topics.ORDERS_REJECTED, order.symbol(), rejection));

                        log.info("❌ REJECTED: {} — Reason: {}", order.orderId(), rejectionReason);
                    }
                }
            }
        }
    }

    /**
     * Validates an order against risk rules.
     * @return null if approved, or a rejection reason string
     */
    private static String validateOrder(Order order) {
        // Rule 1: Max order size
        if (order.quantity() > MAX_ORDER_SIZE) {
            return "Order size " + order.quantity() + " exceeds max of " + MAX_ORDER_SIZE;
        }

        int currentPosition = positions.getOrDefault(order.symbol(), 0);

        // Rule 2: No short selling (long-only in Phase 1)
        if (order.side() == Side.SELL && currentPosition < order.quantity()) {
            return "Cannot sell " + order.quantity() + " shares of " + order.symbol()
                    + " — only holding " + currentPosition;
        }

        // Rule 3: Max position per symbol
        if (order.side() == Side.BUY) {
            int newPosition = currentPosition + order.quantity();
            if (newPosition > MAX_POSITION_PER_SYMBOL) {
                return "Position would be " + newPosition + " for " + order.symbol()
                        + " — exceeds max of " + MAX_POSITION_PER_SYMBOL;
            }
        }

        return null; // All checks passed
    }
}
