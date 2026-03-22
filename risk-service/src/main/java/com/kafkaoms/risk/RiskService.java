package com.kafkaoms.risk;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.model.*;
import com.kafkaoms.common.serde.JsonDeserializer;
import com.kafkaoms.common.serde.JsonSerializer;
import com.kafkaoms.common.metrics.MetricsRegistry;
import com.kafkaoms.common.model.DeadLetterEvent;
import com.kafkaoms.common.util.DlqPublisher;
import com.kafkaoms.common.util.IdGenerator;
import io.micrometer.core.instrument.Counter;
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
 * Consumes: orders-submitted (main loop)
 *           market-data     (background thread, for price-based cash check)
 * Produces: orders-approved OR orders-rejected
 *
 * All validation logic lives in RiskValidator — this class only handles
 * Kafka I/O and delegates decisions to the validator.
 *
 * KEY KAFKA CONCEPT — IDEMPOTENCY:
 * We track processed event IDs in a Set. If Kafka delivers the same message
 * twice (which CAN happen), we detect the duplicate and skip it.
 */
public class RiskService {

    private static final Logger log = LoggerFactory.getLogger(RiskService.class);

    // All validation logic is here — extracted for testability
    private static final RiskValidator validator = new RiskValidator();

    // Track processed event IDs for idempotency
    private static final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        MetricsRegistry.init("risk-service", 8081);
        Counter approvedCounter = Counter.builder("risk_orders_total")
                .description("Total orders processed by the risk service")
                .tag("result", "approved")
                .register(MetricsRegistry.get());
        Counter rejectedCounter = Counter.builder("risk_orders_total")
                .description("Total orders processed by the risk service")
                .tag("result", "rejected")
                .register(MetricsRegistry.get());

        // --- Market data consumer (background thread, for cash check prices) ---
        Properties marketConsumerProps = new Properties();
        marketConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        marketConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "risk-service-market");
        marketConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        marketConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        marketConsumerProps.put("value.deserializer.class", MarketData.class.getName());
        marketConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Thread marketThread = new Thread(() -> {
            try (KafkaConsumer<String, MarketData> consumer = new KafkaConsumer<>(marketConsumerProps)) {
                consumer.subscribe(List.of(Topics.MARKET_DATA));
                while (true) {
                    ConsumerRecords<String, MarketData> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, MarketData> record : records) {
                        MarketData tick = record.value();
                        validator.updatePrice(tick.symbol(), tick.price());
                    }
                }
            }
        }, "risk-market-data-reader");
        marketThread.setDaemon(true);
        marketThread.start();

        // --- Orders consumer config ---
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
             KafkaProducer<String, OrderValidation> producer = new KafkaProducer<>(producerProps);
             KafkaProducer<String, DeadLetterEvent> dlqProducer = DlqPublisher.buildProducer()) {

            consumer.subscribe(List.of(Topics.ORDERS_SUBMITTED));
            log.info("Risk Service started. Validating incoming orders...");

            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, Order> record : records) {
                    if (record.value() == null) {
                        DlqPublisher.publish(dlqProducer, record, "RiskService",
                                new RuntimeException("Deserialization failed — message could not be parsed as Order"));
                        continue;
                    }

                    try {
                        Order order = record.value();

                        if (processedEventIds.contains(order.eventId())) {
                            log.warn("Duplicate event detected, skipping: {}", order.eventId());
                            continue;
                        }
                        processedEventIds.add(order.eventId());

                        String rejectionReason = validator.validate(order);

                        if (rejectionReason == null) {
                            OrderValidation approval = new OrderValidation(
                                    IdGenerator.newEventId(),
                                    order.orderId(),
                                    OrderStatus.APPROVED,
                                    null,
                                    Instant.now()
                            );
                            producer.send(new ProducerRecord<>(Topics.ORDERS_APPROVED, order.symbol(), approval));
                            validator.recordApproval(order);
                            approvedCounter.increment();

                            log.info("✅ APPROVED: {} {} {} x{} | cash remaining: ${}",
                                    order.orderId(), order.side(), order.symbol(), order.quantity(),
                                    String.format("%.2f", validator.getAvailableCash()));
                        } else {
                            OrderValidation rejection = new OrderValidation(
                                    IdGenerator.newEventId(),
                                    order.orderId(),
                                    OrderStatus.REJECTED,
                                    rejectionReason,
                                    Instant.now()
                            );
                            producer.send(new ProducerRecord<>(Topics.ORDERS_REJECTED, order.symbol(), rejection));
                            rejectedCounter.increment();

                            log.info("❌ REJECTED: {} — Reason: {}", order.orderId(), rejectionReason);
                        }
                    } catch (Exception e) {
                        DlqPublisher.publish(dlqProducer, record, "RiskService", e);
                    }
                }
            }
        }
    }
}
