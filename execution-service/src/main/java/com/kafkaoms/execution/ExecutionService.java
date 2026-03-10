package com.kafkaoms.execution;

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
 * EXECUTION SERVICE
 * =================
 * Simulates the actual execution of approved orders against market prices.
 *
 * Consumes: orders-approved (to know which orders to execute)
 *           market-data (to get current prices for execution)
 * Produces: fills (the result of execution)
 *
 * HOW EXECUTION WORKS (Phase 1):
 *   1. An approved order arrives
 *   2. We look up the latest market price for that symbol
 *   3. We apply slippage: BUY fills slightly above market, SELL slightly below
 *   4. We add a fixed commission fee
 *   5. We publish a Fill event
 *
 * SLIPPAGE EXPLAINED:
 * In real markets, the price you actually get is slightly worse than the
 * price you saw when you submitted the order. This is because:
 *   - Other traders are also buying/selling
 *   - Large orders move the price
 *   - There's a delay between decision and execution
 * We simulate this with a fixed 0.01% slippage.
 *
 * KEY KAFKA CONCEPT — MULTIPLE TOPIC CONSUMPTION:
 * This service subscribes to TWO topics. It reads market data to stay
 * updated on prices, and reads approved orders to execute them.
 * Both arrive through the same poll loop.
 */
public class ExecutionService {

    private static final Logger log = LoggerFactory.getLogger(ExecutionService.class);

    private static final double SLIPPAGE_BPS = 0.0001;   // 0.01% slippage (1 basis point)
    private static final double FIXED_FEE = 1.00;         // $1.00 per trade

    // Cache of latest prices from market data
    private static final Map<String, Double> latestPrices = new ConcurrentHashMap<>();

    // Track processed event IDs for idempotency
    private static final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    // Pending orders waiting for price data
    private static final Map<String, OrderValidation> pendingOrders = new ConcurrentHashMap<>();

    // We also need the original order details — in a real system this would come from a database.
    // For now, we also consume orders-submitted to cache order details.
    private static final Map<String, Order> orderCache = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // --- Market data consumer (for prices) ---
        Properties marketConsumerProps = new Properties();
        marketConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        marketConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "execution-service-market");
        marketConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        marketConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        marketConsumerProps.put("value.deserializer.class", MarketData.class.getName());
        marketConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // --- Orders-submitted consumer (to cache order details) ---
        Properties orderConsumerProps = new Properties();
        orderConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        orderConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "execution-service-orders");
        orderConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        orderConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        orderConsumerProps.put("value.deserializer.class", Order.class.getName());
        orderConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --- Approved orders consumer ---
        Properties approvedConsumerProps = new Properties();
        approvedConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        approvedConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "execution-service-approved");
        approvedConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        approvedConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        approvedConsumerProps.put("value.deserializer.class", OrderValidation.class.getName());
        approvedConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --- Fill producer ---
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // Start market data consumer in a background thread
        Thread marketThread = new Thread(() -> {
            try (KafkaConsumer<String, MarketData> consumer = new KafkaConsumer<>(marketConsumerProps)) {
                consumer.subscribe(List.of(Topics.MARKET_DATA));
                while (true) {
                    ConsumerRecords<String, MarketData> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, MarketData> record : records) {
                        MarketData tick = record.value();
                        latestPrices.put(tick.symbol(), tick.price());
                    }
                }
            }
        }, "market-data-reader");
        marketThread.setDaemon(true);
        marketThread.start();

        // Start order cache consumer in a background thread
        Thread orderThread = new Thread(() -> {
            try (KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(orderConsumerProps)) {
                consumer.subscribe(List.of(Topics.ORDERS_SUBMITTED));
                while (true) {
                    ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, Order> record : records) {
                        Order order = record.value();
                        orderCache.put(order.orderId(), order);
                    }
                }
            }
        }, "order-cache-reader");
        orderThread.setDaemon(true);
        orderThread.start();

        // Main loop: consume approved orders and execute them
        try (KafkaConsumer<String, OrderValidation> consumer = new KafkaConsumer<>(approvedConsumerProps);
             KafkaProducer<String, Fill> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(List.of(Topics.ORDERS_APPROVED));
            log.info("Execution Service started. Waiting for approved orders...");

            while (true) {
                ConsumerRecords<String, OrderValidation> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, OrderValidation> record : records) {
                    OrderValidation approval = record.value();

                    // Idempotency check
                    if (processedEventIds.contains(approval.eventId())) {
                        log.warn("Duplicate event detected, skipping: {}", approval.eventId());
                        continue;
                    }
                    processedEventIds.add(approval.eventId());

                    // Look up the original order
                    Order order = orderCache.get(approval.orderId());
                    if (order == null) {
                        log.warn("Order details not found for {}. Waiting...", approval.orderId());
                        // In a production system, we'd retry or fetch from a database
                        continue;
                    }

                    // Execute the order
                    executeFill(order, producer);
                }
            }
        }
    }

    private static void executeFill(Order order, KafkaProducer<String, Fill> producer) {
        Double marketPrice = latestPrices.get(order.symbol());
        if (marketPrice == null) {
            log.warn("No market price available for {}. Cannot execute {}.", order.symbol(), order.orderId());
            return;
        }

        // Apply slippage: BUY gets a slightly worse (higher) price,
        // SELL gets a slightly worse (lower) price
        double slippage = marketPrice * SLIPPAGE_BPS;
        double fillPrice = order.side() == Side.BUY
                ? marketPrice + slippage
                : marketPrice - slippage;
        fillPrice = Math.round(fillPrice * 100.0) / 100.0;

        Fill fill = new Fill(
                IdGenerator.newEventId(),
                IdGenerator.newFillId(),
                order.orderId(),
                order.symbol(),
                order.side(),
                order.quantity(),        // Full fill in Phase 1 (no partial fills)
                fillPrice,
                FIXED_FEE,
                Instant.now()
        );

        producer.send(
                new ProducerRecord<>(Topics.FILLS, order.symbol(), fill),
                (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish fill: {}", exception.getMessage());
                    } else {
                        log.info("💰 FILL: {} {} {} x{} @ ${} (fee: ${}) → partition {}, offset {}",
                                fill.fillId(), order.side(), order.symbol(), fill.filledQuantity(),
                                fill.fillPrice(), fill.fee(), metadata.partition(), metadata.offset());
                    }
                }
        );
    }
}
