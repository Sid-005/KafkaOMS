package com.kafkaoms.strategy;

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
 * STRATEGY SERVICE
 * ================
 * This service is BOTH a consumer AND a producer:
 *   - Consumes: market-data (to see current prices)
 *   - Produces: orders-submitted (when the strategy decides to trade)
 *
 * WHAT IT DOES:
 * Implements a dead-simple "random trading" strategy for Phase 1:
 *   1. Reads market data prices
 *   2. Every N ticks, randomly decides to BUY or SELL a random symbol
 *   3. Publishes the order to orders-submitted
 *
 * In a real system, this would contain actual trading logic (moving averages,
 * momentum signals, etc.). For learning Kafka, the strategy logic doesn't
 * matter — what matters is the event flow.
 *
 * KEY KAFKA CONCEPT — CONSUMER GROUP:
 * The "group.id" config assigns this consumer to a group. Kafka uses groups to:
 *   - Track which messages this consumer has already read (offsets)
 *   - Distribute partitions across multiple consumers in the same group
 *
 * If you run 2 instances of this service with the same group.id, Kafka splits
 * the partitions between them — that's horizontal scaling!
 *
 * KEY KAFKA CONCEPT — POLL LOOP:
 * Consumers don't get messages pushed to them. Instead, they POLL (ask) Kafka
 * repeatedly: "Any new messages?" This gives the consumer control over its
 * own pace of processing.
 */
public class StrategyService {

    private static final Logger log = LoggerFactory.getLogger(StrategyService.class);
    private static final Random random = new Random();

    // Cache of latest prices from market data
    private static final Map<String, Double> latestPrices = new ConcurrentHashMap<>();

    // How many market data ticks before we submit an order
    private static final int TICKS_BETWEEN_ORDERS = 5;
    private static int tickCount = 0;

    public static void main(String[] args) {
        // --- Configure Kafka Consumer (reads market data) ---
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "strategy-service");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProps.put("value.deserializer.class", MarketData.class.getName());
        // Start reading from the latest messages (don't replay old prices)
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // --- Configure Kafka Producer (submits orders) ---
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaConsumer<String, MarketData> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, Order> producer = new KafkaProducer<>(producerProps)) {

            // Subscribe to the market-data topic
            consumer.subscribe(List.of(Topics.MARKET_DATA));
            log.info("Strategy Service started. Listening for market data...");

            // The poll loop — the heart of every Kafka consumer
            while (true) {
                // Ask Kafka: "Any new messages in the last 1 second?"
                ConsumerRecords<String, MarketData> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, MarketData> record : records) {
                    MarketData tick = record.value();
                    latestPrices.put(tick.symbol(), tick.price());
                    tickCount++;

                    log.debug("Received: {} @ ${}", tick.symbol(), tick.price());
                }

                // Every N ticks, submit a random order
                if (tickCount >= TICKS_BETWEEN_ORDERS && !latestPrices.isEmpty()) {
                    tickCount = 0;
                    submitRandomOrder(producer);
                }
            }
        }
    }

    private static void submitRandomOrder(KafkaProducer<String, Order> producer) {
        // Pick a random symbol from the ones we've seen prices for
        List<String> symbols = new ArrayList<>(latestPrices.keySet());
        String symbol = symbols.get(random.nextInt(symbols.size()));

        // Random side: 70% BUY, 30% SELL (biased toward buying for Phase 1)
        Side side = random.nextDouble() < 0.7 ? Side.BUY : Side.SELL;

        // Random quantity: 1-20 shares
        int quantity = random.nextInt(20) + 1;

        Order order = new Order(
                IdGenerator.newEventId(),
                IdGenerator.newOrderId(),
                "random_strategy_v1",
                symbol,
                side,
                quantity,
                OrderType.MARKET,
                Instant.now()
        );

        ProducerRecord<String, Order> record =
                new ProducerRecord<>(Topics.ORDERS_SUBMITTED, symbol, order);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to submit order: {}", exception.getMessage());
            } else {
                log.info("📋 Order submitted: {} {} {} x{} → partition {}, offset {}",
                        order.orderId(), side, symbol, quantity,
                        metadata.partition(), metadata.offset());
            }
        });
    }
}
