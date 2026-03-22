package com.kafkaoms.portfolio;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.model.*;
import com.kafkaoms.common.serde.JsonDeserializer;
import com.kafkaoms.common.serde.JsonSerializer;
import com.kafkaoms.common.metrics.MetricsRegistry;
import com.kafkaoms.common.model.DeadLetterEvent;
import com.kafkaoms.common.util.DlqPublisher;
import com.kafkaoms.common.util.IdGenerator;
import io.micrometer.core.instrument.Gauge;
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
 * PORTFOLIO SERVICE
 * =================
 * The "bookkeeper" of the system. Processes fills and maintains the
 * current state of our portfolio: positions, cash, and PnL.
 *
 * Consumes: fills
 * Produces: portfolio-updated
 *
 * PORTFOLIO MATH EXPLAINED:
 *
 * When we BUY 10 shares of AAPL at $187.42:
 *   position:  0 → 10
 *   avg_cost:  $187.42 (we paid $187.42 per share on average)
 *   cash:      -$1,875.20 (10 × $187.42 + $1.00 fee)
 *
 * When we BUY 5 more at $190.00:
 *   position:  10 → 15
 *   avg_cost:  (10 × $187.42 + 5 × $190.00) / 15 = $188.28
 *   cash:      previous - (5 × $190.00 + $1.00 fee)
 *
 * When we SELL 8 at $195.00:
 *   position:  15 → 7
 *   avg_cost:  still $188.28 (selling doesn't change avg cost of remaining shares)
 *   realized_pnl:  8 × ($195.00 - $188.28) - $1.00 fee = $52.76
 *   cash:      previous + (8 × $195.00 - $1.00 fee)
 *
 * Unrealized PnL = (current_price - avg_cost) × position
 *   If AAPL is now at $200: (200 - 188.28) × 7 = $82.04 paper profit
 *
 * KEY KAFKA CONCEPT — STATE FROM EVENTS:
 * We don't store portfolio state externally (yet). We BUILD it entirely
 * from the stream of fill events. This means if we reset this consumer's
 * offset to the beginning, it replays all fills and reconstructs the
 * exact same portfolio state. This is "event sourcing" in action.
 */
public class PortfolioService {

    private static final Logger log = LoggerFactory.getLogger(PortfolioService.class);

    // Starting cash balance
    private static double cash = 100_000.00;

    // Per-symbol state
    private static final Map<String, Integer> positions = new ConcurrentHashMap<>();    // symbol → shares held
    private static final Map<String, Double> avgCosts = new ConcurrentHashMap<>();      // symbol → avg cost per share
    private static final Map<String, Double> realizedPnl = new ConcurrentHashMap<>();   // symbol → realized P&L

    // Latest market prices (for unrealized PnL calculation)
    private static final Map<String, Double> latestPrices = new ConcurrentHashMap<>();

    // Idempotency: track processed fill event IDs
    private static final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        MetricsRegistry.init("portfolio-service", 8083);
        Gauge.builder("portfolio_cash_balance", () -> cash)
                .description("Current cash balance in the portfolio")
                .register(MetricsRegistry.get());
        Gauge.builder("portfolio_realized_pnl", () ->
                        realizedPnl.values().stream().mapToDouble(Double::doubleValue).sum())
                .description("Total realized PnL across all symbols")
                .register(MetricsRegistry.get());

        // Market data consumer (background thread)
        Properties marketProps = new Properties();
        marketProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        marketProps.put(ConsumerConfig.GROUP_ID_CONFIG, "portfolio-service-market");
        marketProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        marketProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        marketProps.put("value.deserializer.class", MarketData.class.getName());
        marketProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Fill consumer
        Properties fillProps = new Properties();
        fillProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fillProps.put(ConsumerConfig.GROUP_ID_CONFIG, "portfolio-service");
        fillProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        fillProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        fillProps.put("value.deserializer.class", Fill.class.getName());
        fillProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Portfolio update producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        // Background thread for market data
        Thread marketThread = new Thread(() -> {
            try (KafkaConsumer<String, MarketData> consumer = new KafkaConsumer<>(marketProps)) {
                consumer.subscribe(List.of(Topics.MARKET_DATA));
                while (true) {
                    ConsumerRecords<String, MarketData> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, MarketData> record : records) {
                        latestPrices.put(record.value().symbol(), record.value().price());
                    }
                }
            }
        }, "market-data-reader");
        marketThread.setDaemon(true);
        marketThread.start();

        // Main loop: process fills
        try (KafkaConsumer<String, Fill> consumer = new KafkaConsumer<>(fillProps);
             KafkaProducer<String, PortfolioUpdate> producer = new KafkaProducer<>(producerProps);
             KafkaProducer<String, DeadLetterEvent> dlqProducer = DlqPublisher.buildProducer()) {

            consumer.subscribe(List.of(Topics.FILLS));
            log.info("Portfolio Service started. Cash: ${}. Waiting for fills...", String.format("%.2f", cash));

            while (true) {
                ConsumerRecords<String, Fill> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, Fill> record : records) {
                    if (record.value() == null) {
                        DlqPublisher.publish(dlqProducer, record, "PortfolioService",
                                new RuntimeException("Deserialization failed — message could not be parsed as Fill"));
                        continue;
                    }

                    try {
                        Fill fill = record.value();

                        if (processedEventIds.contains(fill.eventId())) {
                            log.warn("Duplicate fill event detected, skipping: {}", fill.eventId());
                            continue;
                        }
                        processedEventIds.add(fill.eventId());

                        processFill(fill, producer);
                    } catch (Exception e) {
                        DlqPublisher.publish(dlqProducer, record, "PortfolioService", e);
                    }
                }
            }
        }
    }

    private static void processFill(Fill fill, KafkaProducer<String, PortfolioUpdate> producer) {
        String symbol = fill.symbol();
        int currentPos = positions.getOrDefault(symbol, 0);
        double currentAvgCost = avgCosts.getOrDefault(symbol, 0.0);
        double currentRealizedPnl = realizedPnl.getOrDefault(symbol, 0.0);

        double totalCost = fill.filledQuantity() * fill.fillPrice();

        if (fill.side() == Side.BUY) {
            // --- BUY: increase position, recalculate average cost ---
            // New avg cost = (old_shares × old_avg + new_shares × new_price) / total_shares
            double oldValue = currentPos * currentAvgCost;
            int newPos = currentPos + fill.filledQuantity();
            double newAvgCost = newPos > 0 ? (oldValue + totalCost) / newPos : 0.0;

            positions.put(symbol, newPos);
            avgCosts.put(symbol, Math.round(newAvgCost * 100.0) / 100.0);
            cash -= (totalCost + fill.fee());

        } else {
            // --- SELL: decrease position, realize profit/loss ---
            // Realized PnL = (sell_price - avg_cost) × quantity - fee
            double pnl = (fill.fillPrice() - currentAvgCost) * fill.filledQuantity() - fill.fee();
            int newPos = currentPos - fill.filledQuantity();

            positions.put(symbol, newPos);
            realizedPnl.put(symbol, currentRealizedPnl + pnl);
            cash += (totalCost - fill.fee());

            // If position is fully closed, reset avg cost
            if (newPos == 0) {
                avgCosts.put(symbol, 0.0);
            }
            // If still holding shares, avg cost stays the same
        }

        // Calculate unrealized PnL
        int pos = positions.getOrDefault(symbol, 0);
        double avgC = avgCosts.getOrDefault(symbol, 0.0);
        Double marketPrice = latestPrices.get(symbol);
        double unrealizedPnl = (marketPrice != null && pos > 0)
                ? (marketPrice - avgC) * pos
                : 0.0;

        // Publish portfolio update
        PortfolioUpdate update = new PortfolioUpdate(
                IdGenerator.newEventId(),
                symbol,
                pos,
                avgCosts.getOrDefault(symbol, 0.0),
                Math.round(cash * 100.0) / 100.0,
                Math.round(realizedPnl.getOrDefault(symbol, 0.0) * 100.0) / 100.0,
                Math.round(unrealizedPnl * 100.0) / 100.0,
                Instant.now()
        );

        producer.send(
                new ProducerRecord<>(Topics.PORTFOLIO_UPDATED, symbol, update),
                (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish portfolio update: {}", exception.getMessage());
                    } else {
                        log.info("📊 PORTFOLIO: {} pos={} avg=${} cash=${} realized=${} unrealized=${}",
                                symbol, update.position(), update.avgCost(),
                                String.format("%.2f", update.cash()),
                                String.format("%.2f", update.realizedPnl()),
                                String.format("%.2f", update.unrealizedPnl()));
                    }
                }
        );
    }
}
