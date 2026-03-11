package com.kafkaoms.marketdata;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.model.MarketData;
import com.kafkaoms.common.serde.JsonSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * MARKET DATA PRODUCER
 * ====================
 * This is the FIRST service in our pipeline. It simulates a stock market
 * by publishing random price updates to the "market-data" Kafka topic.
 *
 * In a real system, this would connect to a market data feed (like Bloomberg
 * or a broker API). For learning purposes, I'm just gonna simulate it with random walks.
 *
 * HOW IT WORKS:
 * 1. Start with base prices for a few symbols (AAPL, GOOGL, MSFT)
 * 2. Every second, randomly adjust each price by a small amount (±0.5%)
 * 3. Publish a MarketData event to the "market-data" topic
 *
 * KEY KAFKA CONCEPT — PRODUCER:
 * A producer is any application that WRITES messages to Kafka.
 * We configure it with:
 *   - bootstrap.servers: where to find Kafka (localhost:9092)
 *   - key.serializer: how to convert the message key to bytes (String → bytes)
 *   - value.serializer: how to convert the message value to bytes (MarketData → JSON → bytes)
 *
 * KEY KAFKA CONCEPT — MESSAGE KEY:
 * We use the stock symbol as the Kafka key. This means ALL price updates
 * for "AAPL" go to the SAME partition, guaranteeing they arrive in order.
 * This is called "key-based partitioning."
 */
public class MarketDataProducer {

    private static final Logger log = LoggerFactory.getLogger(MarketDataProducer.class);
    private static final Random random = new Random();

    // Symbols and their starting prices
    private static final Map<String, Double> PRICES = new LinkedHashMap<>();
    static {
        PRICES.put("AAPL", 187.00);
        PRICES.put("GOOGL", 141.00);
        PRICES.put("MSFT", 410.00);
    }

    public static void main(String[] args) throws InterruptedException {
        // --- Configure the Kafka producer ---
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        // Acks = "all" means Kafka confirms the message is stored before we continue.
        // This is the safest setting — no silent data loss.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, MarketData> producer = new KafkaProducer<>(props)) {
            log.info("Market Data Producer started. Publishing prices for: {}", PRICES.keySet());

            // Run forever (until you press Ctrl+C)
            while (true) {
                for (Map.Entry<String, Double> entry : PRICES.entrySet()) {
                    String symbol = entry.getKey();
                    double currentPrice = entry.getValue();

                    // Simulate price movement: random walk ±0.5%
                    double change = currentPrice * (random.nextGaussian() * 0.005);
                    double newPrice = Math.round((currentPrice + change) * 100.0) / 100.0;
                    PRICES.put(symbol, newPrice);

                    MarketData tick = new MarketData(
                            symbol,
                            newPrice,
                            random.nextInt(10000) + 1000,   // random volume
                            Instant.now()
                    );

                    // Send to Kafka. The key is the symbol, so all AAPL events
                    // go to the same partition (ordering guarantee).
                    ProducerRecord<String, MarketData> record =
                            new ProducerRecord<>(Topics.MARKET_DATA, symbol, tick);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Failed to send market data: {}", exception.getMessage());
                        } else {
                            log.info("📈 {} @ ${} → partition {}, offset {}",
                                    symbol, newPrice, metadata.partition(), metadata.offset());
                        }
                    });
                }

                // Publish prices every 2 seconds
                Thread.sleep(2000);
            }
        }
    }
}
