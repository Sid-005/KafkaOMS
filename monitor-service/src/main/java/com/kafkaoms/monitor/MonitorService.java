package com.kafkaoms.monitor;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.model.*;
import com.kafkaoms.common.serde.JsonDeserializer;
import com.kafkaoms.common.util.DlqPublisher;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MONITOR SERVICE
 * ===============
 * The "dashboard" of the system. A read-only consumer that listens to
 * multiple topics and prints a live summary of the entire trading system.
 *
 * Consumes: portfolio-updated, orders-rejected, fills
 *
 * This service demonstrates an important Kafka pattern:
 * You can have MULTIPLE consumers reading the SAME topics independently.
 * The portfolio-service processes fills to update state, while the
 * monitor-service reads the SAME fills just for display purposes.
 * Neither interferes with the other.
 *
 * KEY KAFKA CONCEPT — INDEPENDENT CONSUMER GROUPS:
 * Because this service has a DIFFERENT group.id than portfolio-service,
 * it gets its OWN copy of every message. Each consumer group tracks
 * its own offsets independently. This is how Kafka supports multiple
 * subscribers without message loss.
 *
 * This service is PURE OUTPUT — it never produces to any topic.
 * It periodically prints a formatted dashboard to the terminal.
 */
public class MonitorService {

    private static final Logger log = LoggerFactory.getLogger(MonitorService.class);

    // Aggregated state for the dashboard
    private static final Map<String, Integer> positions = new ConcurrentHashMap<>();
    private static final Map<String, Double> avgCosts = new ConcurrentHashMap<>();
    private static final Map<String, Double> realizedPnl = new ConcurrentHashMap<>();
    private static final Map<String, Double> unrealizedPnl = new ConcurrentHashMap<>();
    private static double cash = 100_000.00;

    // Counters
    private static int totalFills = 0;
    private static int totalRejections = 0;
    private static final List<String> recentEvents = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_RECENT_EVENTS = 10;

    public static void main(String[] args) {
        // --- Portfolio updates consumer ---
        Properties portfolioProps = new Properties();
        portfolioProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        portfolioProps.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor-service-portfolio");
        portfolioProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        portfolioProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        portfolioProps.put("value.deserializer.class", PortfolioUpdate.class.getName());
        portfolioProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --- Rejected orders consumer ---
        Properties rejectedProps = new Properties();
        rejectedProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        rejectedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor-service-rejected");
        rejectedProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        rejectedProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        rejectedProps.put("value.deserializer.class", OrderValidation.class.getName());
        rejectedProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --- Fills consumer (for event log) ---
        Properties fillProps = new Properties();
        fillProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        fillProps.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor-service-fills");
        fillProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        fillProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        fillProps.put("value.deserializer.class", Fill.class.getName());
        fillProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // DLQ producer — shared across all background threads (effectively final)
        final KafkaProducer<String, DeadLetterEvent> dlqProducer = DlqPublisher.buildProducer();

        // Background: read portfolio updates
        Thread portfolioThread = new Thread(() -> {
            try (KafkaConsumer<String, PortfolioUpdate> consumer = new KafkaConsumer<>(portfolioProps)) {
                consumer.subscribe(List.of(Topics.PORTFOLIO_UPDATED));
                while (true) {
                    ConsumerRecords<String, PortfolioUpdate> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, PortfolioUpdate> record : records) {
                        if (record.value() == null) {
                            DlqPublisher.publish(dlqProducer, record, "MonitorService",
                                    new RuntimeException("Deserialization failed — message could not be parsed as PortfolioUpdate"));
                            continue;
                        }
                        PortfolioUpdate update = record.value();
                        positions.put(update.symbol(), update.position());
                        avgCosts.put(update.symbol(), update.avgCost());
                        realizedPnl.put(update.symbol(), update.realizedPnl());
                        unrealizedPnl.put(update.symbol(), update.unrealizedPnl());
                        cash = update.cash();
                    }
                }
            }
        }, "portfolio-reader");
        portfolioThread.setDaemon(true);
        portfolioThread.start();

        // Background: read rejected orders
        Thread rejectedThread = new Thread(() -> {
            try (KafkaConsumer<String, OrderValidation> consumer = new KafkaConsumer<>(rejectedProps)) {
                consumer.subscribe(List.of(Topics.ORDERS_REJECTED));
                while (true) {
                    ConsumerRecords<String, OrderValidation> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, OrderValidation> record : records) {
                        if (record.value() == null) {
                            DlqPublisher.publish(dlqProducer, record, "MonitorService",
                                    new RuntimeException("Deserialization failed — message could not be parsed as OrderValidation"));
                            continue;
                        }
                        OrderValidation rejection = record.value();
                        totalRejections++;
                        addRecentEvent("❌ REJECTED " + rejection.orderId() + ": " + rejection.reason());
                    }
                }
            }
        }, "rejected-reader");
        rejectedThread.setDaemon(true);
        rejectedThread.start();

        // Background: read fills
        Thread fillThread = new Thread(() -> {
            try (KafkaConsumer<String, Fill> consumer = new KafkaConsumer<>(fillProps)) {
                consumer.subscribe(List.of(Topics.FILLS));
                while (true) {
                    ConsumerRecords<String, Fill> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, Fill> record : records) {
                        if (record.value() == null) {
                            DlqPublisher.publish(dlqProducer, record, "MonitorService",
                                    new RuntimeException("Deserialization failed — message could not be parsed as Fill"));
                            continue;
                        }
                        Fill fill = record.value();
                        totalFills++;
                        addRecentEvent(String.format("💰 FILL %s %s %s x%d @ $%.2f",
                                fill.fillId(), fill.side(), fill.symbol(),
                                fill.filledQuantity(), fill.fillPrice()));
                    }
                }
            }
        }, "fill-reader");
        fillThread.setDaemon(true);
        fillThread.start();

        // Main loop: print dashboard every 5 seconds
        log.info("Monitor Service started. Dashboard refreshes every 5 seconds...");
        while (true) {
            try {
                Thread.sleep(5000);
                printDashboard();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void addRecentEvent(String event) {
        recentEvents.add(event);
        if (recentEvents.size() > MAX_RECENT_EVENTS) {
            recentEvents.remove(0);
        }
    }

    private static void printDashboard() {
        double totalRealizedPnl = realizedPnl.values().stream().mapToDouble(Double::doubleValue).sum();
        double totalUnrealizedPnl = unrealizedPnl.values().stream().mapToDouble(Double::doubleValue).sum();
        double totalPortfolioValue = cash + positions.entrySet().stream()
                .mapToDouble(e -> e.getValue() * avgCosts.getOrDefault(e.getKey(), 0.0))
                .sum() + totalUnrealizedPnl;

        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("╔══════════════════════════════════════════════════════════════╗\n");
        sb.append("║              📊 KAFKA OMS — LIVE DASHBOARD                  ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║  💵 Cash:              $%,12.2f                       ║%n", cash));
        sb.append(String.format("║  📈 Portfolio Value:   $%,12.2f                       ║%n", totalPortfolioValue));
        sb.append(String.format("║  ✅ Realized PnL:      $%,12.2f                       ║%n", totalRealizedPnl));
        sb.append(String.format("║  📋 Unrealized PnL:    $%,12.2f                       ║%n", totalUnrealizedPnl));
        sb.append(String.format("║  🔄 Total Fills:       %,8d                             ║%n", totalFills));
        sb.append(String.format("║  ❌ Total Rejections:   %,8d                             ║%n", totalRejections));
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║  POSITIONS                                                  ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");

        if (positions.isEmpty()) {
            sb.append("║  (no positions yet)                                         ║\n");
        } else {
            for (Map.Entry<String, Integer> entry : positions.entrySet()) {
                String sym = entry.getKey();
                int pos = entry.getValue();
                double avg = avgCosts.getOrDefault(sym, 0.0);
                double rpnl = realizedPnl.getOrDefault(sym, 0.0);
                double upnl = unrealizedPnl.getOrDefault(sym, 0.0);
                sb.append(String.format("║  %-6s  pos=%-5d  avg=$%-8.2f  rpnl=$%-8.2f upnl=$%-8.2f║%n",
                        sym, pos, avg, rpnl, upnl));
            }
        }

        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║  RECENT EVENTS                                              ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");

        if (recentEvents.isEmpty()) {
            sb.append("║  (no events yet)                                            ║\n");
        } else {
            synchronized (recentEvents) {
                for (String event : recentEvents) {
                    String truncated = event.length() > 58 ? event.substring(0, 55) + "..." : event;
                    sb.append(String.format("║  %-60s║%n", truncated));
                }
            }
        }

        sb.append("╚══════════════════════════════════════════════════════════════╝\n");

        System.out.print(sb);
    }
}
