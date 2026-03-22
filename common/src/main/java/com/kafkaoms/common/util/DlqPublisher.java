package com.kafkaoms.common.util;

import com.kafkaoms.common.config.Topics;
import com.kafkaoms.common.metrics.MetricsRegistry;
import com.kafkaoms.common.model.DeadLetterEvent;
import com.kafkaoms.common.serde.JsonSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

/**
 * Utility for routing unprocessable messages to the dead-letter topic.
 *
 * USAGE — two steps in each service:
 *
 *   1. Create a producer once (at startup, alongside the main producer):
 *        KafkaProducer<String, DeadLetterEvent> dlqProducer = DlqPublisher.buildProducer();
 *
 *   2. Call publish() whenever you catch a processing error:
 *        } catch (Exception e) {
 *            DlqPublisher.publish(dlqProducer, record, "MyService", e);
 *        }
 *
 * The dead-letter message will contain enough context to find the original
 * message in Kafka UI (topic + partition + offset) and diagnose the error.
 */
public final class DlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private DlqPublisher() {}

    /** Creates a ready-to-use Kafka producer pointed at the dead-letter topic. */
    public static KafkaProducer<String, DeadLetterEvent> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    /**
     * Publishes a failed record to the dead-letter topic.
     *
     * @param dlqProducer   The DLQ producer (created via buildProducer())
     * @param record        The original Kafka record that failed
     * @param sourceService Human-readable name of the service (e.g. "RiskService")
     * @param cause         The exception that caused the failure
     */
    public static <V> void publish(
            KafkaProducer<String, DeadLetterEvent> dlqProducer,
            ConsumerRecord<String, V> record,
            String sourceService,
            Exception cause) {

        DeadLetterEvent event = new DeadLetterEvent(
                IdGenerator.newUUID(),
                record.topic(),
                sourceService,
                record.key(),
                record.partition(),
                record.offset(),
                cause.getClass().getSimpleName(),
                cause.getMessage(),
                Instant.now()
        );

        dlqProducer.send(
                new ProducerRecord<>(Topics.DEAD_LETTER, record.key(), event),
                (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to publish dead-letter event: {}", exception.getMessage());
                    } else {
                        log.warn("☠️  DEAD LETTER: service={} topic={} partition={} offset={} error={}: {}",
                                sourceService, record.topic(), record.partition(), record.offset(),
                                cause.getClass().getSimpleName(), cause.getMessage());
                        incrementDeadLetterCounter(sourceService);
                    }
                }
        );
    }

    private static void incrementDeadLetterCounter(String sourceService) {
        PrometheusMeterRegistry registry = MetricsRegistry.getOrNull();
        if (registry == null) return;
        Counter.builder("dead_letter_events_total")
                .description("Total messages routed to the dead-letter topic")
                .tag("source_service", sourceService)
                .register(registry)
                .increment();
    }
}
