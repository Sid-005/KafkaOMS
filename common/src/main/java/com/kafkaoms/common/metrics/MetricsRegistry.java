package com.kafkaoms.common.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Singleton that holds the Prometheus meter registry and exposes a /metrics
 * HTTP endpoint that Prometheus scrapes.
 *
 * USAGE — call once at the top of each service's main():
 *
 *   MetricsRegistry.init("risk-service", 8081);
 *   PrometheusMeterRegistry registry = MetricsRegistry.get();
 *
 * Then open http://localhost:8081/metrics in your browser to see raw metrics,
 * or let Prometheus scrape it automatically.
 *
 * HOW MICROMETER WORKS:
 *   Micrometer is a "metrics facade" — similar to how SLF4J is a logging facade.
 *   You write:  counter.increment()
 *   Micrometer handles the Prometheus-specific format, thread safety, and labelling.
 *
 *   The PrometheusMeterRegistry formats the metrics as Prometheus text format:
 *     # HELP risk_orders_total Total orders processed by the risk service
 *     # TYPE risk_orders_total counter
 *     risk_orders_total{result="approved",service="risk-service"} 42.0
 */
public final class MetricsRegistry {

    private static final Logger log = LoggerFactory.getLogger(MetricsRegistry.class);
    private static PrometheusMeterRegistry registry;

    private MetricsRegistry() {}

    /**
     * Initialises the registry and starts the /metrics HTTP server.
     * Must be called once at service startup before any metrics are recorded.
     *
     * @param serviceName added as a common tag on every metric (e.g. "risk-service")
     * @param port        the port to expose /metrics on
     */
    public static synchronized void init(String serviceName, int port) {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("service", serviceName);
        startHttpServer(port);
        log.info("📊 Metrics server started → http://localhost:{}/metrics", port);
    }

    /** Returns the registry. Throws if init() was not called first. */
    public static PrometheusMeterRegistry get() {
        if (registry == null) {
            throw new IllegalStateException("MetricsRegistry not initialised — call init() first");
        }
        return registry;
    }

    /**
     * Returns the registry, or null if init() was never called.
     * Safe for optional usage — callers must null-check before using.
     */
    public static PrometheusMeterRegistry getOrNull() {
        return registry;
    }

    private static void startHttpServer(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", exchange -> {
                String body = registry.scrape();
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });
            server.setExecutor(null);
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start metrics HTTP server on port " + port, e);
        }
    }
}
