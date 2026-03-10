package com.kafkaoms.common.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates unique IDs for events, orders, and fills.
 *
 * WHY unique IDs matter:
 * In an event-driven system, every event needs a unique identifier so we can:
 *   1. Deduplicate — if a message is processed twice, we can detect it
 *   2. Trace — follow an order through the entire pipeline
 *   3. Audit — reconstruct exactly what happened
 *
 * We use a simple counter + prefix for readability.
 * In production, you'd typically use UUIDs or distributed ID generators.
 */
public final class IdGenerator {

    private static final AtomicLong EVENT_COUNTER = new AtomicLong(0);
    private static final AtomicLong ORDER_COUNTER = new AtomicLong(1000);
    private static final AtomicLong FILL_COUNTER  = new AtomicLong(9000);

    private IdGenerator() {}

    public static String newEventId() {
        return "evt_" + String.format("%03d", EVENT_COUNTER.incrementAndGet());
    }

    public static String newOrderId() {
        return "ord_" + ORDER_COUNTER.incrementAndGet();
    }

    public static String newFillId() {
        return "fill_" + FILL_COUNTER.incrementAndGet();
    }

    /** For cases where you need a truly unique ID (e.g., across restarts) */
    public static String newUUID() {
        return UUID.randomUUID().toString();
    }
}
