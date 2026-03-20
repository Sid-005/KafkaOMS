package com.kafkaoms.risk;

import com.kafkaoms.common.model.Order;
import com.kafkaoms.common.model.Side;

import java.util.HashMap;
import java.util.Map;

/**
 * Pure business logic for pre-trade risk validation.
 *
 * Extracted from RiskService so it can be unit tested without Kafka.
 * RiskService handles all Kafka I/O; this class handles all rule logic.
 *
 * Using instance state (not static) means each test gets a clean slate —
 * no state bleeds between test cases.
 *
 * RULES:
 *   1. Max order size:        quantity <= MAX_ORDER_SIZE
 *   2. Long-only:             cannot sell more shares than currently held
 *   3. Max position:          resulting position <= MAX_POSITION_PER_SYMBOL
 *   4. Sufficient cash:       estimated BUY cost <= availableCash
 */
public class RiskValidator {

    static final int MAX_ORDER_SIZE = 100;
    static final int MAX_POSITION_PER_SYMBOL = 500;
    static final double STARTING_CASH = 100_000.0;

    private final Map<String, Integer> positions = new HashMap<>();
    private final Map<String, Double> latestPrices = new HashMap<>();
    private double availableCash = STARTING_CASH;

    /**
     * Validates an order against all risk rules.
     * @return null if the order passes all checks, or a rejection reason string
     */
    public String validate(Order order) {
        // Rule 1: Max order size
        if (order.quantity() > MAX_ORDER_SIZE) {
            return "Order size " + order.quantity() + " exceeds max of " + MAX_ORDER_SIZE;
        }

        int currentPosition = positions.getOrDefault(order.symbol(), 0);

        // Rule 2: No short selling
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

        // Rule 4: Sufficient cash for BUY orders
        if (order.side() == Side.BUY) {
            Double marketPrice = latestPrices.get(order.symbol());
            if (marketPrice == null) {
                return "No market price available for " + order.symbol() + " — cannot assess cash requirement";
            }
            double estimatedCost = order.quantity() * marketPrice;
            if (estimatedCost > availableCash) {
                return String.format("Insufficient cash: order costs ~$%.2f but only $%.2f available",
                        estimatedCost, availableCash);
            }
        }

        return null;
    }

    /**
     * Records the effect of an approved order on positions and cash.
     * Must be called by RiskService after every approval.
     */
    public void recordApproval(Order order) {
        int delta = order.side() == Side.BUY ? order.quantity() : -order.quantity();
        positions.merge(order.symbol(), delta, Integer::sum);

        double price = latestPrices.getOrDefault(order.symbol(), 0.0);
        if (order.side() == Side.BUY) {
            availableCash -= order.quantity() * price;
        } else {
            availableCash += order.quantity() * price;
        }
    }

    /** Updates the cached market price for a symbol. */
    public void updatePrice(String symbol, double price) {
        latestPrices.put(symbol, price);
    }

    public double getAvailableCash() {
        return availableCash;
    }

    public int getPosition(String symbol) {
        return positions.getOrDefault(symbol, 0);
    }
}
