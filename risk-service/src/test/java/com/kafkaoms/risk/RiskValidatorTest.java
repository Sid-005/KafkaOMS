package com.kafkaoms.risk;

import com.kafkaoms.common.model.Order;
import com.kafkaoms.common.model.OrderType;
import com.kafkaoms.common.model.Side;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RiskValidator.
 *
 * Each test creates a fresh RiskValidator instance (via @BeforeEach),
 * so no state bleeds between test cases. This is why we extracted the
 * logic into a class with instance state instead of static state.
 *
 * STRUCTURE:
 *   @Nested classes group tests by rule, making the test file readable
 *   as documentation — you can see exactly what each rule covers.
 */
class RiskValidatorTest {

    private RiskValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RiskValidator();
    }

    // -----------------------------------------------------------------------
    // Helper: build a test order without boilerplate in every test
    // -----------------------------------------------------------------------
    private Order order(String symbol, Side side, int quantity) {
        return new Order(
                "evt_test",
                "ord_test",
                "test_strategy",
                symbol,
                side,
                quantity,
                OrderType.MARKET,
                Instant.now()
        );
    }

    // -----------------------------------------------------------------------
    // Rule 1: Max order size
    // -----------------------------------------------------------------------
    @Nested
    @DisplayName("Rule 1: Max order size")
    class MaxOrderSizeTests {

        @Test
        @DisplayName("Order at the limit (100 shares) is approved")
        void orderAtLimit_approved() {
            validator.updatePrice("AAPL", 100.0);
            assertNull(validator.validate(order("AAPL", Side.BUY, 100)));
        }

        @Test
        @DisplayName("Order one share over the limit is rejected")
        void orderOverLimit_rejected() {
            String reason = validator.validate(order("AAPL", Side.BUY, 101));
            assertNotNull(reason);
            assertTrue(reason.contains("exceeds max"));
        }

        @Test
        @DisplayName("Large order is rejected regardless of cash")
        void largeOrder_rejectedBeforeCashCheck() {
            validator.updatePrice("AAPL", 1.0); // cheap price, cash would be fine
            String reason = validator.validate(order("AAPL", Side.BUY, 200));
            assertNotNull(reason);
            assertTrue(reason.contains("exceeds max"));
        }
    }

    // -----------------------------------------------------------------------
    // Rule 2: Long-only (no short selling)
    // -----------------------------------------------------------------------
    @Nested
    @DisplayName("Rule 2: Long-only")
    class LongOnlyTests {

        @Test
        @DisplayName("Sell exactly what you hold is approved")
        void sellExactHolding_approved() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10));

            assertNull(validator.validate(order("AAPL", Side.SELL, 10)));
        }

        @Test
        @DisplayName("Sell less than you hold is approved")
        void sellPartialHolding_approved() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10));

            assertNull(validator.validate(order("AAPL", Side.SELL, 5)));
        }

        @Test
        @DisplayName("Sell more than you hold is rejected")
        void sellMoreThanHolding_rejected() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10));

            String reason = validator.validate(order("AAPL", Side.SELL, 11));
            assertNotNull(reason);
            assertTrue(reason.contains("only holding 10"));
        }

        @Test
        @DisplayName("Sell when holding nothing is rejected")
        void sellWithNoPosition_rejected() {
            String reason = validator.validate(order("AAPL", Side.SELL, 1));
            assertNotNull(reason);
            assertTrue(reason.contains("only holding 0"));
        }
    }

    // -----------------------------------------------------------------------
    // Rule 3: Max position per symbol
    // -----------------------------------------------------------------------
    @Nested
    @DisplayName("Rule 3: Max position per symbol")
    class MaxPositionTests {

        @Test
        @DisplayName("Buy that lands exactly at the limit is approved")
        void buyToExactLimit_approved() {
            validator.updatePrice("AAPL", 1.0);
            // Buy up to 400 first
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));

            // This should land exactly at 500 — should pass
            assertNull(validator.validate(order("AAPL", Side.BUY, 100)));
        }

        @Test
        @DisplayName("Buy that would exceed the limit is rejected")
        void buyOverLimit_rejected() {
            validator.updatePrice("AAPL", 1.0);
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100));
            validator.recordApproval(order("AAPL", Side.BUY, 100)); // now at 500

            String reason = validator.validate(order("AAPL", Side.BUY, 1));
            assertNotNull(reason);
            assertTrue(reason.contains("exceeds max of 500"));
        }

        @Test
        @DisplayName("Position limit is per symbol — other symbols are unaffected")
        void positionLimitIsPerSymbol() {
            validator.updatePrice("AAPL", 1.0);
            validator.updatePrice("GOOGL", 1.0);

            // Max out AAPL
            for (int i = 0; i < 5; i++) {
                validator.recordApproval(order("AAPL", Side.BUY, 100));
            }

            // GOOGL should still be fine
            assertNull(validator.validate(order("GOOGL", Side.BUY, 100)));
        }
    }

    // -----------------------------------------------------------------------
    // Rule 4: Sufficient cash
    // -----------------------------------------------------------------------
    @Nested
    @DisplayName("Rule 4: Sufficient cash")
    class SufficientCashTests {

        @Test
        @DisplayName("Buy within available cash is approved")
        void buyWithinCash_approved() {
            validator.updatePrice("AAPL", 100.0);
            // 10 × $100 = $1,000 — well within $100,000
            assertNull(validator.validate(order("AAPL", Side.BUY, 10)));
        }

        @Test
        @DisplayName("Buy that exceeds available cash is rejected")
        void buyExceedsCash_rejected() {
            validator.updatePrice("AAPL", 10_000.0); // expensive!
            // 100 × $10,000 = $1,000,000 — exceeds $100,000
            String reason = validator.validate(order("AAPL", Side.BUY, 100));
            assertNotNull(reason);
            assertTrue(reason.contains("Insufficient cash"));
        }

        @Test
        @DisplayName("Buy with no market price available is rejected")
        void buyWithNoPrice_rejected() {
            // No updatePrice() call — no price available
            String reason = validator.validate(order("AAPL", Side.BUY, 10));
            assertNotNull(reason);
            assertTrue(reason.contains("No market price available"));
        }

        @Test
        @DisplayName("Cash reduces correctly after each approval")
        void cashReducesAfterApproval() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10)); // costs $1,000

            assertEquals(RiskValidator.STARTING_CASH - 1_000.0, validator.getAvailableCash(), 0.01);
        }

        @Test
        @DisplayName("Cash increases after SELL approval")
        void cashIncreasesAfterSell() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10));  // buy 10 @ $100
            validator.recordApproval(order("AAPL", Side.SELL, 10)); // sell 10 @ $100

            // Should be back to starting cash
            assertEquals(RiskValidator.STARTING_CASH, validator.getAvailableCash(), 0.01);
        }

        @Test
        @DisplayName("SELL orders bypass the cash check")
        void sellOrderBypassesCashCheck() {
            validator.updatePrice("AAPL", 100.0);
            validator.recordApproval(order("AAPL", Side.BUY, 10));

            // Even if we had no cash, a sell should not be blocked by Rule 4
            assertNull(validator.validate(order("AAPL", Side.SELL, 5)));
        }
    }

    // -----------------------------------------------------------------------
    // Rule ordering: first failing rule wins
    // -----------------------------------------------------------------------
    @Nested
    @DisplayName("Rule ordering")
    class RuleOrderingTests {

        @Test
        @DisplayName("Rule 1 fires before Rule 4 — size rejection returned for oversized order")
        void rule1BeforeRule4() {
            validator.updatePrice("AAPL", 1.0); // cheap — cash would be fine
            String reason = validator.validate(order("AAPL", Side.BUY, 200));
            assertNotNull(reason);
            assertTrue(reason.contains("exceeds max"), "Expected Rule 1 rejection, got: " + reason);
        }
    }
}
