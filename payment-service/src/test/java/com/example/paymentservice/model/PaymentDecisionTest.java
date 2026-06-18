package com.example.paymentservice.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PaymentDecisionTest {

    @Test
    void storesOrderIdAndSuccessFlag() {
        PaymentDecision success = new PaymentDecision("order-1", true);
        assertEquals("order-1", success.getOrderId());
        assertTrue(success.isSuccess());

        PaymentDecision failure = new PaymentDecision("order-2", false);
        assertEquals("order-2", failure.getOrderId());
        assertFalse(failure.isSuccess());
    }
}
