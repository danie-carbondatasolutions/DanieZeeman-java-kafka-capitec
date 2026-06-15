package com.example.paymentservice.model;

public class PaymentDecision {
    private final String orderId;
    private final boolean success;

    public PaymentDecision(String orderId, boolean success) {
        this.orderId = orderId;
        this.success = success;
    }

    public String getOrderId() {
        return orderId;
    }

    public boolean isSuccess() {
        return success;
    }
}
