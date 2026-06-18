package com.example.paymentservice.model;

public class PaymentResultEvent {
    private String orderId;
    private String status;
    private int amount;
    private String source;

    public PaymentResultEvent() {}

    public PaymentResultEvent(String orderId, String status, int amount, String source) {
        this.orderId = orderId;
        this.status = status;
        this.amount = amount;
        this.source = source;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getAmount() { return amount; }
    public void setAmount(int amount) { this.amount = amount; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
}
