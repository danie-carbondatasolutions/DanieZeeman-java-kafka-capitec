package com.example.inventoryservice.model;

public class PaymentResultEvent {
    private String orderId;
    private String status;
    private int amount;
    private String source;

    public PaymentResultEvent() {}

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getAmount() { return amount; }
    public void setAmount(int amount) { this.amount = amount; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
}
