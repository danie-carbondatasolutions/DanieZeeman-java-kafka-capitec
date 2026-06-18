package com.example.orderproducer.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void serialisesAllFieldsToJson() throws Exception {
        OrderEvent event = new OrderEvent("42", "customer-1", 150, "2026-01-01T00:00:00Z");
        String json = objectMapper.writeValueAsString(event);

        assertTrue(json.contains("\"orderId\":\"42\""));
        assertTrue(json.contains("\"customerId\":\"customer-1\""));
        assertTrue(json.contains("\"amount\":150"));
        assertTrue(json.contains("\"createdAt\":\"2026-01-01T00:00:00Z\""));
    }

    @Test
    void deserialisesFromJson() throws Exception {
        String json = "{\"orderId\":\"99\",\"customerId\":\"cust-x\",\"amount\":75,\"createdAt\":\"2026-01-02T00:00:00Z\"}";
        OrderEvent event = objectMapper.readValue(json, OrderEvent.class);

        assertEquals("99", event.getOrderId());
        assertEquals("cust-x", event.getCustomerId());
        assertEquals(75, event.getAmount());
        assertEquals("2026-01-02T00:00:00Z", event.getCreatedAt());
    }

    @Test
    void roundTripPreservesAllFields() throws Exception {
        OrderEvent original = new OrderEvent("1", "c1", 100, "2026-06-18T10:00:00Z");
        String json = objectMapper.writeValueAsString(original);
        OrderEvent restored = objectMapper.readValue(json, OrderEvent.class);

        assertEquals(original.getOrderId(), restored.getOrderId());
        assertEquals(original.getCustomerId(), restored.getCustomerId());
        assertEquals(original.getAmount(), restored.getAmount());
        assertEquals(original.getCreatedAt(), restored.getCreatedAt());
    }
}
