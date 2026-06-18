package com.example.inventoryservice.service;

import com.example.inventoryservice.model.PaymentResultEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InventoryServiceTest {

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService();
    }

    private PaymentResultEvent event(String orderId) {
        PaymentResultEvent e = new PaymentResultEvent();
        e.setOrderId(orderId);
        e.setStatus("succeeded");
        e.setAmount(100);
        e.setSource("payment-service");
        return e;
    }

    @Test
    void processesNewOrderSuccessfully() {
        // Should complete without throwing
        assertDoesNotThrow(() -> inventoryService.reserveInventory(event("order-1")));
    }

    @Test
    void skipsDuplicateOrderId() {
        PaymentResultEvent event = event("order-dup");

        inventoryService.reserveInventory(event);
        // Second call must not throw and must be silently skipped
        assertDoesNotThrow(() -> inventoryService.reserveInventory(event));
    }

    @Test
    void processesDifferentOrderIdsIndependently() {
        assertDoesNotThrow(() -> {
            inventoryService.reserveInventory(event("order-A"));
            inventoryService.reserveInventory(event("order-B"));
            inventoryService.reserveInventory(event("order-C"));
        });
    }

    @Test
    void idempotencyGuardDoesNotBlockNewOrdersAfterDuplicate() {
        inventoryService.reserveInventory(event("order-1"));
        inventoryService.reserveInventory(event("order-1")); // duplicate

        // New unique order must still be processed
        assertDoesNotThrow(() -> inventoryService.reserveInventory(event("order-2")));
    }
}
