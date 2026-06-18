package com.example.inventoryservice.service;

import com.example.inventoryservice.model.PaymentResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // In-memory idempotency store — tracks processed orderIds to prevent double-reservation.
    // In production this would be backed by a persistent store (e.g. Redis or a DB unique constraint).
    private final Set<String> processedOrders = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public void reserveInventory(PaymentResultEvent event) {
        String orderId = event.getOrderId();

        if (!processedOrders.add(orderId)) {
            log.warn("Duplicate payment.succeeded event detected for orderId={}, skipping.", orderId);
            return;
        }

        log.info("Reserving inventory for orderId={}, amount={}", orderId, event.getAmount());

        // Inventory reservation logic goes here (e.g. decrement stock, persist reservation).
        // Placeholder: log the successful reservation.
        log.info("Inventory reserved successfully for orderId={}", orderId);
    }
}
